package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multi-tenant task scheduler with per-tenant token-bucket quotas and
 * weighted-fair dispatching.
 *
 * Quota: each tenant owns a token bucket whose capacity equals its
 * tokensPerSecond and which is refilled once per one-second period of clock
 * time. A queued task may only be released for execution when its tenant
 * holds at least one token, so the number of releases inside any aligned
 * one-second window never exceeds tokensPerSecond. Submissions that exceed
 * the quota are never dropped; they wait in the tenant's queue.
 *
 * Fairness: whenever several tenants have releasable work, the scheduler
 * picks the tenant with the smallest virtual finish time and charges it
 * 1/weight per released task. Over a long run the release (and therefore
 * completion) counts converge to the weight ratio, and a continuously
 * backlogged tenant is never starved for more than one second of clock time.
 *
 * Submission is not serialized through a single global lock: submit() only
 * touches the target tenant's concurrent queue and then signals the
 * dispatcher. All time is read from the injected Clock.
 */
public final class Scheduler {

    private static final long REFILL_PERIOD_NANOS = 1000000000L;
    private static final long WORKER_WAIT_NANOS = 1000000000L;

    private final Clock clock;
    private final ConcurrentHashMap<String, Tenant> tenants =
            new ConcurrentHashMap<String, Tenant>();
    private final CopyOnWriteArrayList<Tenant> tenantList =
            new CopyOnWriteArrayList<Tenant>();
    private final ReentrantLock schedLock = new ReentrantLock();
    private final Condition schedChanged = schedLock.newCondition();
    private final Semaphore freeWorkers;
    private final ExecutorService workers;
    private final Thread dispatcher;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong totalCompleted = new AtomicLong();
    private volatile boolean running;
    private volatile boolean shutdown;

    public Scheduler() {
        this(SystemClock.INSTANCE, defaultWorkerCount());
    }

    public Scheduler(Clock clock) {
        this(clock, defaultWorkerCount());
    }

    public Scheduler(Clock clock, int workerThreads) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        if (workerThreads < 1) {
            throw new IllegalArgumentException("workerThreads must be >= 1");
        }
        this.clock = clock;
        this.freeWorkers = new Semaphore(workerThreads);
        this.workers = Executors.newFixedThreadPool(workerThreads, new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "gsb-quota-worker-" + count.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        this.dispatcher = new Thread(new Runnable() {
            @Override
            public void run() {
                dispatchLoop();
            }
        }, "gsb-quota-dispatcher");
        this.dispatcher.setDaemon(true);
        this.clock.addTickListener(new Runnable() {
            @Override
            public void run() {
                signalDispatcher();
            }
        });
    }

    private static int defaultWorkerCount() {
        return Math.max(2, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Registers a tenant with a per-second token quota and a scheduling weight.
     */
    public void register(String tenantId, long tokensPerSecond, int weight) {
        if (tenantId == null || tenantId.isEmpty()) {
            throw new IllegalArgumentException("tenantId must not be null or empty");
        }
        if (tokensPerSecond <= 0L) {
            throw new IllegalArgumentException("tokensPerSecond must be > 0");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be > 0");
        }
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
        Tenant tenant = new Tenant(tenantId, tokensPerSecond, weight, clock.nanoTime());
        if (tenants.putIfAbsent(tenantId, tenant) != null) {
            throw new IllegalArgumentException("tenant already registered: " + tenantId);
        }
        tenantList.add(tenant);
        signalDispatcher();
    }

    /**
     * Enqueues a task for the given tenant and returns its task id. The task
     * is released for execution once the tenant's quota and the weighted-fair
     * order allow it; tasks are never dropped because of quota exhaustion.
     */
    public String submit(String tenantId, Callable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        Tenant tenant = tenants.get(tenantId);
        if (tenant == null) {
            throw new IllegalArgumentException("unknown tenant: " + tenantId);
        }
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
        String id = tenantId + "/" + tenant.sequence.incrementAndGet();
        tenant.queue.offer(new Task(id, task));
        tenant.pending.incrementAndGet();
        signalDispatcher();
        return id;
    }

    /**
     * Starts the dispatcher and worker threads. Idempotent.
     */
    public void start() {
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
        if (started.compareAndSet(false, true)) {
            running = true;
            dispatcher.start();
        }
    }

    /**
     * Stops dispatching and shuts the worker pool down. Idempotent.
     */
    public void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        running = false;
        signalDispatcher();
        if (started.get()) {
            try {
                dispatcher.join(TimeUnit.SECONDS.toMillis(5L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5L, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** @return tasks released for execution so far for the tenant. */
    public long releasedCount(String tenantId) {
        return tenant(tenantId).released.get();
    }

    /** @return tasks completed (successfully or not) so far for the tenant. */
    public long completedCount(String tenantId) {
        return tenant(tenantId).completed.get();
    }

    /** @return tasks still waiting in the tenant's queue. */
    public long queuedCount(String tenantId) {
        return tenant(tenantId).pending.get();
    }

    /**
     * @return the largest gap, in nanoseconds of clock time, between two
     * consecutive releases of this tenant (or since its registration).
     */
    public long maxReleaseGapNanos(String tenantId) {
        return tenant(tenantId).maxReleaseGapNanos.get();
    }

    /** @return total tasks completed across all tenants. */
    public long totalCompleted() {
        return totalCompleted.get();
    }

    private Tenant tenant(String tenantId) {
        Tenant tenant = tenants.get(tenantId);
        if (tenant == null) {
            throw new IllegalArgumentException("unknown tenant: " + tenantId);
        }
        return tenant;
    }

    private void dispatchLoop() {
        long order = 0L;
        while (running) {
            schedLock.lock();
            try {
                if (!running) {
                    break;
                }
                long now = clock.nanoTime();
                Tenant best = null;
                long nextWake = Long.MAX_VALUE;
                for (Tenant tenant : tenantList) {
                    refill(tenant, now);
                    if (tenant.pending.get() <= 0) {
                        continue;
                    }
                    if (tenant.tokens <= 0L) {
                        long wake = tenant.lastRefillNanos + REFILL_PERIOD_NANOS - now;
                        if (wake > 0L && wake < nextWake) {
                            nextWake = wake;
                        }
                        continue;
                    }
                    if (best == null || comesBefore(tenant, best)) {
                        best = tenant;
                    }
                }
                if (best != null && freeWorkers.tryAcquire()) {
                    Task task = best.queue.poll();
                    if (task == null) {
                        freeWorkers.release();
                        continue;
                    }
                    best.pending.decrementAndGet();
                    best.tokens -= 1L;
                    best.virtualFinish += best.weightCost;
                    best.lastDispatchOrder = order++;
                    long previous = best.lastReleaseNanos.getAndSet(now);
                    if (now > previous) {
                        final long gap = now - previous;
                        best.maxReleaseGapNanos.accumulateAndGet(gap,
                                new java.util.function.LongBinaryOperator() {
                                    @Override
                                    public long applyAsLong(long x, long y) {
                                        return x >= y ? x : y;
                                    }
                                });
                    }
                    best.released.incrementAndGet();
                    execute(best, task);
                    continue;
                }
                if (best != null) {
                    schedChanged.awaitNanos(WORKER_WAIT_NANOS);
                } else if (nextWake == Long.MAX_VALUE) {
                    schedChanged.await();
                } else {
                    schedChanged.awaitNanos(nextWake);
                }
            } catch (InterruptedException ignored) {
                // The loop condition re-checks the running flag.
            } finally {
                schedLock.unlock();
            }
        }
    }

    private static boolean comesBefore(Tenant a, Tenant b) {
        if (a.virtualFinish != b.virtualFinish) {
            return a.virtualFinish < b.virtualFinish;
        }
        return a.lastDispatchOrder < b.lastDispatchOrder;
    }

    private void refill(Tenant tenant, long now) {
        long elapsed = now - tenant.lastRefillNanos;
        if (elapsed < REFILL_PERIOD_NANOS) {
            return;
        }
        long periods = elapsed / REFILL_PERIOD_NANOS;
        long capacity = tenant.tokensPerSecond;
        long needed = capacity - tenant.tokens;
        if (needed > 0L) {
            long periodsToFull = needed / tenant.tokensPerSecond + 1L;
            if (periods >= periodsToFull) {
                tenant.tokens = capacity;
            } else {
                tenant.tokens += periods * tenant.tokensPerSecond;
            }
        }
        tenant.lastRefillNanos += periods * REFILL_PERIOD_NANOS;
    }

    private void execute(final Tenant tenant, final Task task) {
        try {
            workers.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        task.callable.call();
                    } catch (Throwable t) {
                        tenant.failed.incrementAndGet();
                    } finally {
                        tenant.completed.incrementAndGet();
                        totalCompleted.incrementAndGet();
                        freeWorkers.release();
                        signalDispatcher();
                    }
                }
            });
        } catch (RuntimeException rejected) {
            freeWorkers.release();
        }
    }

    private void signalDispatcher() {
        schedLock.lock();
        try {
            schedChanged.signalAll();
        } finally {
            schedLock.unlock();
        }
    }

    private static final class Tenant {
        final String id;
        final long tokensPerSecond;
        final int weight;
        final double weightCost;
        final ConcurrentLinkedQueue<Task> queue = new ConcurrentLinkedQueue<Task>();
        final AtomicInteger pending = new AtomicInteger();
        final AtomicLong sequence = new AtomicLong();
        final AtomicLong released = new AtomicLong();
        final AtomicLong completed = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicLong lastReleaseNanos;
        final AtomicLong maxReleaseGapNanos = new AtomicLong();
        // Token-bucket and WFQ state, confined to the dispatcher thread.
        long tokens;
        long lastRefillNanos;
        double virtualFinish;
        long lastDispatchOrder = -1L;

        Tenant(String id, long tokensPerSecond, int weight, long now) {
            this.id = id;
            this.tokensPerSecond = tokensPerSecond;
            this.weight = weight;
            this.weightCost = 1.0d / weight;
            this.tokens = tokensPerSecond;
            this.lastRefillNanos = now;
            this.lastReleaseNanos = new AtomicLong(now);
        }
    }

    private static final class Task {
        final String id;
        final Callable callable;

        Task(String id, Callable callable) {
            this.id = id;
            this.callable = callable;
        }
    }
}
