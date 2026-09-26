package com.gsb.quota;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-tenant scheduler combining per-tenant token-bucket quotas with
 * weighted fair queuing (virtual-finish-time WFQ).
 *
 * <p>Submissions never block on a global lock: {@link #submit} only touches a
 * concurrent map lookup, a tenant-local tag lock and a lock-free queue. A
 * single dispatcher thread picks the eligible tenant whose head task has the
 * smallest virtual finish tag, spends one of that tenant's tokens, and hands
 * the task to a worker pool. Over any sufficiently long run, each tenant's
 * share of completed tasks converges to its weight share, and a backlogged
 * tenant is never skipped while it holds tokens.
 *
 * <p>All time is read from the injected {@link Clock}; the dispatcher waits
 * via {@link Clock#await(long)} and is woken early by an interrupt from
 * {@link #submit} or {@link #shutdown}.
 */
public final class Scheduler {

    private final Clock clock;
    private final int workerThreads;
    private final ConcurrentHashMap<String, TenantState> tenants =
            new ConcurrentHashMap<String, TenantState>();
    private final Object dispatchLock = new Object();
    private final AtomicLong idSequence = new AtomicLong();

    /** WFQ virtual time: the largest virtual finish tag dispatched so far. */
    private volatile double virtualTime;
    private volatile boolean running;
    private volatile boolean stopped;
    private Thread dispatcher;
    private ExecutorService executor;

    /** Creates a scheduler running on the real (system) clock. */
    public Scheduler() {
        this(new SystemClock());
    }

    /** Creates a scheduler running on the given clock. */
    public Scheduler(Clock clock) {
        this(clock, Math.max(1, Runtime.getRuntime().availableProcessors()));
    }

    /** Creates a scheduler running on the given clock with a fixed worker count. */
    public Scheduler(Clock clock, int workerThreads) {
        if (clock == null) {
            throw new NullPointerException("clock");
        }
        if (workerThreads < 1) {
            throw new IllegalArgumentException("workerThreads must be >= 1");
        }
        this.clock = clock;
        this.workerThreads = workerThreads;
    }

    /**
     * Registers a tenant with a per-second token quota and a fairness weight.
     */
    public void register(String tenantId, long tokensPerSecond, int weight) {
        if (tenantId == null || tenantId.isEmpty()) {
            throw new IllegalArgumentException("tenantId must not be empty");
        }
        if (tokensPerSecond < 1L) {
            throw new IllegalArgumentException("tokensPerSecond must be >= 1");
        }
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1");
        }
        TenantState tenant = new TenantState(tenantId, tokensPerSecond, weight, clock);
        if (tenants.putIfAbsent(tenantId, tenant) != null) {
            throw new IllegalStateException("tenant already registered: " + tenantId);
        }
    }

    /**
     * Enqueues a task for the tenant and returns its id. The task is never
     * dropped: if the tenant is over quota it waits in the tenant's queue
     * until tokens become available.
     */
    public String submit(String tenantId, Callable<?> task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (stopped) {
            throw new IllegalStateException("scheduler has been shut down");
        }
        TenantState tenant = tenants.get(tenantId);
        if (tenant == null) {
            throw new IllegalArgumentException("unknown tenant: " + tenantId);
        }
        String id = "task-" + idSequence.incrementAndGet();
        tenant.enqueue(new QueuedTask(id, task, tenant.nextTag(virtualTime)));
        Thread d = dispatcher;
        if (d != null) {
            d.interrupt();
        }
        return id;
    }

    /** Starts the dispatcher and worker threads. Idempotent. */
    public synchronized void start() {
        if (stopped) {
            throw new IllegalStateException("scheduler has been shut down");
        }
        if (running) {
            return;
        }
        running = true;
        executor = Executors.newFixedThreadPool(workerThreads, new ThreadFactory() {
            private final AtomicLong seq = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "quota-scheduler-worker-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        dispatcher = new Thread(new Runnable() {
            @Override
            public void run() {
                dispatchLoop();
            }
        }, "quota-scheduler-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
    }

    /** Stops dispatching and shuts down the worker pool. Idempotent. */
    public void shutdown() {
        Thread d;
        ExecutorService e;
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            running = false;
            d = dispatcher;
            e = executor;
        }
        if (d != null) {
            d.interrupt();
        }
        if (e != null) {
            e.shutdownNow();
            try {
                e.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Number of tasks completed for the tenant (0 for unknown tenants). */
    public long completedCount(String tenantId) {
        TenantState tenant = tenants.get(tenantId);
        return tenant == null ? 0L : tenant.completed.get();
    }

    /** Number of tasks currently waiting in the tenant's queue (0 for unknown tenants). */
    public long queuedCount(String tenantId) {
        TenantState tenant = tenants.get(tenantId);
        return tenant == null ? 0L : tenant.queueSize();
    }

    private void dispatchLoop() {
        while (running) {
            long deadline = dispatchOnceOrDeadline();
            if (deadline < 0L) {
                continue;
            }
            if (!running) {
                break;
            }
            try {
                clock.await(deadline);
            } catch (InterruptedException ie) {
                // Woken by submit()/shutdown(); re-evaluate.
            }
            // Clear the interrupt flag so park-based clocks do not spin.
            Thread.interrupted();
        }
    }

    /**
     * Dispatches at most one task.
     *
     * @return -1 if a task was dispatched, otherwise the clock deadline the
     *         dispatcher should wait until (the next token refill time, or
     *         {@link Long#MAX_VALUE} when no tenant has queued work).
     */
    private long dispatchOnceOrDeadline() {
        synchronized (dispatchLock) {
            long now = clock.millis();
            TenantState best = null;
            double bestTag = 0.0;
            long deadline = Long.MAX_VALUE;
            for (Map.Entry<String, TenantState> entry : tenants.entrySet()) {
                TenantState tenant = entry.getValue();
                if (tenant.isEmpty()) {
                    continue;
                }
                long waitMillis = tenant.bucket.millisUntilToken(now);
                if (waitMillis <= 0L) {
                    double tag = tenant.headTag();
                    if (best == null || tag < bestTag) {
                        best = tenant;
                        bestTag = tag;
                    }
                } else {
                    long candidate = now + waitMillis;
                    if (candidate < deadline) {
                        deadline = candidate;
                    }
                }
            }
            if (best == null) {
                return deadline;
            }
            QueuedTask task = best.poll();
            best.bucket.consume(now);
            if (task.tag > virtualTime) {
                virtualTime = task.tag;
            }
            ExecutorService e = executor;
            if (e != null) {
                try {
                    e.execute(new TaskRunner(best, task));
                } catch (RejectedExecutionException rejected) {
                    // Shutting down; the task is abandoned on purpose.
                }
            }
            return -1L;
        }
    }

    private static final class TaskRunner implements Runnable {

        private final TenantState tenant;
        private final QueuedTask task;

        TaskRunner(TenantState tenant, QueuedTask task) {
            this.tenant = tenant;
            this.task = task;
        }

        @Override
        public void run() {
            try {
                task.callable.call();
            } catch (Throwable ignored) {
                // A failing tenant task must not kill the worker thread.
            } finally {
                tenant.completed.incrementAndGet();
            }
        }
    }
}
