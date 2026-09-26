package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Smoke tests: queueing before start, argument validation, counters,
 * shutdown semantics.
 */
public final class SchedulerBasicTest {

    private SchedulerBasicTest() {
    }

    public static void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 1);
        scheduler.register("t1", 10L, 1);

        final AtomicLong ran = new AtomicLong();
        Callable<Object> task = new Callable<Object>() {
            @Override
            public Object call() {
                ran.incrementAndGet();
                return null;
            }
        };

        // Submissions before start() are queued, not dropped.
        String id = scheduler.submit("t1", task);
        TestSupport.check(id != null && id.length() > 0, "submit must return a task id");
        TestSupport.check(scheduler.queuedCount("t1") == 1L, "task must be queued before start");
        TestSupport.check(scheduler.completedCount("t1") == 0L, "nothing completed before start");

        scheduler.start();
        clock.advance(100L); // 10 tokens/s -> exactly 1 token after 100ms
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return ran.get() == 1L;
            }
        }, 5000L, "queued task runs after start");
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return scheduler.completedCount("t1") == 1L;
            }
        }, 5000L, "completedCount reflects the finished task");

        // Unknown tenant is rejected.
        boolean rejected = false;
        try {
            scheduler.submit("no-such-tenant", task);
        } catch (IllegalArgumentException expected) {
            rejected = true;
        }
        TestSupport.check(rejected, "submit to unknown tenant must throw IllegalArgumentException");

        // Duplicate registration is rejected.
        rejected = false;
        try {
            scheduler.register("t1", 1L, 1);
        } catch (IllegalStateException expected) {
            rejected = true;
        }
        TestSupport.check(rejected, "duplicate register must throw IllegalStateException");

        // Invalid parameters are rejected.
        rejected = false;
        try {
            scheduler.register("bad", 0L, 1);
        } catch (IllegalArgumentException expected) {
            rejected = true;
        }
        TestSupport.check(rejected, "zero quota must be rejected");

        // Shutdown is idempotent; submissions after shutdown are rejected.
        scheduler.shutdown();
        scheduler.shutdown();
        rejected = false;
        try {
            scheduler.submit("t1", task);
        } catch (IllegalStateException expected) {
            rejected = true;
        }
        TestSupport.check(rejected, "submit after shutdown must throw IllegalStateException");
    }
}
