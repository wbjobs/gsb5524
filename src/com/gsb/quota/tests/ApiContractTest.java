package com.gsb.quota.tests;

import com.gsb.quota.ManualClock;
import com.gsb.quota.Scheduler;

import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;

/**
 * Basic API contract: argument validation, duplicate registration, unknown
 * tenants, unique task ids, and lifecycle rules.
 */
public final class ApiContractTest {

    private ApiContractTest() {
    }

    public static void run() throws Exception {
        ManualClock clock = new ManualClock();
        Scheduler scheduler = new Scheduler(clock, 1);

        Checks.expectThrow(IllegalArgumentException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.register(null, 10L, 1);
                    }
                }, "null tenant id must be rejected");
        Checks.expectThrow(IllegalArgumentException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.register("t", 0L, 1);
                    }
                }, "zero quota must be rejected");
        Checks.expectThrow(IllegalArgumentException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.register("t", 10L, 0);
                    }
                }, "zero weight must be rejected");

        scheduler.register("t", 10L, 1);
        Checks.expectThrow(IllegalArgumentException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.register("t", 10L, 1);
                    }
                }, "duplicate registration must be rejected");
        Checks.expectThrow(IllegalArgumentException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.submit("ghost", noop());
                    }
                }, "submit for unknown tenant must be rejected");
        Checks.expectThrow(NullPointerException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.submit("t", null);
                    }
                }, "null task must be rejected");

        String first = scheduler.submit("t", noop());
        String second = scheduler.submit("t", noop());
        Checks.assertTrue(first != null && second != null, "task ids must not be null");
        Checks.assertTrue(!first.equals(second), "task ids must be unique");
        Checks.assertEquals(2L, scheduler.queuedCount("t"), "tasks must queue before start");

        scheduler.start();
        Checks.waitUntil(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return scheduler.completedCount("t") == 2L;
            }
        }, 10000L, "queued tasks run after start");

        scheduler.shutdown();
        Checks.expectThrow(IllegalStateException.class,
                new Checks.ThrowingRunnable() {
                    @Override
                    public void run() {
                        scheduler.submit("t", noop());
                    }
                }, "submit after shutdown must be rejected");
    }

    static Callable<Void> noop() {
        return new Callable<Void>() {
            @Override
            public Void call() {
                return null;
            }
        };
    }
}
