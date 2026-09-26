package com.gsb.quota.tests;

import com.gsb.quota.ManualClock;
import com.gsb.quota.Scheduler;

/**
 * The token bucket must release exactly the quota on the boundary and never
 * one token more: a bucket of N releases N tasks immediately, the (N+1)-th
 * waits until the one-second refill tick, and no refill happens even one
 * millisecond before it.
 */
public final class TokenBucketBoundaryTest {

    private TokenBucketBoundaryTest() {
    }

    public static void run() throws Exception {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 2);
        final long quota = 10L;
        scheduler.register("tenant-b", quota, 1);
        try {
            for (int i = 0; i < quota; i++) {
                scheduler.submit("tenant-b", ApiContractTest.noop());
            }
            scheduler.start();

            Checks.waitUntil(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.releasedCount("tenant-b") == quota;
                }
            }, 10000L, "exactly quota tasks are released at the boundary");
            Checks.assertEquals(0L, scheduler.queuedCount("tenant-b"),
                    "nothing may be queued when submissions equal the quota");

            scheduler.submit("tenant-b", ApiContractTest.noop());
            Checks.assertStable(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.releasedCount("tenant-b") == quota;
                }
            }, 200L, "bucket must not release one more than the quota");
            Checks.assertEquals(1L, scheduler.queuedCount("tenant-b"),
                    "the extra task must be queued, not dropped");

            clock.advanceMillis(999L);
            Checks.assertStable(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.releasedCount("tenant-b") == quota;
                }
            }, 200L, "no refill may happen before the one-second boundary");

            clock.advanceMillis(1L);
            Checks.waitUntil(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.releasedCount("tenant-b") == quota + 1L;
                }
            }, 10000L, "the queued task is released exactly at the refill boundary");
            Checks.assertEquals(0L, scheduler.queuedCount("tenant-b"),
                    "queue must drain once the refill arrives");
        } finally {
            scheduler.shutdown();
        }
    }
}
