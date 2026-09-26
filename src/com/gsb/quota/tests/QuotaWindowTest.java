package com.gsb.quota.tests;

import com.gsb.quota.ManualClock;
import com.gsb.quota.Scheduler;

/**
 * With a constant backlog, every aligned one-second window of virtual time
 * releases at most tokensPerSecond tasks, and everything beyond the quota
 * waits in the queue instead of being dropped.
 */
public final class QuotaWindowTest {

    private QuotaWindowTest() {
    }

    public static void run() throws Exception {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 4);
        final long quota = 10L;
        final int submitted = 35;
        scheduler.register("tenant-q", quota, 1);
        try {
            for (int i = 0; i < submitted; i++) {
                scheduler.submit("tenant-q", ApiContractTest.noop());
            }
            scheduler.start();

            long releasedBefore = 0L;
            for (int window = 0; window < 3; window++) {
                if (window > 0) {
                    clock.advanceSeconds(1L);
                }
                final long expected = Math.min((long) submitted, quota * (window + 1L));
                Checks.waitUntil(new java.util.function.BooleanSupplier() {
                    @Override
                    public boolean getAsBoolean() {
                        return scheduler.releasedCount("tenant-q") == expected;
                    }
                }, 10000L, "window " + window + " releases up to the quota");
                Checks.assertStable(new java.util.function.BooleanSupplier() {
                    @Override
                    public boolean getAsBoolean() {
                        return scheduler.releasedCount("tenant-q") == expected;
                    }
                }, 150L, "window " + window + " must not release more than the quota");
                long releasedInWindow = expected - releasedBefore;
                Checks.assertTrue(releasedInWindow <= quota,
                        "window " + window + " released " + releasedInWindow
                                + " which exceeds quota " + quota);
                Checks.assertEquals(submitted - expected, scheduler.queuedCount("tenant-q"),
                        "window " + window + ": over-quota tasks must queue, not be dropped");
                releasedBefore = expected;
            }

            Checks.assertEquals(5L, scheduler.queuedCount("tenant-q"),
                    "remaining tasks stay queued, none dropped");
            clock.advanceSeconds(1L);
            Checks.waitUntil(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.releasedCount("tenant-q") == submitted;
                }
            }, 10000L, "all queued tasks are eventually released");
            Checks.waitUntil(new java.util.function.BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return scheduler.completedCount("tenant-q") == submitted;
                }
            }, 10000L, "all submitted tasks complete");
            Checks.assertEquals(0L, scheduler.queuedCount("tenant-q"), "queue fully drains");
        } finally {
            scheduler.shutdown();
        }
    }
}
