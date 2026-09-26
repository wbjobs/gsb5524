package com.gsb.quota.tests;

import com.gsb.quota.ManualClock;
import com.gsb.quota.Scheduler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Three tenants with weights 1:2:3 and quotas high enough to never bind.
 * After a long run (driven entirely by virtual time), completion counts must
 * match the 1:2:3 ratio within 15%, and no tenant may go more than one
 * second of virtual time without a release.
 */
public final class WeightedFairnessTest {

    private static final long ONE_SECOND_NANOS = 1000000000L;

    private WeightedFairnessTest() {
    }

    public static void run() throws Exception {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 4);
        scheduler.register("tenant-a", 1000000L, 1);
        scheduler.register("tenant-b", 1000000L, 2);
        scheduler.register("tenant-c", 1000000L, 3);
        try {
            int perTenant = 60000;
            for (int i = 0; i < perTenant; i++) {
                scheduler.submit("tenant-a", ApiContractTest.noop());
                scheduler.submit("tenant-b", ApiContractTest.noop());
                scheduler.submit("tenant-c", ApiContractTest.noop());
            }
            scheduler.start();

            long target = 90000L;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
            while (scheduler.totalCompleted() < target) {
                if (System.nanoTime() - deadline > 0L) {
                    throw new AssertionError("fairness test timed out");
                }
                clock.advanceMillis(1L);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }

            long a = scheduler.completedCount("tenant-a");
            long b = scheduler.completedCount("tenant-b");
            long c = scheduler.completedCount("tenant-c");
            long total = a + b + c;
            assertShare("tenant-a", a, total, 1.0d / 6.0d);
            assertShare("tenant-b", b, total, 2.0d / 6.0d);
            assertShare("tenant-c", c, total, 3.0d / 6.0d);

            Checks.assertTrue(scheduler.maxReleaseGapNanos("tenant-a") <= ONE_SECOND_NANOS,
                    "tenant-a starved beyond one second of virtual time: "
                            + scheduler.maxReleaseGapNanos("tenant-a") + " ns");
            Checks.assertTrue(scheduler.maxReleaseGapNanos("tenant-b") <= ONE_SECOND_NANOS,
                    "tenant-b starved beyond one second of virtual time: "
                            + scheduler.maxReleaseGapNanos("tenant-b") + " ns");
            Checks.assertTrue(scheduler.maxReleaseGapNanos("tenant-c") <= ONE_SECOND_NANOS,
                    "tenant-c starved beyond one second of virtual time: "
                            + scheduler.maxReleaseGapNanos("tenant-c") + " ns");

            System.out.println("  completions a:b:c = " + a + ":" + b + ":" + c
                    + " (virtual seconds elapsed: " + clock.nanoTime() / ONE_SECOND_NANOS + ")");
        } finally {
            scheduler.shutdown();
        }
    }

    private static void assertShare(String name, long count, long total, double expectedShare) {
        double share = count / (double) total;
        double tolerance = expectedShare * 0.15d;
        Checks.assertTrue(Math.abs(share - expectedShare) <= tolerance,
                name + " share " + share + " is outside " + expectedShare
                        + " +/- " + tolerance + " (15% tolerance)");
    }
}
