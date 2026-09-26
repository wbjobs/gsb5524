package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Weighted fairness: three tenants with weights 1:2:3 and (effectively
 * unlimited) equal quotas submit a large backlog each. While virtual time
 * advances, the completion stream must interleave so that every window of
 * 600 consecutive completions splits 100:200:300 within 15%, and no tenant
 * ever waits more than one second of virtual time between completions.
 */
public final class FairnessTest {

    private static final int TENANTS = 3;
    private static final int[] WEIGHTS = {1, 2, 3};
    private static final int WEIGHT_SUM = 6;
    private static final int PER_TENANT = 3000;
    private static final int TOTAL = PER_TENANT * TENANTS;
    private static final int WINDOW = 600;
    private static final double TOLERANCE = 0.15;
    private static final long STARVATION_LIMIT_MILLIS = 1000L;

    private FairnessTest() {
    }

    public static void run() {
        final ManualClock clock = new ManualClock();
        // A single worker makes completion order exactly equal to dispatch
        // order, so the assertions measure the WFQ policy itself.
        final Scheduler scheduler = new Scheduler(clock, 1);
        final String[] ids = {"alpha", "beta", "gamma"};
        for (int i = 0; i < TENANTS; i++) {
            // Quotas are huge so tokens never limit this test; only the WFQ
            // policy shapes the completion order.
            scheduler.register(ids[i], 1000000L, WEIGHTS[i]);
        }

        final AtomicLong sequence = new AtomicLong();
        final AtomicLong[] perTenantDone = new AtomicLong[TENANTS];
        final int[] completionOrder = new int[TOTAL];
        final long[] lastCompletionAt = new long[TENANTS];
        final long[] maxGap = new long[TENANTS];
        for (int i = 0; i < TENANTS; i++) {
            perTenantDone[i] = new AtomicLong();
            lastCompletionAt[i] = -1L;
        }

        // Submit the whole backlog before start() so every task competes
        // under the WFQ policy from the same virtual time.
        for (int t = 0; t < TENANTS; t++) {
            final int tenantIndex = t;
            for (int k = 0; k < PER_TENANT; k++) {
                scheduler.submit(ids[t], new Callable<Object>() {
                    @Override
                    public Object call() {
                        long now = clock.millis();
                        long prev = lastCompletionAt[tenantIndex];
                        if (prev >= 0L) {
                            long gap = now - prev;
                            if (gap > maxGap[tenantIndex]) {
                                maxGap[tenantIndex] = gap;
                            }
                        }
                        lastCompletionAt[tenantIndex] = now;
                        int pos = (int) sequence.getAndIncrement();
                        completionOrder[pos] = tenantIndex;
                        perTenantDone[tenantIndex].incrementAndGet();
                        return null;
                    }
                });
            }
        }

        scheduler.start();

        // Drive virtual time: a 1ms tick plus 1 extra virtual millisecond per
        // completion, so the run spans roughly TOTAL virtual milliseconds
        // (~9 seconds) and starvation gaps are measured meaningfully.
        long credited = 0L;
        while (sequence.get() < TOTAL) {
            long done = sequence.get();
            long delta = done - credited;
            credited = done;
            clock.advance(1L + Math.max(0L, delta));
            LockSupport.parkNanos(200000L);
        }
        scheduler.shutdown();

        // Every tenant finished its whole backlog: nothing was dropped.
        for (int t = 0; t < TENANTS; t++) {
            TestSupport.check(perTenantDone[t].get() == PER_TENANT,
                    "tenant " + ids[t] + " must complete its whole backlog");
        }

        // Fairness: every window of 600 consecutive completions must split
        // 1:2:3 within 15% of the target shares.
        for (int start = 0; start < TOTAL; start += WINDOW) {
            long[] counts = new long[TENANTS];
            for (int i = start; i < start + WINDOW; i++) {
                counts[completionOrder[i]]++;
            }
            for (int t = 0; t < TENANTS; t++) {
                double expected = (double) WINDOW * WEIGHTS[t] / WEIGHT_SUM;
                double ratio = counts[t] / expected;
                TestSupport.check(Math.abs(ratio - 1.0) <= TOLERANCE,
                        "window starting at completion " + start + ": tenant " + ids[t]
                                + " got " + counts[t] + " completions, expected ~" + expected
                                + " (weight share " + WEIGHTS[t] + "/" + WEIGHT_SUM + ")");
            }
        }

        // No starvation: no tenant ever waited more than one second of
        // virtual time between two consecutive completions.
        for (int t = 0; t < TENANTS; t++) {
            TestSupport.check(maxGap[t] <= STARVATION_LIMIT_MILLIS,
                    "tenant " + ids[t] + " starved for " + maxGap[t]
                            + " virtual ms, limit is " + STARVATION_LIMIT_MILLIS);
        }

        // The run really did span multiple seconds of virtual time.
        TestSupport.check(clock.millis() >= TOTAL,
                "virtual time must advance through the run, got " + clock.millis() + " ms");
    }
}
