package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Quota enforcement: with a quota of 5 tokens/second and a permanent
 * backlog, the number of tasks released in every one-second window must
 * never exceed 5, and over-quota submissions must wait in the queue instead
 * of being dropped.
 */
public final class QuotaWindowTest {

    private static final long QUOTA = 5L;
    private static final int SUBMITTED = 40;
    private static final int SECONDS = 8;

    private QuotaWindowTest() {
    }

    public static void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 1);
        scheduler.register("tenant", QUOTA, 1);
        scheduler.start();

        final AtomicLong completed = new AtomicLong();
        final ConcurrentLinkedQueue<Long> releaseTimes = new ConcurrentLinkedQueue<Long>();
        Callable<Object> task = new Callable<Object>() {
            @Override
            public Object call() {
                releaseTimes.add(clock.millis());
                completed.incrementAndGet();
                return null;
            }
        };

        // Permanent backlog from t=0; the bucket starts empty.
        for (int i = 0; i < SUBMITTED; i++) {
            scheduler.submit("tenant", task);
        }

        for (long second = 1L; second <= SECONDS; second++) {
            clock.advance(1000L);
            final long expected = Math.min(QUOTA * second, SUBMITTED);
            TestSupport.awaitTrue(new BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return completed.get() >= expected;
                }
            }, 5000L, "released " + expected + " tasks after " + second + "s");
            final long expectedFinal = expected;
            TestSupport.holdFalse(new BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    return completed.get() > expectedFinal;
                }
            }, 200L, "released more than " + expectedFinal + " tasks within " + second + "s");
            // The remainder is still queued, not dropped.
            TestSupport.check(scheduler.queuedCount("tenant") == SUBMITTED - expected,
                    "over-quota tasks must wait in the queue, not be dropped");
        }
        scheduler.shutdown();

        TestSupport.check(completed.get() == SUBMITTED, "all submitted tasks must eventually run");

        // Per-second window accounting: no window may exceed the quota.
        long[] windows = new long[SECONDS + 1];
        for (Long t : releaseTimes) {
            windows[(int) (t.longValue() / 1000L)]++;
        }
        for (int w = 0; w < windows.length; w++) {
            TestSupport.check(windows[w] <= QUOTA,
                    "window [" + w + "s," + (w + 1) + "s) released " + windows[w]
                            + " tasks, quota is " + QUOTA);
        }
    }
}
