package com.gsb.quota;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Token bucket boundary: a bucket holding exactly the quota releases exactly
 * that many tasks and not one more; the (quota+1)-th task waits in the
 * queue; the bucket never accumulates beyond its capacity.
 */
public final class TokenBucketBoundaryTest {

    private static final long QUOTA = 5L;

    private TokenBucketBoundaryTest() {
    }

    public static void run() {
        final ManualClock clock = new ManualClock();
        final Scheduler scheduler = new Scheduler(clock, 1);
        scheduler.register("tenant", QUOTA, 1);
        scheduler.start();

        final AtomicLong completed = new AtomicLong();
        Callable<Object> task = new Callable<Object>() {
            @Override
            public Object call() {
                completed.incrementAndGet();
                return null;
            }
        };

        // Idle for 3 virtual seconds: the bucket fills to its capacity
        // (exactly QUOTA) and does not grow beyond it.
        clock.advance(3000L);

        // Submitting exactly QUOTA tasks releases all of them at once...
        for (int i = 0; i < QUOTA; i++) {
            scheduler.submit("tenant", task);
        }
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() == QUOTA;
            }
        }, 5000L, "exactly QUOTA tasks released on a full bucket");

        // ...but the (QUOTA+1)-th task is NOT released: the bucket does not
        // hold a single extra token. It waits in the queue instead.
        scheduler.submit("tenant", task);
        TestSupport.holdFalse(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() > QUOTA;
            }
        }, 300L, "bucket must not release more than its capacity");
        TestSupport.check(scheduler.queuedCount("tenant") == 1L,
                "the over-quota task must be queued, not dropped");

        // 200ms at 5 tokens/s refills exactly one token: exactly one more
        // task is released.
        clock.advance(200L);
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() == QUOTA + 1;
            }
        }, 5000L, "one refilled token releases exactly one queued task");

        // Even after a long idle period the bucket caps at QUOTA tokens.
        clock.advance(10000L);
        for (int i = 0; i < QUOTA + 2; i++) {
            scheduler.submit("tenant", task);
        }
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() == QUOTA + 1 + QUOTA;
            }
        }, 5000L, "bucket capped at capacity after long idle");
        TestSupport.holdFalse(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() > QUOTA + 1 + QUOTA;
            }
        }, 300L, "bucket must not accumulate beyond capacity");
        TestSupport.check(scheduler.queuedCount("tenant") == 2L,
                "tasks beyond capacity must remain queued");

        // 400ms refills exactly two tokens: the remaining two queued tasks run.
        clock.advance(400L);
        TestSupport.awaitTrue(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return completed.get() == QUOTA + 1 + QUOTA + 2;
            }
        }, 5000L, "refilled tokens release the remaining queued tasks");

        scheduler.shutdown();
    }
}
