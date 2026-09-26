package com.gsb.quota;

import java.util.concurrent.locks.LockSupport;

/**
 * Real-time clock backed by {@link System#nanoTime()} (monotonic).
 */
public final class SystemClock implements Clock {

    private static final long MAX_PARK_MILLIS = 60000L;

    private final long startNanos = System.nanoTime();

    @Override
    public long millis() {
        return (System.nanoTime() - startNanos) / 1000000L;
    }

    @Override
    public void await(long deadlineMillis) {
        while (true) {
            long remaining = deadlineMillis - millis();
            if (remaining <= 0L) {
                return;
            }
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            LockSupport.parkNanos(Math.min(remaining, MAX_PARK_MILLIS) * 1000000L);
        }
    }
}
