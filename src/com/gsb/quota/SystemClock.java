package com.gsb.quota;

/**
 * Default clock backed by the monotonic system nano timer.
 */
public final class SystemClock implements Clock {

    public static final SystemClock INSTANCE = new SystemClock();

    private SystemClock() {
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
