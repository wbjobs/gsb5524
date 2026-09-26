package com.gsb.quota;

/**
 * Test clock that only moves when {@link #advance(long)} is called.
 * Threads blocked in {@link #await(long)} wake up as soon as the virtual
 * time reaches their deadline (or when interrupted).
 */
public final class ManualClock implements Clock {

    private static final long MAX_WAIT_MILLIS = 1000L;

    private long now;

    @Override
    public synchronized long millis() {
        return now;
    }

    public synchronized void advance(long deltaMillis) {
        if (deltaMillis < 0L) {
            throw new IllegalArgumentException("deltaMillis must be >= 0");
        }
        now += deltaMillis;
        notifyAll();
    }

    @Override
    public synchronized void await(long deadlineMillis) throws InterruptedException {
        while (now < deadlineMillis) {
            if (deadlineMillis == Long.MAX_VALUE) {
                wait();
            } else {
                wait(Math.min(deadlineMillis - now, MAX_WAIT_MILLIS));
            }
        }
    }
}
