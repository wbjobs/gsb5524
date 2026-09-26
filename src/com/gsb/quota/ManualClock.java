package com.gsb.quota;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-friendly virtual clock. Time only moves when one of the advance
 * methods is called, and registered tick listeners are notified after each
 * advance so the scheduler can re-evaluate its state immediately.
 */
public final class ManualClock implements Clock {

    private final List<Runnable> listeners = new CopyOnWriteArrayList<Runnable>();
    private long nanos;

    @Override
    public synchronized long nanoTime() {
        return nanos;
    }

    @Override
    public void addTickListener(Runnable listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public void advanceNanos(long deltaNanos) {
        if (deltaNanos < 0L) {
            throw new IllegalArgumentException("deltaNanos must be >= 0");
        }
        synchronized (this) {
            nanos += deltaNanos;
        }
        for (Runnable listener : listeners) {
            listener.run();
        }
    }

    public void advanceMillis(long deltaMillis) {
        advanceNanos(deltaMillis * 1000000L);
    }

    public void advanceSeconds(long deltaSeconds) {
        advanceNanos(deltaSeconds * 1000000000L);
    }
}
