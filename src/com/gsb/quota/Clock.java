package com.gsb.quota;

/**
 * Time source used by the scheduler. All scheduling decisions read time
 * exclusively through this interface, so tests can inject a virtual clock.
 */
public interface Clock {

    /**
     * @return current time in nanoseconds, monotonically non-decreasing.
     */
    long nanoTime();

    /**
     * Registers a callback that is invoked whenever this clock's time may
     * have advanced. Clocks backed by real time do not need to support this;
     * the scheduler falls back to timed waits for them.
     */
    default void addTickListener(Runnable listener) {
    }
}
