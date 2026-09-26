package com.gsb.quota;

/**
 * Time source used by the scheduler. All scheduler time reads go through this
 * interface so tests can drive the scheduler with virtual time.
 */
public interface Clock {

    /** Current time in milliseconds. */
    long millis();

    /**
     * Blocks the calling thread until {@code millis() >= deadlineMillis},
     * or until the thread is interrupted. A deadline of
     * {@link Long#MAX_VALUE} means "wait until interrupted".
     */
    void await(long deadlineMillis) throws InterruptedException;
}
