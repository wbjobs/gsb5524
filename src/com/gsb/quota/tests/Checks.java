package com.gsb.quota.tests;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Minimal assertion / waiting helpers for the test suite. Real-time waits
 * appear only here to observe the worker threads; all scheduler behaviour
 * under test is driven by the virtual ManualClock.
 */
final class Checks {

    private Checks() {
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static void assertEquals(long expected, long actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " expected=" + expected + " actual=" + actual);
        }
    }

    static void expectThrow(Class<? extends Throwable> type, ThrowingRunnable runnable,
                            String message) {
        try {
            runnable.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                return;
            }
            throw new AssertionError(message + " wrong exception: " + t);
        }
        throw new AssertionError(message + " expected " + type.getSimpleName() + " but nothing was thrown");
    }

    static void waitUntil(BooleanSupplier condition, long timeoutMillis, String message) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0L) {
                throw new AssertionError("timed out waiting: " + message);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
        }
    }

    static void assertStable(BooleanSupplier condition, long durationMillis, String message) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMillis);
        while (System.nanoTime() - deadline < 0L) {
            if (!condition.getAsBoolean()) {
                throw new AssertionError(message);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
        }
    }
}
