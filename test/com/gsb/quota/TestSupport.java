package com.gsb.quota;

import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Minimal assertion / waiting helpers (no JUnit allowed).
 */
final class TestSupport {

    private TestSupport() {
    }

    static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /** Waits (in real time, bounded) until the condition holds. */
    static void awaitTrue(BooleanSupplier condition, long timeoutMillis, String description) {
        long deadline = System.nanoTime() + timeoutMillis * 1000000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + description);
            }
            LockSupport.parkNanos(1000000L);
        }
    }

    /** Asserts that the condition stays false for the given (real) duration. */
    static void holdFalse(BooleanSupplier condition, long holdMillis, String description) {
        long deadline = System.nanoTime() + holdMillis * 1000000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                throw new AssertionError("unexpectedly became true: " + description);
            }
            LockSupport.parkNanos(1000000L);
        }
    }
}
