package com.gsb.quota.tests;

import java.util.concurrent.TimeUnit;

/**
 * Plain main()-based test entry point (no JUnit). Exits non-zero on failure.
 */
public final class TestRunner {

    private TestRunner() {
    }

    private interface TestCase {
        void run() throws Exception;
    }

    public static void main(String[] args) {
        int failures = 0;
        failures += run("ApiContractTest", new TestCase() {
            @Override
            public void run() throws Exception {
                ApiContractTest.run();
            }
        });
        failures += run("TokenBucketBoundaryTest", new TestCase() {
            @Override
            public void run() throws Exception {
                TokenBucketBoundaryTest.run();
            }
        });
        failures += run("QuotaWindowTest", new TestCase() {
            @Override
            public void run() throws Exception {
                QuotaWindowTest.run();
            }
        });
        failures += run("WeightedFairnessTest", new TestCase() {
            @Override
            public void run() throws Exception {
                WeightedFairnessTest.run();
            }
        });
        if (failures > 0) {
            System.out.println(failures + " test(s) FAILED");
            System.exit(1);
        }
        System.out.println("ALL TESTS PASSED");
    }

    private static int run(String name, TestCase test) {
        long start = System.nanoTime();
        try {
            test.run();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println("[PASS] " + name + " (" + elapsedMs + " ms)");
            return 0;
        } catch (Throwable t) {
            System.out.println("[FAIL] " + name);
            t.printStackTrace(System.out);
            return 1;
        }
    }
}
