package com.gsb.quota;

/**
 * Plain-Java test runner (no JUnit). Exits non-zero if any test fails.
 */
public final class TestMain {

    private TestMain() {
    }

    private interface TestCase {
        void run();
    }

    public static void main(String[] args) {
        int passed = 0;
        int failed = 0;

        String[] names = {
                "SchedulerBasicTest",
                "TokenBucketBoundaryTest",
                "QuotaWindowTest",
                "FairnessTest",
        };
        TestCase[] tests = {
                new TestCase() { @Override public void run() { SchedulerBasicTest.run(); } },
                new TestCase() { @Override public void run() { TokenBucketBoundaryTest.run(); } },
                new TestCase() { @Override public void run() { QuotaWindowTest.run(); } },
                new TestCase() { @Override public void run() { FairnessTest.run(); } },
        };

        for (int i = 0; i < tests.length; i++) {
            long started = System.nanoTime();
            try {
                tests[i].run();
                passed++;
                System.out.println("PASS " + names[i]
                        + " (" + (System.nanoTime() - started) / 1000000L + " ms)");
            } catch (Throwable t) {
                failed++;
                System.out.println("FAIL " + names[i] + ": " + t);
                t.printStackTrace(System.out);
            }
        }

        System.out.println("----");
        System.out.println("passed: " + passed + ", failed: " + failed);
        System.exit(failed == 0 ? 0 : 1);
    }
}
