package com.distri.cache.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LatencyHistogramTest {

    @Test
    void anEmptyHistogramHasNoQuantile() {
        LatencyHistogram h = new LatencyHistogram();
        assertEquals(0, h.count());
        assertTrue(Double.isNaN(h.quantile(0.5)));
    }

    @Test
    void aSampleIsReportedAsTheUpperEdgeOfItsBucket() {
        LatencyHistogram h = new LatencyHistogram();
        h.recordNanos(30_000); // 30 microseconds: the 25us..50us bucket
        assertEquals(50e-6, h.quantile(0.5), 0.0);
        h.recordNanos(5_000); // exactly on an edge belongs to that edge's bucket
        assertEquals(2, h.count());
        assertEquals(5e-6, h.quantile(0.5), 0.0); // rank 1 of 2 is the 5us sample
        assertEquals(50e-6, h.quantile(1.0), 0.0);
    }

    @Test
    void theRankPicksTheRightBucket() {
        LatencyHistogram h = new LatencyHistogram();
        for (int i = 0; i < 99; i++) {
            h.recordNanos(30_000);
        }
        h.recordNanos(3_000_000_000L); // one 3 second outlier
        assertEquals(50e-6, h.quantile(0.5), 0.0);
        assertEquals(50e-6, h.quantile(0.99), 0.0); // rank 99 of 100: still a fast one
        assertEquals(5.0, h.quantile(1.0), 0.0);     // the outlier is in the 2.5s..5s bucket
    }

    @Test
    void aSampleBeyondTheLastEdgeIsInfinite() {
        LatencyHistogram h = new LatencyHistogram();
        h.recordNanos(60_000_000_000L);
        assertEquals(Double.POSITIVE_INFINITY, h.quantile(0.5), 0.0);
    }

    @Test
    void aNegativeDurationCountsAsZero() {
        LatencyHistogram h = new LatencyHistogram();
        h.recordNanos(-5);
        assertEquals(1, h.count());
        assertEquals(0.0, h.sumSeconds(), 0.0);
        assertEquals(5e-6, h.quantile(0.5), 0.0);
    }

    @Test
    void invalidQuantilesAreRejected() {
        LatencyHistogram h = new LatencyHistogram();
        assertThrows(IllegalArgumentException.class, () -> h.quantile(0));
        assertThrows(IllegalArgumentException.class, () -> h.quantile(1.01));
        assertThrows(IllegalArgumentException.class, () -> h.quantile(Double.NaN));
    }

    /** The promise in the class comment: never below the true value, and at most one bucket above it. */
    @Test
    void theEstimateIsNeverBelowTheExactValueAndNeverMoreThanOneBucketAbove() {
        Random random = new Random(42);
        for (int round = 0; round < 50; round++) {
            LatencyHistogram h = new LatencyHistogram();
            int n = 1 + random.nextInt(2000);
            long[] samples = new long[n];
            for (int i = 0; i < n; i++) {
                // log-uniform between 1 microsecond and 5 seconds
                samples[i] = (long) Math.exp(Math.log(1_000) + random.nextDouble() * (Math.log(5_000_000_000L) - Math.log(1_000)));
                h.recordNanos(samples[i]);
            }
            Arrays.sort(samples);
            for (double q : new double[] {0.5, 0.9, 0.99, 0.999, 1.0}) {
                long exactNanos = samples[(int) Math.ceil(q * n) - 1];
                double estimate = h.quantile(q);
                assertTrue(estimate * 1e9 >= exactNanos - 1, "estimate below exact: q=" + q);
                // the edge below the estimate must be below the exact value, so the error is one bucket at most
                int edge = Arrays.binarySearch(LatencyHistogram.BOUNDS_SECONDS, estimate);
                assertTrue(edge >= 0, "estimate is always a bucket edge");
                if (edge > 0) {
                    assertTrue(LatencyHistogram.BOUNDS_SECONDS[edge - 1] * 1e9 < exactNanos, "estimate more than one bucket high");
                }
            }
        }
    }

    @Test
    void concurrentRecordingLosesNothing() throws Exception {
        LatencyHistogram h = new LatencyHistogram();
        int threads = 8;
        int perThread = 100_000;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    h.recordNanos(1_000 + (i % 7) * 100_000L);
                }
            });
            workers[t].start();
        }
        for (Thread w : workers) {
            w.join();
        }
        assertEquals((long) threads * perThread, h.count());
        long expectedNanos = 0;
        for (int i = 0; i < perThread; i++) {
            expectedNanos += 1_000 + (i % 7) * 100_000L;
        }
        assertEquals(expectedNanos * threads / 1e9, h.sumSeconds(), 1e-6);
    }
}
