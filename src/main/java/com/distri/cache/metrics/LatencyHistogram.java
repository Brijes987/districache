package com.distri.cache.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A latency histogram with fixed buckets, built for being updated from many threads at once: recording is one
 * binary search and one {@link LongAdder} increment, with no lock and no allocation.
 *
 * <p>Percentiles are <b>estimates</b>. Only the bucket a sample fell into is remembered, so
 * {@link #quantile(double)} returns the upper edge of the bucket that holds the requested rank. It never reports
 * less than the true value, and is off by at most the width of one bucket (the bucket edges are roughly 2 to 2.5
 * times apart). The sum of all samples is exact. A reader that runs while writers are recording sees each bucket
 * correctly but not all buckets at the same instant, so a scrape can be off by the few samples recorded during it.
 */
public final class LatencyHistogram {

    /** Upper bucket edges in seconds. A sample larger than the last edge is counted in an extra overflow bucket. */
    static final double[] BOUNDS_SECONDS = {
        5e-6, 10e-6, 25e-6, 50e-6, 100e-6, 250e-6, 500e-6,
        1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3, 50e-3, 100e-3, 250e-3, 500e-3,
        1, 2.5, 5, 10
    };

    private static final long[] BOUNDS_NANOS = new long[BOUNDS_SECONDS.length];

    static {
        for (int i = 0; i < BOUNDS_SECONDS.length; i++) {
            BOUNDS_NANOS[i] = Math.round(BOUNDS_SECONDS[i] * 1e9);
        }
    }

    private final LongAdder[] buckets = new LongAdder[BOUNDS_NANOS.length + 1];
    private final LongAdder sumNanos = new LongAdder();

    public LatencyHistogram() {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /** Records one sample. A negative duration (clock oddity) is counted as zero. */
    public void recordNanos(long nanos) {
        long n = Math.max(0, nanos);
        int lo = 0;
        int hi = BOUNDS_NANOS.length; // hi == length means "overflow bucket"
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (n <= BOUNDS_NANOS[mid]) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        buckets[lo].increment();
        sumNanos.add(n);
    }

    /** Samples in each bucket (not cumulative); the last entry is the overflow bucket. */
    long[] counts() {
        long[] out = new long[buckets.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = buckets[i].sum();
        }
        return out;
    }

    public long count() {
        long total = 0;
        for (LongAdder b : buckets) {
            total += b.sum();
        }
        return total;
    }

    public double sumSeconds() {
        return sumNanos.sum() / 1e9;
    }

    /**
     * Estimated latency in seconds at quantile {@code q} (0 &lt; q &le; 1): the upper edge of the bucket holding
     * that rank. NaN when nothing was recorded; positive infinity when the rank falls in the overflow bucket
     * (slower than the last edge, 10 seconds).
     */
    public double quantile(double q) {
        if (!(q > 0 && q <= 1)) {
            throw new IllegalArgumentException("quantile must be in (0, 1]: " + q);
        }
        return quantile(counts(), q);
    }

    static double quantile(long[] counts, double q) {
        long total = 0;
        for (long c : counts) {
            total += c;
        }
        if (total == 0) {
            return Double.NaN;
        }
        long rank = (long) Math.ceil(q * total); // 1-based rank of the sample we want
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= rank) {
                return i < BOUNDS_SECONDS.length ? BOUNDS_SECONDS[i] : Double.POSITIVE_INFINITY;
            }
        }
        return Double.POSITIVE_INFINITY; // unreachable
    }
}
