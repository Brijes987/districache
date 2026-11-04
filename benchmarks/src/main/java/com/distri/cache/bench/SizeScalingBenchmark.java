package com.distri.cache.bench;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import com.distri.cache.LegacyLRUCache;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

/**
 * Shows how the cost of an operation grows with the number of entries, for the current LRUCache and
 * the earlier implementation (LegacyLRUCache).
 *
 * The cache is filled to capacity first, then every put uses a key that is not present, so every put
 * has to evict. Run with a single thread: java -jar target/benchmarks.jar SizeScalingBenchmark -t 1
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class SizeScalingBenchmark {

    @Param({"current", "legacy"})
    public String impl;

    @Param({"1000", "10000", "100000"})
    public int size;

    private Cache<Integer, Integer> cache;
    private int next;

    @Setup(Level.Trial)
    public void setup() {
        cache = impl.equals("current")
                ? new LRUCache<>(size, 0)
                : new LegacyLRUCache<>(size, 3_600_000L); // legacy needs an interval > 0; keep it out of the way
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }
        next = size;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cache.shutdown();
    }

    /** Insert a new key into a full cache: forces one eviction per call. */
    @Benchmark
    public void putNewKey() {
        cache.put(next, next);
        next++;
    }

    @Benchmark
    public int sizeCall() {
        return cache.size();
    }
}
