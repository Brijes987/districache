package com.distri.cache.bench;

import com.distri.cache.BaselineCache;
import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

/**
 * Compares LRUCache with a plain ConcurrentHashMap wrapper. The baseline has no eviction and no
 * ordering, so it is an upper bound on speed, not a like-for-like competitor: the interesting
 * number is how much the LRU bookkeeping and locking cost.
 *
 * Run:  java -jar target/benchmarks.jar -t 1   (single thread)
 *       java -jar target/benchmarks.jar -t 4   (4 threads)
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class CacheBenchmark {

    private static final int KEY_SPACE = 10_000;

    @Param({"lru", "baseline"})
    public String impl;

    /** Cache capacity as a fraction of the key space: smaller = more misses and evictions. */
    @Param({"1.0", "0.1"})
    public double capacityRatio;

    private Cache<Integer, Integer> cache;

    @Setup(Level.Trial)
    public void setup() {
        int capacity = Math.max(1, (int) (KEY_SPACE * capacityRatio));
        cache = impl.equals("lru") ? new LRUCache<>(capacity, 0) : new BaselineCache<>();
        for (int i = 0; i < KEY_SPACE; i++) {
            cache.put(i, i);
        }
    }

    @Benchmark
    public Object get() {
        return cache.get(ThreadLocalRandom.current().nextInt(KEY_SPACE));
    }

    @Benchmark
    public void put() {
        int k = ThreadLocalRandom.current().nextInt(KEY_SPACE);
        cache.put(k, k);
    }

    /** 90% reads, 10% writes. */
    @Benchmark
    public Object mixed90_10() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int k = r.nextInt(KEY_SPACE);
        if (r.nextInt(10) == 0) {
            cache.put(k, k);
            return null;
        }
        return cache.get(k);
    }
}
