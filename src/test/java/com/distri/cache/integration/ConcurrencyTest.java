package com.distri.cache.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.LRUCache;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * These tests check: no exceptions, no deadlock, size never above capacity, and an intact internal
 * structure afterwards. They do NOT prove linearizability.
 */
class ConcurrencyTest {

    private static final int THREADS = 8;

    /** Few keys + small capacity = constant contention and constant eviction. */
    @Test
    void contendedMixedWorkloadKeepsInvariants() throws Exception {
        LRUCache<Integer, Integer> c = new LRUCache<>(32, 5);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger maxSeen = new AtomicInteger();
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final int seed = t;
            fs.add(pool.submit(() -> {
                Random r = new Random(seed);
                start.await();
                for (int i = 0; i < 50_000; i++) {
                    int k = r.nextInt(128);
                    switch (r.nextInt(5)) {
                        case 0 -> c.put(k, i);
                        case 1 -> c.put(k, i, 1 + r.nextInt(4));
                        case 2, 3 -> c.get(k);
                        default -> c.remove(k);
                    }
                    if (i % 500 == 0) maxSeen.accumulateAndGet(c.size(), Math::max);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS); // a deadlock shows up as TimeoutException
        pool.shutdown();
        assertTrue(maxSeen.get() <= 32, "size exceeded capacity: " + maxSeen.get());
        c.verifyInternalConsistency();
        c.shutdown();
    }

    @Test
    void sameKeyHammered() throws Exception {
        LRUCache<String, Integer> c = new LRUCache<>(2, 0);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final int id = t;
            fs.add(pool.submit(() -> {
                for (int i = 0; i < 50_000; i++) {
                    c.put("hot", id);
                    c.get("hot");
                    if (i % 7 == 0) c.remove("hot");
                }
                return null;
            }));
        }
        for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(c.size() <= 1);
        c.verifyInternalConsistency();
    }

    @Test
    void concurrentReadersOnPreloadedCacheSeeCorrectValues() throws Exception {
        LRUCache<Integer, Integer> c = new LRUCache<>(100, 0);
        for (int i = 0; i < 100; i++) c.put(i, i * 10);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<Boolean>> fs = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final int seed = t;
            fs.add(pool.submit(() -> {
                Random r = new Random(seed);
                for (int i = 0; i < 100_000; i++) {
                    int k = r.nextInt(100);
                    if (c.get(k).orElse(-1) != k * 10) return false;
                }
                return true;
            }));
        }
        for (Future<Boolean> f : fs) assertTrue(f.get(60, TimeUnit.SECONDS), "a reader saw a wrong value");
        pool.shutdown();
        assertEquals(100, c.size());
        c.verifyInternalConsistency();
    }
}
