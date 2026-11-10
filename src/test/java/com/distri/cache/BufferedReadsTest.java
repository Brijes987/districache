package com.distri.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Buffered-read mode: hits are recorded and applied to the LRU order in batches. With a single thread nothing is
 * ever dropped, so it must behave exactly like the default mode; with many threads the structure must stay intact
 * and every value must stay correct, even though recency becomes approximate.
 */
class BufferedReadsTest {

    private static List<String> order(LRUCache<String, String> cache) {
        List<String> keys = new ArrayList<>();
        for (CacheSnapshotEntry<String, String> e : cache.snapshotLruToMru()) {
            keys.add(e.key());
        }
        return keys;
    }

    @Test
    void theDefaultIsExactAndTheFlagIsReported() {
        try (LRUCache<String, String> exact = new LRUCache<>(4, 0);
                LRUCache<String, String> buffered = new LRUCache<>(4, 0, true)) {
            assertFalse(exact.bufferedReads());
            assertTrue(buffered.bufferedReads());
        }
    }

    @Test
    void aHitProtectsAnEntryFromEviction() {
        try (LRUCache<String, String> c = new LRUCache<>(2, 0, true)) {
            c.put("a", "1");
            c.put("b", "2");
            assertEquals(Optional.of("1"), c.get("a")); // a is now more recent than b
            c.put("c", "3");                              // must evict b, not a
            assertEquals(Optional.of("1"), c.get("a"));
            assertEquals(Optional.empty(), c.get("b"));
            assertEquals(Optional.of("3"), c.get("c"));
            c.verifyInternalConsistency();
        }
    }

    @Test
    void aSnapshotSeesRecentHits() {
        try (LRUCache<String, String> c = new LRUCache<>(4, 0, true)) {
            c.put("a", "1");
            c.put("b", "2");
            c.put("c", "3");
            c.get("a");
            c.get("b");
            assertEquals(List.of("c", "a", "b"), order(c)); // least to most recent
        }
    }

    @Test
    void anEntryRemovedAfterAHitIsNotBroughtBack() {
        try (LRUCache<String, String> c = new LRUCache<>(4, 0, true)) {
            c.put("a", "1");
            c.put("b", "2");
            c.get("a");
            c.remove("a");
            c.drainReadBuffer();
            assertEquals(Optional.empty(), c.get("a"));
            assertEquals(1, c.size());
            c.verifyInternalConsistency();
        }
    }

    @Test
    void clearAfterAHitLeavesAConsistentEmptyCache() {
        try (LRUCache<String, String> c = new LRUCache<>(4, 0, true)) {
            c.put("a", "1");
            c.put("b", "2");
            c.get("a");
            c.get("b");
            c.clear();
            c.drainReadBuffer();
            assertEquals(0, c.size());
            c.verifyInternalConsistency();
            c.put("x", "9");
            assertEquals(List.of("x"), order(c));
        }
    }

    @Test
    void anEntryReplacedByDeleteAndPutKeepsTheNewValue() {
        try (LRUCache<String, String> c = new LRUCache<>(4, 0, true)) {
            c.put("a", "old");
            c.get("a");
            assertTrue(c.delete("a"));
            c.put("a", "new");
            c.drainReadBuffer();
            assertEquals(Optional.of("new"), c.get("a"));
            c.verifyInternalConsistency();
        }
    }

    @Test
    void anOverwriteIsVisibleToTheNextGet() {
        try (LRUCache<String, String> c = new LRUCache<>(4, 0, true)) {
            c.put("a", "1");
            c.get("a");
            c.put("a", "2");
            assertEquals(Optional.of("2"), c.get("a"));
        }
    }

    @Test
    void anExpiredEntryIsAbsentAndIsReclaimedByThePut() throws Exception {
        try (LRUCache<String, String> c = new LRUCache<>(2, 0, true)) {
            c.put("t", "x", 30);
            c.put("k", "y");
            assertEquals(Optional.of("x"), c.get("t"));
            Thread.sleep(80);
            assertEquals(Optional.empty(), c.get("t"));
            c.put("n", "z");              // purges the expired entry: nothing valid is evicted
            assertEquals(Optional.of("y"), c.get("k"));
            assertEquals(Optional.of("z"), c.get("n"));
            c.verifyInternalConsistency();
        }
    }

    /** One thread: the buffered cache and the exact cache must give identical answers and identical order. */
    @Test
    void singleThreadedBufferedModeIsIdenticalToExactLru() {
        for (int seed = 0; seed < 40; seed++) {
            Random random = new Random(seed);
            int capacity = 1 + random.nextInt(12);
            int keys = 1 + random.nextInt(30);
            try (LRUCache<String, String> exact = new LRUCache<>(capacity, 0);
                    LRUCache<String, String> buffered = new LRUCache<>(capacity, 0, true)) {
                for (int step = 0; step < 3000; step++) {
                    String key = "k" + random.nextInt(keys);
                    switch (random.nextInt(10)) {
                        case 0, 1, 2 -> {
                            String value = "v" + step;
                            exact.put(key, value);
                            buffered.put(key, value);
                        }
                        case 3 -> {
                            exact.remove(key);
                            buffered.remove(key);
                        }
                        case 4 -> assertEquals(exact.delete(key), buffered.delete(key));
                        default -> assertEquals(exact.get(key), buffered.get(key), "seed " + seed + " step " + step);
                    }
                    assertEquals(exact.size(), buffered.size());
                    if (step % 97 == 0) {
                        assertEquals(order(exact), order(buffered), "seed " + seed + " step " + step);
                    }
                }
                assertEquals(order(exact), order(buffered), "seed " + seed);
                buffered.verifyInternalConsistency();
            }
        }
    }

    @Test
    void shardedBufferedModeAnswersLikeShardedExactWithOneThread() {
        Random random = new Random(7);
        try (ShardedLRUCache<String, String> exact = new ShardedLRUCache<>(40, 4, 0);
                ShardedLRUCache<String, String> buffered = new ShardedLRUCache<>(40, 4, 0, true)) {
            for (int step = 0; step < 20_000; step++) {
                String key = "k" + random.nextInt(120);
                if (random.nextInt(4) == 0) {
                    exact.put(key, "v" + step);
                    buffered.put(key, "v" + step);
                } else {
                    assertEquals(exact.get(key), buffered.get(key), "step " + step);
                }
                assertEquals(exact.size(), buffered.size());
            }
        }
    }

    /** Many threads: values stay correct, capacity holds, and the internal structure stays intact. */
    @Test
    void concurrentUseKeepsValuesCapacityAndStructure() throws Exception {
        int threads = 8;
        int capacity = 50;
        try (LRUCache<Integer, Integer> c = new LRUCache<>(capacity, 0, true)) {
            Thread[] workers = new Thread[threads];
            String[] problem = new String[1];
            for (int t = 0; t < threads; t++) {
                final long seed = t;
                workers[t] = new Thread(() -> {
                    Random random = new Random(seed);
                    for (int i = 0; i < 200_000 && problem[0] == null; i++) {
                        int key = random.nextInt(200);
                        switch (random.nextInt(10)) {
                            case 0 -> c.put(key, key);
                            case 1 -> c.remove(key);
                            default -> {
                                Optional<Integer> hit = c.get(key);
                                if (hit.isPresent() && hit.get() != key) {
                                    problem[0] = "key " + key + " returned " + hit.get();
                                }
                            }
                        }
                        if (c.size() > capacity) {
                            problem[0] = "size above capacity: " + c.size();
                        }
                    }
                });
                workers[t].start();
            }
            for (Thread w : workers) {
                w.join();
            }
            assertEquals(null, problem[0]);
            c.drainReadBuffer();
            c.verifyInternalConsistency();
            assertTrue(c.size() <= capacity);
        }
    }
}
