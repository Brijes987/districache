package com.distri.cache.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import org.junit.jupiter.api.Test;

class ShardedLRUCacheTest {

    private static <K, V> ShardedLRUCache<K, V> cache(int capacity, int shards) {
        return new ShardedLRUCache<>(capacity, shards, 0); // no background thread: deterministic
    }

    @Test
    void basicPutGetRemove() {
        ShardedLRUCache<String, Integer> c = cache(100, 4);
        c.put("a", 1);
        c.put("b", 2);
        assertEquals(1, c.get("a").orElseThrow());
        assertEquals(2, c.get("b").orElseThrow());
        c.remove("a");
        assertTrue(c.get("a").isEmpty());
        assertEquals(1, c.size());
        c.verifyInternalConsistency();
    }

    @Test
    void shardCountIsRoundedDownToPowerOfTwo() {
        assertEquals(4, cache(100, 6).shardCount());
        assertEquals(8, cache(100, 8).shardCount());
        assertEquals(1, cache(100, 1).shardCount());
    }

    @Test
    void shardCountIsCappedByCapacity() {
        assertEquals(4, cache(5, 64).shardCount());   // 5 entries cannot feed more than 4 power-of-two shards
        assertEquals(1, cache(1, 16).shardCount());
    }

    @Test
    void shardCapacitiesSumToTotalCapacity() {
        for (int capacity : new int[] {1, 7, 10, 33, 100, 1000}) {
            ShardedLRUCache<Integer, Integer> c = cache(capacity, 16);
            int sum = 0;
            int min = Integer.MAX_VALUE;
            int max = 0;
            for (int i = 0; i < c.shardCount(); i++) {
                int sc = c.shardCapacity(i);
                sum += sc;
                min = Math.min(min, sc);
                max = Math.max(max, sc);
            }
            assertEquals(capacity, sum, "capacity " + capacity);
            assertTrue(max - min <= 1, "shards should differ by at most one entry");
            assertEquals(capacity, c.capacity());
        }
    }

    @Test
    void sameKeyAlwaysMapsToSameShard() {
        ShardedLRUCache<String, Integer> c = cache(100, 8);
        int first = c.shardIndexOf("hello");
        for (int i = 0; i < 10; i++) {
            assertEquals(first, c.shardIndexOf("hello"));
        }
        assertTrue(first >= 0 && first < c.shardCount());
    }

    @Test
    void sequentialIntegerKeysSpreadAcrossAllShards() {
        ShardedLRUCache<Integer, Integer> c = cache(1000, 8);
        boolean[] used = new boolean[c.shardCount()];
        for (int k = 0; k < 64; k++) {
            used[c.shardIndexOf(k)] = true;
        }
        for (boolean u : used) {
            assertTrue(u, "every shard should receive some of 64 sequential keys");
        }
    }

    @Test
    void sizeNeverExceedsCapacity() {
        ShardedLRUCache<Integer, Integer> c = cache(50, 8);
        for (int i = 0; i < 5_000; i++) {
            c.put(i, i);
            assertTrue(c.size() <= 50);
        }
        c.verifyInternalConsistency();
    }

    @Test
    void evictsLeastRecentlyUsedWithinAShard() {
        ShardedLRUCache<Integer, Integer> c = cache(8, 1); // one shard = ordinary global LRU
        for (int i = 0; i < 8; i++) c.put(i, i);
        c.get(0);
        c.put(100, 100);
        assertTrue(c.get(0).isPresent());
        assertTrue(c.get(1).isEmpty());
    }

    @Test
    void validationRejectsNullsBadTtlAndBadConstructorArguments() {
        ShardedLRUCache<String, String> c = cache(10, 2);
        assertThrows(IllegalArgumentException.class, () -> c.put(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> c.put("k", null));
        assertThrows(IllegalArgumentException.class, () -> c.get(null));
        assertThrows(IllegalArgumentException.class, () -> c.remove(null));
        assertThrows(IllegalArgumentException.class, () -> c.put("k", "v", 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardedLRUCache<String, String>(0, 4, 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardedLRUCache<String, String>(10, 0, 0));
    }

    @Test
    void ttlExpiryWorksAcrossShards() throws Exception {
        ShardedLRUCache<Integer, Integer> c = cache(100, 4);
        for (int i = 0; i < 20; i++) c.put(i, i, 30);
        c.put(999, 999);
        Thread.sleep(80);
        assertEquals(1, c.size());
        assertEquals(20, c.cleanupExpiredEntries());
        assertEquals(1, c.physicalSize());
        c.verifyInternalConsistency();
    }

    /**
     * Regression: shard choice must not be correlated with the low hash bits that a shard's own HashMap
     * uses for buckets. If it were, all keys in one shard would share their low bits and pile into a few
     * buckets (measured as a ~2.5x slowdown on put).
     */
    @Test
    void keysInOneShardDoNotShareTheirLowHashBits() {
        ShardedLRUCache<Integer, Integer> c = cache(1000, 16);
        boolean[] seen = new boolean[16];
        for (int k = 0; k < 10_000; k++) {
            if (c.shardIndexOf(k) == 0) {
                seen[k & 15] = true;
            }
        }
        int distinct = 0;
        for (boolean b : seen) {
            if (b) distinct++;
        }
        assertTrue(distinct >= 12, "keys of shard 0 use only " + distinct + " of 16 low-bit patterns");
    }

    @Test
    void shardsReceiveRoughlyEqualShareOfSequentialKeys() {
        ShardedLRUCache<Integer, Integer> c = cache(1000, 16);
        int[] counts = new int[16];
        for (int k = 0; k < 16_000; k++) {
            counts[c.shardIndexOf(k)]++;
        }
        for (int count : counts) {
            assertTrue(count > 700 && count < 1300, "uneven distribution: " + count);
        }
    }

    @Test
    void shutdownIsIdempotentAndCacheKeepsWorking() {
        ShardedLRUCache<String, Integer> c = new ShardedLRUCache<>(10, 4, 20);
        c.shutdown();
        c.close();
        c.put("a", 1);
        assertEquals(1, c.get("a").orElseThrow());
    }
}
