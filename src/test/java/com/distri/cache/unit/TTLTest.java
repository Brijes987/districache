package com.distri.cache.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

class TTLTest {

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void entryVisibleBeforeExpiryAndGoneAfter() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("k", 1, 300); // generous, so a slow machine cannot expire it before the first check
        assertTrue(c.get("k").isPresent());
        sleep(700);
        assertTrue(c.get("k").isEmpty());
        assertEquals(0, c.physicalSize(), "lazy expiry must physically remove the entry");
    }

    @Test
    void sizeExcludesExpiredEntriesEvenBeforePurge() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("a", 1, 30);
        c.put("b", 2);
        sleep(80);
        assertEquals(1, c.size());
        assertEquals(2, c.physicalSize(), "nothing purged yet");
    }

    @Test
    void overwriteWithoutTtlClearsExpiry() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("k", 1, 30);
        c.put("k", 2);
        sleep(80);
        assertEquals(2, c.get("k").orElseThrow());
        c.verifyInternalConsistency();
    }

    @Test
    void overwriteWithTtlExtendsExpiry() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("k", 1, 40);
        c.put("k", 2, 5_000);
        sleep(100);
        assertEquals(2, c.get("k").orElseThrow());
        c.verifyInternalConsistency();
    }

    /** The regression for the original bug: capacity pressure must not evict a valid entry while an expired one exists. */
    @Test
    void expiredEntryIsReclaimedBeforeValidLruEntryIsEvicted() {
        LRUCache<String, Integer> c = new LRUCache<>(2, 0);
        c.put("validOldest", 1);
        c.put("willExpire", 2, 30);
        sleep(80);
        c.put("fresh", 3);
        assertTrue(c.get("validOldest").isPresent(), "valid LRU entry must survive");
        assertTrue(c.get("willExpire").isEmpty());
        assertTrue(c.get("fresh").isPresent());
        c.verifyInternalConsistency();
    }

    @Test
    void expiredEntriesDoNotCountAgainstCapacity() {
        LRUCache<Integer, Integer> c = new LRUCache<>(3, 0);
        c.put(1, 1, 30); c.put(2, 2, 30); c.put(3, 3, 30);
        sleep(80);
        c.put(4, 4); c.put(5, 5); c.put(6, 6);
        assertEquals(3, c.size());
        assertTrue(c.get(4).isPresent() && c.get(5).isPresent() && c.get(6).isPresent());
    }

    @Test
    void cleanupExpiredEntriesReturnsCount() {
        LRUCache<Integer, Integer> c = new LRUCache<>(10, 0);
        for (int i = 0; i < 4; i++) c.put(i, i, 30);
        c.put(99, 99);
        sleep(80);
        assertEquals(4, c.cleanupExpiredEntries());
        assertEquals(1, c.physicalSize());
        assertEquals(0, c.cleanupExpiredEntries());
        c.verifyInternalConsistency();
    }

    @Test
    void removeOfTtlEntryKeepsIndexConsistent() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("a", 1, 5_000);
        c.remove("a");
        c.verifyInternalConsistency();
    }

    @Test
    void hugeTtlDoesNotOverflow() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("k", 1, Long.MAX_VALUE);
        assertTrue(c.get("k").isPresent());
    }
}
