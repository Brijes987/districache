package com.distri.cache.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.LRUCache;
import com.distri.cache.ShardedLRUCache;
import org.junit.jupiter.api.Test;

class DeleteTest {

    @Test
    void lruDeleteReportsWhetherAValidEntryExisted() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("a", 1);
        assertTrue(c.delete("a"));
        assertFalse(c.delete("a"));
        assertFalse(c.delete("never"));
        assertTrue(c.get("a").isEmpty());
        c.verifyInternalConsistency();
    }

    @Test
    void lruDeleteOfAnExpiredEntryReturnsFalseButRemovesIt() throws Exception {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("a", 1, 30);
        Thread.sleep(100);
        assertFalse(c.delete("a"));
        assertEquals(0, c.physicalSize());
        c.verifyInternalConsistency();
    }

    @Test
    void lruDeleteKeepsTheExpiryIndexConsistent() {
        LRUCache<String, Integer> c = new LRUCache<>(5, 0);
        c.put("a", 1, 60_000);
        assertTrue(c.delete("a"));
        c.verifyInternalConsistency();
    }

    @Test
    void lruDeleteRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<String, Integer>(5, 0).delete(null));
    }

    @Test
    void shardedDelete() {
        ShardedLRUCache<String, Integer> c = new ShardedLRUCache<>(100, 4, 0);
        c.put("a", 1);
        assertTrue(c.delete("a"));
        assertFalse(c.delete("a"));
        assertThrows(IllegalArgumentException.class, () -> c.delete(null));
    }
}
