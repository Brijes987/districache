package com.distri.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class LRUCachePeekTest {

    @Test
    void peekReturnsTheValueWithoutChangingTheRecencyOrder() {
        try (LRUCache<String, String> cache = new LRUCache<>(3, 0)) {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            List<CacheSnapshotEntry<String, String>> before = cache.snapshotLruToMru();
            assertEquals("1", cache.peek("a").orElseThrow());
            assertEquals(before, cache.snapshotLruToMru(), "peek must not promote");
            cache.get("a");
            assertEquals("a", cache.snapshotLruToMru().get(2).key(), "get does promote (control)");
        }
    }

    @Test
    void peekOfAMissingKeyIsEmpty() {
        try (LRUCache<String, String> cache = new LRUCache<>(3, 0)) {
            assertFalse(cache.peek("x").isPresent());
        }
    }

    @Test
    void anExpiredEntryReadsAsAbsentAndIsNotRemovedByPeek() throws InterruptedException {
        try (LRUCache<String, String> cache = new LRUCache<>(3, 0)) {
            cache.put("a", "1", 20);
            assertTrue(cache.peek("a").isPresent());
            Thread.sleep(60);
            int physicalBefore = cache.physicalSize();
            assertFalse(cache.peek("a").isPresent());
            assertEquals(physicalBefore, cache.physicalSize(), "peek must not remove: that would change eviction timing");
        }
    }
}
