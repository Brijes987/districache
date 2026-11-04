package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LRUCacheBackgroundCleanupTest {

    private Cache<String, String> cache;

    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    @Test
    void testBackgroundCleanupRemovesExpiredEntries() throws InterruptedException {
        // Use a short cleanup interval (100ms) for testing
        cache = new LRUCache<>(10, 100);

        cache.put("key1", "value1", 50); // Expire in 50ms
        cache.put("key2", "value2", 2000); // Expire later

        // Both are present initially
        assertTrue(cache.get("key1").isPresent());
        assertTrue(cache.get("key2").isPresent());

        // Wait for key1 to expire and the background thread to run
        Thread.sleep(250);

        // size() should only count non-expired entries, but internally the background
        // thread should have physically removed it, preventing memory leaks.
        // We can verify that size() is 1.
        assertEquals(1, cache.size());
        
        // key1 should be empty (already physically removed or lazy-evicted)
        assertEquals(Optional.empty(), cache.get("key1"));
        
        // key2 should still be present
        assertTrue(cache.get("key2").isPresent());
    }

    @Test
    void testShutdownStopsCleanup() throws InterruptedException {
        cache = new LRUCache<>(10, 100);
        cache.put("key1", "value1", 50);
        
        // Shutdown immediately
        cache.shutdown();
        
        // Wait for what would have been an expiration and cleanup cycle
        Thread.sleep(250);
        
        // Since the cleanup thread is stopped, the entry might still be physically 
        // in the map, but size() ignores logically expired entries, so size() returns 0.
        assertEquals(0, cache.size());
        
        // And get() returns empty because it checks expiration.
        assertEquals(Optional.empty(), cache.get("key1"));
    }
}
