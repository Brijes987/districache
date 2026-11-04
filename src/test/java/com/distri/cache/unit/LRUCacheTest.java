package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class LRUCacheTest {

    private Cache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(3); // Capacity of 3 for testing eviction
    }

    @Test
    void testPutAndGet() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");

        assertEquals(Optional.of("value1"), cache.get("key1"));
        assertEquals(Optional.of("value2"), cache.get("key2"));
        assertEquals(Optional.empty(), cache.get("key3"));
        assertEquals(2, cache.size());
    }

    @Test
    void testCapacityValidation() {
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<>(0));
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<>(-1));
    }

    @Test
    void testNullValidation() {
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, "value"));
        assertThrows(IllegalArgumentException.class, () -> cache.put("key", null));
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
        assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
    }

    @Test
    void testEviction() {
        cache.put("1", "one");
        cache.put("2", "two");
        cache.put("3", "three");

        // Cache is full (capacity 3)
        assertEquals(3, cache.size());

        // Access 1 to make it most recently used
        cache.get("1");

        // Add 4, which should evict 2 (least recently used)
        cache.put("4", "four");

        assertEquals(Optional.empty(), cache.get("2"));
        assertEquals(Optional.of("one"), cache.get("1"));
        assertEquals(Optional.of("three"), cache.get("3"));
        assertEquals(Optional.of("four"), cache.get("4"));
        assertEquals(3, cache.size());
    }

    @Test
    void testPreferentialEviction() throws InterruptedException {
        // Create cache with capacity 3
        cache = new LRUCache<>(3);
        
        cache.put("1", "one");
        cache.put("2", "two", 50); // This one will expire quickly
        cache.put("3", "three");
        
        // Ensure all are inserted
        assertEquals(3, cache.size());
        
        // Wait for entry "2" to expire logically
        Thread.sleep(60);
        
        // Now logical size is 2 (since 2 is expired), but physical size is still 3.
        assertEquals(2, cache.size());
        
        // We add two more items. 
        // Adding "4" should not evict "1" (the LRU valid item) because logical size is 2 (< 3).
        // It will evict "2" (preferential eviction).
        cache.put("4", "four");
        
        // Now logical size is 3. Physical size is 3.
        assertEquals(3, cache.size());
        assertTrue(cache.get("1").isPresent()); // 1 is still present!
        assertTrue(cache.get("3").isPresent());
        assertTrue(cache.get("4").isPresent());
        
        // Now we add "5". Logical size is 3, so we MUST evict a valid entry.
        // The LRU valid entry is "1" (since 3 and 4 were added later).
        // It will evict "1" to make room for "5".
        cache.put("5", "five");
        
        // Now 1 should be gone, and 3, 4, 5 should be present.
        assertEquals(Optional.empty(), cache.get("1"));
        assertEquals(Optional.empty(), cache.get("2")); // 2 was expired and evicted
        assertTrue(cache.get("3").isPresent());
        assertTrue(cache.get("4").isPresent());
        assertTrue(cache.get("5").isPresent());
        assertEquals(3, cache.size());
    }

    @Test
    void testExpiredEntryIsEvictedBeforeValidLruEntry() throws Exception {
        cache = new LRUCache<>(3);

        cache.put("1", "one");
        cache.put("2", "two", 50);
        cache.put("3", "three");

        Thread.sleep(60);
        assertEquals(2, cache.size());

        cache.put("4", "four");

        // Verify the expired entry was evicted and valid entries remain
        assertTrue(cache.get("1").isPresent(), "Entry 1 should still be present");
        assertTrue(cache.get("3").isPresent(), "Entry 3 should still be present");
        assertTrue(cache.get("4").isPresent(), "Entry 4 should be present");
        assertEquals(Optional.empty(), cache.get("2"), "Entry 2 should have been evicted (it was expired)");
        assertEquals(3, cache.size(), "Logical size should be 3");
    }

    @Test
    void testTTL() throws InterruptedException {
        cache.put("expiring", "value", 50); // 50ms TTL

        // Should be accessible immediately
        assertTrue(cache.get("expiring").isPresent());

        // Wait for expiration
        Thread.sleep(60);

        // Should be empty after expiration
        assertEquals(Optional.empty(), cache.get("expiring"));
        
        // Size should be 0 since the only item expired
        assertEquals(0, cache.size());
    }
    
    @Test
    void testUpdateValueAndTTL() throws InterruptedException {
        cache.put("key1", "val1", 50);
        
        // Update value and make it non-expiring
        cache.put("key1", "val2");
        
        Thread.sleep(60);
        
        assertEquals(Optional.of("val2"), cache.get("key1"));
    }

    @Test
    void testRemove() {
        cache.put("1", "one");
        cache.remove("1");
        
        assertEquals(Optional.empty(), cache.get("1"));
        assertEquals(0, cache.size());
    }

    @Test
    void testThreadSafety() throws InterruptedException {
        int threads = 10;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        
        // A cache large enough so we don't worry about eviction in this specific test
        Cache<Integer, Integer> concurrentCache = new LRUCache<>(threads * operationsPerThread);

        for (int i = 0; i < threads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        int key = threadId * operationsPerThread + j;
                        concurrentCache.put(key, key);
                        concurrentCache.get(key);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threads * operationsPerThread, concurrentCache.size());
    }
    
    @Test
    void testThreadSafetyWithContentionAndEviction() throws InterruptedException {
        int threads = 10;
        int operationsPerThread = 1000;
        int sharedKeySpace = 10; // Small key space forces contention
        int cacheCapacity = 5; // Smaller than key space forces eviction
        
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        
        Cache<Integer, Integer> concurrentCache = new LRUCache<>(cacheCapacity);

        for (int i = 0; i < threads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Use modulo to create contention on same keys
                        int key = j % sharedKeySpace;
                        concurrentCache.put(key, threadId * 1000 + j);
                        concurrentCache.get(key);
                        
                        // Occasionally remove to create more dynamics
                        if (j % 100 == 0) {
                            concurrentCache.remove(key);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify cache is in valid state after concurrent operations
        assertTrue(concurrentCache.size() <= concurrentCache.capacity(),
            "Cache size must not exceed capacity");
        assertTrue(concurrentCache.size() >= 0,
            "Cache size must be non-negative");
    }
    
    @Test
    void testExpiredEntryEvictedBeforeValidEntry() throws InterruptedException {
        cache = new LRUCache<>(3);
        
        // Add an entry that will expire
        cache.put("expired", "value", 50);
        
        // Add two more valid entries to fill cache
        cache.put("valid1", "v1");
        cache.put("valid2", "v2");
        
        // All 3 entries present
        assertEquals(3, cache.size());
        
        // Wait for "expired" to expire (but not be physically removed yet)
        Thread.sleep(60);
        
        // Logical size is now 2 (expired doesn't count)
        assertEquals(2, cache.size());
        
        // Add another entry - this should evict "expired", NOT "valid1" or "valid2"
        cache.put("valid3", "v3");
        
        // Verify: expired is gone, all valid entries remain
        assertEquals(Optional.empty(), cache.get("expired"));
        assertEquals(Optional.of("v1"), cache.get("valid1"));
        assertEquals(Optional.of("v2"), cache.get("valid2"));
        assertEquals(Optional.of("v3"), cache.get("valid3"));
        assertEquals(3, cache.size());
    }
}
