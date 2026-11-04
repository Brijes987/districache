package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for basic get() operation (Task 2.7).
 * Tests the get method without TTL expiry - focuses on:
 * - Null key validation
 * - HashMap lookup
 * - Returning Optional.of(value) when entry exists
 * - Returning Optional.empty() when entry does not exist
 * - LRU recency update when entry is accessed
 */
class BasicGetOperationTest {

    private Cache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(5);
    }

    @Test
    void getNullKeyThrowsException() {
        // Requirement 1.2, 1.3: Validate key is not null
        assertThrows(IllegalArgumentException.class, () -> cache.get(null),
                "get() should throw IllegalArgumentException for null key");
    }

    @Test
    void getExistingKeyReturnsValue() {
        // Requirement 1.2: When a key exists, get() returns Optional containing the value
        cache.put("key1", "value1");
        
        Optional<String> result = cache.get("key1");
        
        assertTrue(result.isPresent(), "get() should return present Optional for existing key");
        assertEquals("value1", result.get(), "get() should return correct value");
    }

    @Test
    void getNonExistentKeyReturnsEmpty() {
        // Requirement 1.3: When a key does not exist, get() returns empty Optional
        Optional<String> result = cache.get("nonexistent");
        
        assertFalse(result.isPresent(), "get() should return empty Optional for non-existent key");
        assertEquals(Optional.empty(), result, "get() should return Optional.empty()");
    }

    @Test
    void getMultipleExistingKeys() {
        // Test get() with multiple keys
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        assertEquals(Optional.of("value1"), cache.get("key1"));
        assertEquals(Optional.of("value2"), cache.get("key2"));
        assertEquals(Optional.of("value3"), cache.get("key3"));
    }

    @Test
    void getUpdatesLRURecency() {
        // Requirement 3.2: get() updates entry's recency (moves to front)
        // Fill cache to capacity
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");
        cache.put("key5", "value5");
        
        // Access key1 to make it most recently used
        cache.get("key1");
        
        // Add a new key, which should evict the least recently used (key2)
        cache.put("key6", "value6");
        
        // key1 should still be present because we accessed it
        assertTrue(cache.get("key1").isPresent(), 
                "Accessed key should not be evicted");
        
        // key2 should be evicted as it was the least recently used
        assertEquals(Optional.empty(), cache.get("key2"),
                "Least recently used key should be evicted");
    }

    @Test
    void getAfterPutReturnsUpdatedValue() {
        // Test that get() returns the latest value after update
        cache.put("key1", "value1");
        cache.put("key1", "value2");  // Update
        
        Optional<String> result = cache.get("key1");
        
        assertTrue(result.isPresent());
        assertEquals("value2", result.get(), 
                "get() should return updated value after put");
    }

    @Test
    void getAfterRemoveReturnsEmpty() {
        // Test that get() returns empty after remove
        cache.put("key1", "value1");
        cache.remove("key1");
        
        Optional<String> result = cache.get("key1");
        
        assertFalse(result.isPresent(),
                "get() should return empty Optional after remove");
    }

    @Test
    void multipleGetsOnSameKey() {
        // Test that multiple get() calls on same key work correctly
        cache.put("key1", "value1");
        
        assertEquals(Optional.of("value1"), cache.get("key1"));
        assertEquals(Optional.of("value1"), cache.get("key1"));
        assertEquals(Optional.of("value1"), cache.get("key1"));
    }

    @Test
    void getWithDifferentValueTypes() {
        // Test get() with different value types
        Cache<String, Integer> intCache = new LRUCache<>(5);
        intCache.put("one", 1);
        intCache.put("two", 2);
        
        assertEquals(Optional.of(1), intCache.get("one"));
        assertEquals(Optional.of(2), intCache.get("two"));
        assertEquals(Optional.empty(), intCache.get("three"));
    }
}
