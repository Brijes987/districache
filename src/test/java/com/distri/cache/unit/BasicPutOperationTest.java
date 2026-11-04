package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the basic put operation without TTL or eviction.
 * 
 * Tests task 2.6: Implement basic put(K key, V value) without TTL or eviction
 * - Validate key and value are not null
 * - If key exists, update value and move to front
 * - If key does not exist, create new CacheEntry with expiryTime = -1
 * - Add entry to HashMap and linked list front
 */
class BasicPutOperationTest {
    
    @Test
    void putNewEntry_ShouldStoreEntry() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When
        cache.put("key1", "value1");
        
        // Then
        Optional<String> result = cache.get("key1");
        assertTrue(result.isPresent());
        assertEquals("value1", result.get());
    }
    
    @Test
    void putExistingKey_ShouldUpdateValue() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        cache.put("key1", "value1");
        
        // When - update with new value
        cache.put("key1", "value2");
        
        // Then - should have updated value
        Optional<String> result = cache.get("key1");
        assertTrue(result.isPresent());
        assertEquals("value2", result.get());
    }
    
    @Test
    void putMultipleEntries_AllShouldBeAccessible() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        // Then - all entries should be accessible
        assertEquals("value1", cache.get("key1").orElse(null));
        assertEquals("value2", cache.get("key2").orElse(null));
        assertEquals("value3", cache.get("key3").orElse(null));
    }
    
    @Test
    void putWithNullKey_ShouldThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When/Then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put(null, "value")
        );
        assertTrue(exception.getMessage().contains("key"));
    }
    
    @Test
    void putWithNullValue_ShouldThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When/Then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put("key", null)
        );
        assertTrue(exception.getMessage().contains("value"));
    }
    
    @Test
    void putSameKeyMultipleTimes_ShouldKeepUpdatingValue() {
        // Given
        Cache<String, Integer> cache = new LRUCache<>(5);
        
        // When - put same key multiple times with different values
        for (int i = 1; i <= 5; i++) {
            cache.put("counter", i);
        }
        
        // Then - should have the latest value
        Optional<Integer> result = cache.get("counter");
        assertTrue(result.isPresent());
        assertEquals(5, result.get());
    }
    
    @Test
    void putWithDifferentTypes_ShouldWork() {
        // Given
        Cache<Integer, String> cache = new LRUCache<>(10);
        
        // When
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        
        // Then
        assertEquals("one", cache.get(1).orElse(null));
        assertEquals("two", cache.get(2).orElse(null));
        assertEquals("three", cache.get(3).orElse(null));
    }
}
