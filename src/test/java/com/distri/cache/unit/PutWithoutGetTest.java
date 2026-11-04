package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the basic put operation that don't depend on get().
 * 
 * Tests task 2.6: Implement basic put(K key, V value) without TTL or eviction
 * These tests verify that put() doesn't throw exceptions and accepts valid inputs.
 */
class PutWithoutGetTest {
    
    @Test
    void putNewEntry_ShouldNotThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When/Then - should not throw exception
        assertDoesNotThrow(() -> cache.put("key1", "value1"));
    }
    
    @Test
    void putExistingKey_ShouldNotThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        cache.put("key1", "value1");
        
        // When/Then - should not throw exception when updating
        assertDoesNotThrow(() -> cache.put("key1", "value2"));
    }
    
    @Test
    void putMultipleEntries_ShouldNotThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When/Then - should accept multiple entries
        assertDoesNotThrow(() -> {
            cache.put("key1", "value1");
            cache.put("key2", "value2");
            cache.put("key3", "value3");
        });
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
        assertTrue(exception.getMessage().toLowerCase().contains("key"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
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
        assertTrue(exception.getMessage().toLowerCase().contains("value"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void putSameKeyMultipleTimes_ShouldNotThrowException() {
        // Given
        Cache<String, Integer> cache = new LRUCache<>(5);
        
        // When/Then - should accept multiple updates to same key
        assertDoesNotThrow(() -> {
            for (int i = 1; i <= 5; i++) {
                cache.put("counter", i);
            }
        });
    }
    
    @Test
    void putWithDifferentTypes_ShouldWork() {
        // Given
        Cache<Integer, String> cache = new LRUCache<>(10);
        
        // When/Then - should work with different generic types
        assertDoesNotThrow(() -> {
            cache.put(1, "one");
            cache.put(2, "two");
            cache.put(3, "three");
        });
    }
    
    @Test
    void putUpToCapacity_ShouldNotThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(5);
        
        // When/Then - should accept entries up to capacity
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 5; i++) {
                cache.put("key" + i, "value" + i);
            }
        });
    }
    
    @Test
    void putBeyondCapacity_ShouldNotThrowException() {
        // Given
        Cache<String, String> cache = new LRUCache<>(3);
        
        // When/Then - should accept entries beyond capacity
        // Note: Eviction logic will be implemented in later tasks
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 5; i++) {
                cache.put("key" + i, "value" + i);
            }
        });
    }
}
