package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for size() and capacity() methods.
 * 
 * Tests task 2.9: Implement size() and capacity() methods
 * - capacity() returns the configured capacity field
 * - size() returns HashMap.size() (will be refined later for expired entries)
 * 
 * Validates Requirements: 1.5, 1.6
 */
class SizeAndCapacityTest {
    
    @Test
    void capacity_ShouldReturnConfiguredCapacity() {
        // Given
        Cache<String, String> cache1 = new LRUCache<>(10);
        Cache<String, String> cache2 = new LRUCache<>(100);
        Cache<String, String> cache3 = new LRUCache<>(1);
        
        // Then
        assertEquals(10, cache1.capacity());
        assertEquals(100, cache2.capacity());
        assertEquals(1, cache3.capacity());
    }
    
    @Test
    void size_EmptyCache_ShouldReturnZero() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // Then
        assertEquals(0, cache.size());
    }
    
    @Test
    void size_AfterSinglePut_ShouldReturnOne() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When
        cache.put("key1", "value1");
        
        // Then
        assertEquals(1, cache.size());
    }
    
    @Test
    void size_AfterMultiplePuts_ShouldReturnCorrectCount() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        
        // When
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        // Then
        assertEquals(3, cache.size());
    }
    
    @Test
    void size_AfterUpdatingExistingKey_ShouldNotIncrease() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        
        // When - update existing key
        cache.put("key1", "newValue");
        
        // Then - size should remain the same
        assertEquals(2, cache.size());
    }
    
    @Test
    void size_AfterRemove_ShouldDecrease() {
        // Given
        Cache<String, String> cache = new LRUCache<>(10);
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        // When
        cache.remove("key2");
        
        // Then
        assertEquals(2, cache.size());
    }
    
    @Test
    void size_ShouldNeverExceedCapacity() {
        // Given
        Cache<String, String> cache = new LRUCache<>(5);
        
        // When - add more entries than capacity
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        
        // Then - size should not exceed capacity
        assertTrue(cache.size() <= cache.capacity());
    }
    
    @Test
    void size_WithCapacityOne_ShouldWorkCorrectly() {
        // Given
        Cache<String, String> cache = new LRUCache<>(1);
        
        // Then - initially zero
        assertEquals(0, cache.size());
        
        // When - add one entry
        cache.put("key1", "value1");
        
        // Then - size should be 1
        assertEquals(1, cache.size());
        
        // When - add another entry (should evict first)
        cache.put("key2", "value2");
        
        // Then - size should still be 1
        assertEquals(1, cache.size());
    }
    
    @Test
    void sizeAndCapacity_RelationshipTest() {
        // Given
        Cache<String, String> cache = new LRUCache<>(3);
        
        // When/Then - size should be at most capacity
        assertEquals(0, cache.size());
        
        cache.put("key1", "value1");
        assertEquals(1, cache.size());
        assertTrue(cache.size() <= cache.capacity());
        
        cache.put("key2", "value2");
        assertEquals(2, cache.size());
        assertTrue(cache.size() <= cache.capacity());
        
        cache.put("key3", "value3");
        assertEquals(3, cache.size());
        assertTrue(cache.size() <= cache.capacity());
        
        // Adding more entries should not increase size beyond capacity
        cache.put("key4", "value4");
        assertEquals(3, cache.size());
        assertTrue(cache.size() <= cache.capacity());
    }
}
