package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for remove operation in cache implementations.
 * 
 * <p>Tests Requirement 1.4: Remove operation makes entries inaccessible
 */
class RemoveOperationTest {
    
    private Cache<String, String> cache;
    
    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(10);
    }
    
    @Test
    void remove_ExistingKey_ShouldMakeEntryInaccessible() {
        // Given - add an entry to cache
        cache.put("key1", "value1");
        
        // Verify entry exists
        Optional<String> beforeRemove = cache.get("key1");
        assertTrue(beforeRemove.isPresent());
        assertEquals("value1", beforeRemove.get());
        
        // When - remove the entry
        cache.remove("key1");
        
        // Then - subsequent get should return empty Optional
        Optional<String> afterRemove = cache.get("key1");
        assertFalse(afterRemove.isPresent());
    }
    
    @Test
    void remove_NonExistentKey_ShouldNotThrowException() {
        // Given - empty cache
        
        // When/Then - removing non-existent key should not throw
        assertDoesNotThrow(() -> cache.remove("nonExistentKey"));
    }
    
    @Test
    void remove_NonExistentKeyInNonEmptyCache_ShouldNotAffectOtherEntries() {
        // Given - cache with some entries
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        int sizeBefore = cache.size();
        
        // When - remove non-existent key
        cache.remove("nonExistentKey");
        
        // Then - size should remain the same and other entries still accessible
        assertEquals(sizeBefore, cache.size());
        assertTrue(cache.get("key1").isPresent());
        assertTrue(cache.get("key2").isPresent());
    }
    
    @Test
    void remove_MultipleEntries_ShouldRemoveAllSpecifiedKeys() {
        // Given - cache with multiple entries
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        // When - remove two entries
        cache.remove("key1");
        cache.remove("key3");
        
        // Then - removed entries should be inaccessible, remaining entry still accessible
        assertFalse(cache.get("key1").isPresent());
        assertFalse(cache.get("key3").isPresent());
        assertTrue(cache.get("key2").isPresent());
        assertEquals("value2", cache.get("key2").get());
        assertEquals(1, cache.size());
    }
    
    @Test
    void remove_ThenPutSameKey_ShouldAddNewEntry() {
        // Given - add and then remove an entry
        cache.put("key1", "value1");
        cache.remove("key1");
        
        // When - add entry with same key but different value
        cache.put("key1", "newValue");
        
        // Then - should retrieve the new value
        Optional<String> result = cache.get("key1");
        assertTrue(result.isPresent());
        assertEquals("newValue", result.get());
        assertEquals(1, cache.size());
    }
    
    @Test
    void remove_AllEntriesFromCache_ShouldResultInEmptyCache() {
        // Given - cache with three entries
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        
        // When - remove all entries
        cache.remove("key1");
        cache.remove("key2");
        cache.remove("key3");
        
        // Then - cache should be empty
        assertEquals(0, cache.size());
        assertFalse(cache.get("key1").isPresent());
        assertFalse(cache.get("key2").isPresent());
        assertFalse(cache.get("key3").isPresent());
    }
    
    @Test
    void remove_SameKeyMultipleTimes_ShouldBeIdempotent() {
        // Given - cache with one entry
        cache.put("key1", "value1");
        
        // When - remove the same key multiple times
        cache.remove("key1");
        cache.remove("key1");
        cache.remove("key1");
        
        // Then - should result in the same state (entry removed)
        assertFalse(cache.get("key1").isPresent());
        assertEquals(0, cache.size());
    }
}
