package com.distri.cache.unit;

import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LRUCache constructor validation.
 * 
 * <p>Tests verify that the constructor correctly validates capacity
 * and initializes the cache in a valid state.
 */
class LRUCacheConstructorTest {
    
    @Test
    void constructorRejectsZeroCapacity() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new LRUCache<String, String>(0),
            "Constructor should reject capacity of 0"
        );
        
        assertTrue(
            exception.getMessage().contains("positive"),
            "Exception message should mention capacity must be positive"
        );
    }
    
    @Test
    void constructorRejectsNegativeCapacity() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new LRUCache<String, String>(-1),
            "Constructor should reject negative capacity"
        );
        
        assertTrue(
            exception.getMessage().contains("positive"),
            "Exception message should mention capacity must be positive"
        );
    }
    
    @Test
    void constructorAcceptsPositiveCapacity() {
        assertDoesNotThrow(
            () -> new LRUCache<String, String>(1),
            "Constructor should accept capacity of 1"
        );
        
        assertDoesNotThrow(
            () -> new LRUCache<String, String>(100),
            "Constructor should accept capacity of 100"
        );
        
        assertDoesNotThrow(
            () -> new LRUCache<String, String>(Integer.MAX_VALUE),
            "Constructor should accept maximum capacity"
        );
    }
    
    @Test
    void constructorInitializesCapacityCorrectly() {
        LRUCache<String, String> cache = new LRUCache<>(50);
        
        assertEquals(50, cache.capacity(), 
            "Capacity should match the value provided to constructor");
    }
}
