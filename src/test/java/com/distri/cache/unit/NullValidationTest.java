package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for null argument validation in cache implementations.
 * 
 * <p>Tests Requirements 8.1, 8.2, 8.3, 8.4: Null handling
 */
class NullValidationTest {
    
    private Cache<String, String> cache;
    
    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(10);
    }
    
    @Test
    void putWithNullKey_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put(null, "value")
        );
        assertTrue(exception.getMessage().contains("key"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void putWithNullValue_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put("key", null)
        );
        assertTrue(exception.getMessage().contains("value"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void putWithTTLAndNullKey_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put(null, "value", 1000)
        );
        assertTrue(exception.getMessage().contains("key"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void putWithTTLAndNullValue_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.put("key", null, 1000)
        );
        assertTrue(exception.getMessage().contains("value"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void getWithNullKey_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.get(null)
        );
        assertTrue(exception.getMessage().contains("key"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
    
    @Test
    void removeWithNullKey_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> cache.remove(null)
        );
        assertTrue(exception.getMessage().contains("key"));
        assertTrue(exception.getMessage().toLowerCase().contains("null"));
    }
}
