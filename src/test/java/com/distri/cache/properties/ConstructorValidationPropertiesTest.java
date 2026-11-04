package com.distri.cache.properties;

import com.distri.cache.LRUCache;
import net.jqwik.api.*;
import net.jqwik.api.constraints.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for LRUCache constructor validation.
 * 
 * <p>Feature: in-memory-cache-phase1, Property 14: Zero Capacity Rejected
 * <p>For any attempt to construct a cache with capacity &le; 0, the constructor
 * SHALL throw IllegalArgumentException.
 * 
 * <p>Validates: Requirements 9.1
 */
class ConstructorValidationPropertiesTest {
    
    /**
     * Property 14: For any non-positive capacity (&le; 0), the constructor
     * SHALL throw IllegalArgumentException with a message containing "positive".
     */
    @Property(tries = 100)
    @Tag("Property 14: Zero Capacity Rejected")
    void nonPositiveCapacity_shouldThrowIllegalArgumentException(
            @ForAll @IntRange(min = Integer.MIN_VALUE, max = 0) int capacity) {
        
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new LRUCache<String, String>(capacity),
            "Constructor should reject non-positive capacity: " + capacity
        );
        
        assertTrue(exception.getMessage().contains("positive"),
            "Exception message should mention capacity must be positive, got: " 
                + exception.getMessage());
    }
    
    /**
     * Positive counterpart of Property 14: For any positive capacity, the constructor
     * SHALL succeed, report the correct capacity, and start with size 0.
     */
    @Property(tries = 100)
    @Tag("Property 14: Zero Capacity Rejected")
    void positiveCapacity_shouldCreateCacheSuccessfully(
            @ForAll @IntRange(min = 1, max = 10_000) int capacity) {
        
        LRUCache<String, String> cache = new LRUCache<>(capacity);
        
        assertEquals(capacity, cache.capacity(),
            "Capacity should match the value provided to constructor");
        assertEquals(0, cache.size(),
            "New cache should have size 0");
    }
}
