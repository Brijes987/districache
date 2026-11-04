package com.distri.cache.unit;

import com.distri.cache.LRUCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that null validation methods exist and have correct signatures.
 * 
 * <p>This test verifies task 2.3 completion: validation methods are defined with
 * correct exception types and messages.
 * 
 * <p>Tests Requirements 8.1, 8.2, 8.3, 8.4: Null handling
 */
class ValidationMethodsTest {
    
    @Test
    void validationMethods_exist_inLRUCache() {
        // Verify that the LRUCache class exists and can be instantiated
        LRUCache<String, String> cache = new LRUCache<>(10);
        assertNotNull(cache);
        
        // Verify capacity() works (this is implemented)
        assertEquals(10, cache.capacity());
    }
    
    @Test
    void validateKey_existsWithCorrectBehavior() throws Exception {
        LRUCache<String, String> cache = new LRUCache<>(10);
        
        // Use reflection to verify the private validateKey method exists
        var method = LRUCache.class.getDeclaredMethod("validateKey", Object.class);
        assertNotNull(method, "validateKey method should exist");
        
        // Make the method accessible for testing
        method.setAccessible(true);
        
        // Test that it throws IllegalArgumentException for null key
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> {
                try {
                    method.invoke(cache, (Object) null);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
        
        assertTrue(exception.getMessage().toLowerCase().contains("key"),
            "Exception message should mention 'key'");
        assertTrue(exception.getMessage().toLowerCase().contains("null"),
            "Exception message should mention 'null'");
    }
    
    @Test
    void validateValue_existsWithCorrectBehavior() throws Exception {
        LRUCache<String, String> cache = new LRUCache<>(10);
        
        // Use reflection to verify the private validateValue method exists
        var method = LRUCache.class.getDeclaredMethod("validateValue", Object.class);
        assertNotNull(method, "validateValue method should exist");
        
        // Make the method accessible for testing
        method.setAccessible(true);
        
        // Test that it throws IllegalArgumentException for null value
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> {
                try {
                    method.invoke(cache, (Object) null);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
        
        assertTrue(exception.getMessage().toLowerCase().contains("value"),
            "Exception message should mention 'value'");
        assertTrue(exception.getMessage().toLowerCase().contains("null"),
            "Exception message should mention 'null'");
    }
    
    @Test
    void validateKey_doesNotThrow_forNonNullKey() throws Exception {
        LRUCache<String, String> cache = new LRUCache<>(10);
        
        // Use reflection to access the private validateKey method
        var method = LRUCache.class.getDeclaredMethod("validateKey", Object.class);
        method.setAccessible(true);
        
        // Test that it does NOT throw for valid key
        assertDoesNotThrow(() -> method.invoke(cache, "validKey"),
            "validateKey should not throw for non-null key");
    }
    
    @Test
    void validateValue_doesNotThrow_forNonNullValue() throws Exception {
        LRUCache<String, String> cache = new LRUCache<>(10);
        
        // Use reflection to access the private validateValue method
        var method = LRUCache.class.getDeclaredMethod("validateValue", Object.class);
        method.setAccessible(true);
        
        // Test that it does NOT throw for valid value
        assertDoesNotThrow(() -> method.invoke(cache, "validValue"),
            "validateValue should not throw for non-null value");
    }
}
