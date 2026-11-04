package com.distri.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for doubly-linked list helper methods in LRUCache.
 * 
 * <p>These tests verify that the addToFront, removeNode, and moveToFront methods
 * maintain bidirectional link invariants:
 * <ul>
 *   <li>For any node n: n.next.prev == n and n.prev.next == n</li>
 *   <li>Empty state: head.next == tail and tail.prev == head</li>
 *   <li>Ordering: Most recent entries after head, least recent before tail</li>
 * </ul>
 */
class DoublyLinkedListHelperTest {
    
    private LRUCache<String, String> cache;
    private CacheEntry<String, String> head;
    private CacheEntry<String, String> tail;
    
    // Reflection helpers to access private fields and methods
    private Method addToFrontMethod;
    private Method removeNodeMethod;
    private Method moveToFrontMethod;
    
    @BeforeEach
    void setUp() throws Exception {
        cache = new LRUCache<>(5);
        
        // Use reflection to access private fields
        Field headField = LRUCache.class.getDeclaredField("head");
        headField.setAccessible(true);
        head = (CacheEntry<String, String>) headField.get(cache);
        
        Field tailField = LRUCache.class.getDeclaredField("tail");
        tailField.setAccessible(true);
        tail = (CacheEntry<String, String>) tailField.get(cache);
        
        // Use reflection to access private methods
        // Note: We need to get all declared methods and find the ones we need by name
        // because CacheEntry is package-private and may cause reflection issues
        for (Method method : LRUCache.class.getDeclaredMethods()) {
            if (method.getName().equals("addToFront")) {
                addToFrontMethod = method;
                addToFrontMethod.setAccessible(true);
            } else if (method.getName().equals("removeNode")) {
                removeNodeMethod = method;
                removeNodeMethod.setAccessible(true);
            } else if (method.getName().equals("moveToFront")) {
                moveToFrontMethod = method;
                moveToFrontMethod.setAccessible(true);
            }
        }
    }
    
    @Test
    void testInitialState() {
        // Empty list: head.next == tail and tail.prev == head
        assertSame(tail, head.next, "Initially, head.next should be tail");
        assertSame(head, tail.prev, "Initially, tail.prev should be head");
    }
    
    @Test
    void testAddToFront_SingleEntry() throws Exception {
        CacheEntry<String, String> entry = new CacheEntry<>("key1", "value1");
        
        addToFrontMethod.invoke(cache, entry);
        
        // Verify entry is inserted after head and before tail
        assertSame(entry, head.next, "Entry should be after head");
        assertSame(entry, tail.prev, "Entry should be before tail");
        
        // Verify bidirectional links
        assertSame(head, entry.prev, "Entry.prev should be head");
        assertSame(tail, entry.next, "Entry.next should be tail");
    }
    
    @Test
    void testAddToFront_MultipleEntries() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Verify order: head <-> entry3 <-> entry2 <-> entry1 <-> tail
        assertSame(entry3, head.next, "Most recent entry should be entry3");
        assertSame(entry1, tail.prev, "Least recent entry should be entry1");
        
        // Verify entry3 links
        assertSame(head, entry3.prev);
        assertSame(entry2, entry3.next);
        
        // Verify entry2 links
        assertSame(entry3, entry2.prev);
        assertSame(entry1, entry2.next);
        
        // Verify entry1 links
        assertSame(entry2, entry1.prev);
        assertSame(tail, entry1.next);
    }
    
    @Test
    void testRemoveNode_SingleEntry() throws Exception {
        CacheEntry<String, String> entry = new CacheEntry<>("key1", "value1");
        
        addToFrontMethod.invoke(cache, entry);
        removeNodeMethod.invoke(cache, entry);
        
        // Verify list is empty again
        assertSame(tail, head.next, "After removal, head.next should be tail");
        assertSame(head, tail.prev, "After removal, tail.prev should be head");
    }
    
    @Test
    void testRemoveNode_MiddleEntry() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Remove middle entry (entry2)
        removeNodeMethod.invoke(cache, entry2);
        
        // Verify order: head <-> entry3 <-> entry1 <-> tail
        assertSame(entry3, head.next);
        assertSame(entry1, tail.prev);
        
        // Verify entry3 and entry1 are now neighbors
        assertSame(entry1, entry3.next);
        assertSame(entry3, entry1.prev);
        
        // Verify tail and head connections
        assertSame(head, entry3.prev);
        assertSame(tail, entry1.next);
    }
    
    @Test
    void testRemoveNode_HeadEntry() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Remove head entry (entry3, most recent)
        removeNodeMethod.invoke(cache, entry3);
        
        // Verify order: head <-> entry2 <-> entry1 <-> tail
        assertSame(entry2, head.next);
        assertSame(head, entry2.prev);
    }
    
    @Test
    void testRemoveNode_TailEntry() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Remove tail entry (entry1, least recent)
        removeNodeMethod.invoke(cache, entry1);
        
        // Verify order: head <-> entry3 <-> entry2 <-> tail
        assertSame(entry2, tail.prev);
        assertSame(tail, entry2.next);
    }
    
    @Test
    void testMoveToFront_MiddleToFront() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Move entry2 to front
        // Before: head <-> entry3 <-> entry2 <-> entry1 <-> tail
        // After:  head <-> entry2 <-> entry3 <-> entry1 <-> tail
        moveToFrontMethod.invoke(cache, entry2);
        
        // Verify entry2 is now at front
        assertSame(entry2, head.next);
        assertSame(head, entry2.prev);
        assertSame(entry3, entry2.next);
        
        // Verify entry3 and entry1 are still connected properly
        assertSame(entry2, entry3.prev);
        assertSame(entry1, entry3.next);
        assertSame(entry3, entry1.prev);
        assertSame(tail, entry1.next);
    }
    
    @Test
    void testMoveToFront_TailToFront() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        
        // Move entry1 (least recent) to front
        // Before: head <-> entry3 <-> entry2 <-> entry1 <-> tail
        // After:  head <-> entry1 <-> entry3 <-> entry2 <-> tail
        moveToFrontMethod.invoke(cache, entry1);
        
        // Verify entry1 is now at front
        assertSame(entry1, head.next);
        assertSame(head, entry1.prev);
        assertSame(entry3, entry1.next);
        
        // Verify entry2 is now at tail
        assertSame(entry2, tail.prev);
        assertSame(tail, entry2.next);
    }
    
    @Test
    void testMoveToFront_AlreadyAtFront() throws Exception {
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        
        // Move entry2 (already at front) to front again
        moveToFrontMethod.invoke(cache, entry2);
        
        // Verify entry2 is still at front
        assertSame(entry2, head.next);
        assertSame(head, entry2.prev);
        
        // Verify entry1 is still at tail
        assertSame(entry1, tail.prev);
        assertSame(tail, entry1.next);
        
        // Verify they are connected
        assertSame(entry1, entry2.next);
        assertSame(entry2, entry1.prev);
    }
    
    @Test
    void testBidirectionalInvariantsAfterMultipleOperations() throws Exception {
        // Perform a complex sequence of operations
        CacheEntry<String, String> entry1 = new CacheEntry<>("key1", "value1");
        CacheEntry<String, String> entry2 = new CacheEntry<>("key2", "value2");
        CacheEntry<String, String> entry3 = new CacheEntry<>("key3", "value3");
        CacheEntry<String, String> entry4 = new CacheEntry<>("key4", "value4");
        
        addToFrontMethod.invoke(cache, entry1);
        addToFrontMethod.invoke(cache, entry2);
        addToFrontMethod.invoke(cache, entry3);
        addToFrontMethod.invoke(cache, entry4);
        
        moveToFrontMethod.invoke(cache, entry1);  // Move tail to front
        removeNodeMethod.invoke(cache, entry3);   // Remove middle
        moveToFrontMethod.invoke(cache, entry2);  // Move middle to front
        
        // Verify bidirectional invariants throughout the list
        CacheEntry<String, String> current = head.next;
        while (current != tail) {
            assertSame(current, current.next.prev, 
                "For node " + current.key + ": current.next.prev should equal current");
            assertSame(current, current.prev.next,
                "For node " + current.key + ": current.prev.next should equal current");
            current = current.next;
        }
        
        // Verify head and tail connections
        assertSame(head, head.next.prev, "head.next.prev should be head");
        assertSame(tail, tail.prev.next, "tail.prev.next should be tail");
    }
}
