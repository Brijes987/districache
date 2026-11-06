package com.distri.cache.client;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConsistentHashRingTest {

    private static ConsistentHashRing ringOf(int n) {
        ConsistentHashRing ring = new ConsistentHashRing();
        for (int i = 0; i < n; i++) {
            ring.add("node" + i + ":6380");
        }
        return ring;
    }

    @Test
    void sameKeyAlwaysGoesToTheSameNode() {
        ConsistentHashRing ring = ringOf(4);
        String first = ring.nodeFor("user:42");
        for (int i = 0; i < 100; i++) {
            assertEquals(first, ring.nodeFor("user:42"));
        }
    }

    @Test
    void emptyRingRefusesToRoute() {
        assertThrows(IllegalStateException.class, () -> new ConsistentHashRing().nodeFor("k"));
    }

    @Test
    void singleNodeOwnsEverything() {
        ConsistentHashRing ring = ringOf(1);
        for (int i = 0; i < 100; i++) {
            assertEquals("node0:6380", ring.nodeFor("k" + i));
        }
    }

    @Test
    void addAndRemoveReportWhetherAnythingChanged() {
        ConsistentHashRing ring = new ConsistentHashRing();
        assertTrue(ring.add("a:1"));
        assertFalse(ring.add("a:1"));
        assertEquals(1, ring.size());
        assertTrue(ring.remove("a:1"));
        assertFalse(ring.remove("a:1"));
        assertEquals(0, ring.size());
    }

    @Test
    void insertionOrderDoesNotMatter() {
        ConsistentHashRing one = new ConsistentHashRing();
        ConsistentHashRing two = new ConsistentHashRing();
        for (String n : new String[] {"a:1", "b:2", "c:3", "d:4"}) one.add(n);
        for (String n : new String[] {"d:4", "b:2", "a:1", "c:3"}) two.add(n);
        for (int i = 0; i < 2000; i++) {
            assertEquals(one.nodeFor("key" + i), two.nodeFor("key" + i));
        }
    }

    @Test
    void addingANodeOnlyMovesKeysToTheNewNode() {
        ConsistentHashRing ring = ringOf(5);
        int keys = 20_000;
        String[] before = new String[keys];
        for (int k = 0; k < keys; k++) before[k] = ring.nodeFor("key-" + k);
        ring.add("newnode:6380");
        for (int k = 0; k < keys; k++) {
            String now = ring.nodeFor("key-" + k);
            assertTrue(now.equals(before[k]) || now.equals("newnode:6380"),
                    "key moved between two old nodes: " + k);
        }
    }

    @Test
    void removingANodeOnlyMovesTheKeysItOwned() {
        ConsistentHashRing ring = ringOf(5);
        int keys = 20_000;
        String[] before = new String[keys];
        for (int k = 0; k < keys; k++) before[k] = ring.nodeFor("key-" + k);
        ring.remove("node2:6380");
        for (int k = 0; k < keys; k++) {
            String now = ring.nodeFor("key-" + k);
            if (!before[k].equals("node2:6380")) {
                assertEquals(before[k], now, "a key that was not on the removed node moved: " + k);
            }
            assertNotEquals("node2:6380", now);
        }
    }

    /** Measured with 100,000 keys: about 17.9% against an ideal 16.7%. */
    @Test
    void addingASixthNodeMovesRoughlyOneSixthOfTheKeys() {
        ConsistentHashRing ring = ringOf(5);
        int keys = 50_000;
        String[] before = new String[keys];
        for (int k = 0; k < keys; k++) before[k] = ring.nodeFor("key-" + k);
        ring.add("node5:6380");
        int moved = 0;
        for (int k = 0; k < keys; k++) {
            if (!ring.nodeFor("key-" + k).equals(before[k])) moved++;
        }
        double fraction = (double) moved / keys;
        assertTrue(fraction > 0.12 && fraction < 0.24, "moved " + fraction);
    }

    @Test
    void keysSpreadEvenlyEnough() {
        ConsistentHashRing ring = ringOf(5);
        Map<String, Integer> load = new HashMap<>();
        int keys = 50_000;
        for (int k = 0; k < keys; k++) load.merge(ring.nodeFor("key-" + k), 1, Integer::sum);
        assertEquals(5, load.size(), "every node must receive some keys");
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (int v : load.values()) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        assertTrue((double) max / min < 1.35, "largest/smallest = " + (double) max / min);
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> new ConsistentHashRing(0));
        ConsistentHashRing ring = ringOf(1);
        assertThrows(IllegalArgumentException.class, () -> ring.add(null));
        assertThrows(IllegalArgumentException.class, () -> ring.add(""));
        assertThrows(IllegalArgumentException.class, () -> ring.remove(null));
        assertThrows(IllegalArgumentException.class, () -> ring.nodeFor(null));
    }

    @Test
    void hashIsStableAcrossRuns() {
        // Pinned values: the hash must not change between JVMs or releases, or every key would be re-routed.
        assertEquals(ConsistentHashRing.hash("hello"), ConsistentHashRing.hash("hello"));
        assertNotEquals(ConsistentHashRing.hash("hello"), ConsistentHashRing.hash("hellp"));
        assertEquals(0x5d41402abc4b2a76L, ConsistentHashRing.hash("hello")); // first 8 bytes of MD5("hello")
    }
}
