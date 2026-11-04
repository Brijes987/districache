package com.distri.cache.properties;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.LRUCache;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

/**
 * Differential testing: replay random operations on LRUCache and on a reference model built from
 * an access-ordered LinkedHashMap, and require identical observable behaviour. No TTL is used here
 * so that the comparison is fully deterministic (TTL is covered by unit tests).
 */
class LRUCacheModelPropertiesTest {

    sealed interface Op permits Put, Get, Remove {}
    record Put(int key, int value) implements Op {}
    record Get(int key) implements Op {}
    record Remove(int key) implements Op {}

    @Provide
    Arbitrary<List<Op>> ops() {
        Arbitrary<Integer> keys = Arbitraries.integers().between(0, 12);
        Arbitrary<Op> op = Arbitraries.oneOf(
                Combinators.combine(keys, Arbitraries.integers()).as(Put::new),
                keys.map(Get::new),
                keys.map(Remove::new));
        return op.list().ofMaxSize(200);
    }

    @Property
    void matchesReferenceModel(@ForAll @IntRange(min = 1, max = 8) int capacity, @ForAll("ops") List<Op> ops) {
        LRUCache<Integer, Integer> cache = new LRUCache<>(capacity, 0);
        Map<Integer, Integer> model = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
                return size() > capacity;
            }
        };
        for (Op op : ops) {
            if (op instanceof Put p) {
                cache.put(p.key(), p.value());
                model.put(p.key(), p.value());
            } else if (op instanceof Get g) {
                assertEquals(java.util.Optional.ofNullable(model.get(g.key())), cache.get(g.key()));
            } else if (op instanceof Remove r) {
                cache.remove(r.key());
                model.remove(r.key());
            }
            assertEquals(model.size(), cache.size());
            assertTrue(cache.size() <= capacity);
        }
        cache.verifyInternalConsistency();
    }

    @Property
    void sizeNeverExceedsCapacity(@ForAll @IntRange(min = 1, max = 10) int capacity,
                                  @ForAll List<@IntRange(min = 0, max = 50) Integer> keys) {
        LRUCache<Integer, Integer> cache = new LRUCache<>(capacity, 0);
        for (Integer k : keys) {
            cache.put(k, k);
            assertTrue(cache.size() <= capacity);
        }
    }

    @Property
    void putThenGetReturnsValue(@ForAll int key, @ForAll int value) {
        LRUCache<Integer, Integer> cache = new LRUCache<>(4, 0);
        cache.put(key, value);
        assertEquals(value, cache.get(key).orElseThrow());
    }
}
