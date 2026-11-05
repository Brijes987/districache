package com.distri.cache.properties;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

/**
 * Differential test: the sharded cache must behave exactly like one access-ordered LinkedHashMap per
 * shard, each with that shard's capacity. This checks per-shard LRU order, not a global one.
 */
class ShardedModelPropertiesTest {

    sealed interface Op permits Put, Get, Remove {}
    record Put(int key, int value) implements Op {}
    record Get(int key) implements Op {}
    record Remove(int key) implements Op {}

    @Provide
    Arbitrary<List<Op>> ops() {
        Arbitrary<Integer> keys = Arbitraries.integers().between(0, 40);
        Arbitrary<Op> op = Arbitraries.oneOf(
                Combinators.combine(keys, Arbitraries.integers()).as(Put::new),
                keys.map(Get::new),
                keys.map(Remove::new));
        return op.list().ofMaxSize(300);
    }

    @Property
    void matchesPerShardReferenceModel(@ForAll @IntRange(min = 1, max = 40) int capacity,
                                       @ForAll @IntRange(min = 1, max = 8) int requestedShards,
                                       @ForAll("ops") List<Op> ops) {
        ShardedLRUCache<Integer, Integer> cache = new ShardedLRUCache<>(capacity, requestedShards, 0);
        int n = cache.shardCount();
        @SuppressWarnings("unchecked")
        Map<Integer, Integer>[] models = new Map[n];
        for (int i = 0; i < n; i++) {
            final int shardCap = cache.shardCapacity(i);
            models[i] = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
                    return size() > shardCap;
                }
            };
        }
        for (Op op : ops) {
            if (op instanceof Put p) {
                cache.put(p.key(), p.value());
                models[cache.shardIndexOf(p.key())].put(p.key(), p.value());
            } else if (op instanceof Get g) {
                Optional<Integer> expected = Optional.ofNullable(models[cache.shardIndexOf(g.key())].get(g.key()));
                assertEquals(expected, cache.get(g.key()));
            } else if (op instanceof Remove r) {
                cache.remove(r.key());
                models[cache.shardIndexOf(r.key())].remove(r.key());
            }
            int expectedSize = 0;
            for (Map<Integer, Integer> m : models) expectedSize += m.size();
            assertEquals(expectedSize, cache.size());
            assertTrue(cache.size() <= capacity);
        }
        cache.verifyInternalConsistency();
    }
}
