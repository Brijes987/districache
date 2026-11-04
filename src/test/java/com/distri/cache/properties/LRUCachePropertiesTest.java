package com.distri.cache.properties;

import com.distri.cache.LRUCache;
import net.jqwik.api.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LRUCachePropertiesTest {

    @Property
    void sizeNeverExceedsCapacity(
            @ForAll("positiveCapacities") int capacity,
            @ForAll("cacheOperations") List<Operation> operations) {
        
        LRUCache<String, String> cache = new LRUCache<>(capacity);
        
        for (Operation op : operations) {
            op.apply(cache);
            
            // Check invariant after every operation
            assert cache.size() <= capacity : "Size exceeded capacity!";
        }
    }

    @Property(tries = 10)
    void ttlEntriesExpireAndNoLongerCountTowardSize(@ForAll("positiveCapacities") int capacity) throws InterruptedException {
        LRUCache<String, String> cache = new LRUCache<>(capacity, 10);

        cache.put("ttl-key", "ttl-value", 20);
        assertTrue(cache.get("ttl-key").isPresent());

        Thread.sleep(50);

        assertEquals(Optional.empty(), cache.get("ttl-key"));
        assertEquals(0, cache.size());
    }

    @Property(tries = 10)
    void concurrentAccessRemainsThreadSafe(@ForAll("positiveCapacities") int capacity) throws InterruptedException {
        LRUCache<String, String> cache = new LRUCache<>(capacity);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int i = 0; i < 4; i++) {
            final int threadId = i;
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int op = 0; op < 200; op++) {
                        String key = threadId + "-" + op;
                        cache.put(key, "value");
                        cache.get(key);
                        cache.remove(key);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
        }

        ready.await();
        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertTrue(cache.size() <= capacity);
    }

    @Property(tries = 10)
    void capacityOneBehaviorMatchesSpec() {
        LRUCache<String, String> cache = new LRUCache<>(1);

        cache.put("k1", "v1");
        assertEquals(1, cache.size());
        assertEquals(Optional.of("v1"), cache.get("k1"));

        cache.put("k2", "v2");
        assertEquals(1, cache.size());
        assertEquals(Optional.empty(), cache.get("k1"));
        assertEquals(Optional.of("v2"), cache.get("k2"));

        cache.put("k1", "v3");
        assertEquals(1, cache.size());
        assertEquals(Optional.of("v3"), cache.get("k1"));
        assertEquals(Optional.empty(), cache.get("k2"));
    }

    @Provide
    Arbitrary<Integer> positiveCapacities() {
        return Arbitraries.integers().between(1, 100);
    }
    
    @Provide
    Arbitrary<List<Operation>> cacheOperations() {
        return Arbitraries.of(
            new PutOperation("key1", "val1"),
            new PutOperation("key2", "val2"),
            new PutOperation("key3", "val3"),
            new PutOperation("key4", "val4"),
            new GetOperation("key1"),
            new GetOperation("key2"),
            new RemoveOperation("key1"),
            new RemoveOperation("key3")
        ).list().ofMinSize(0).ofMaxSize(100);
    }

    interface Operation {
        void apply(LRUCache<String, String> cache);
    }

    static class PutOperation implements Operation {
        final String key, value;
        PutOperation(String key, String value) { this.key = key; this.value = value; }
        @Override public void apply(LRUCache<String, String> cache) { cache.put(key, value); }
    }

    static class GetOperation implements Operation {
        final String key;
        GetOperation(String key) { this.key = key; }
        @Override public void apply(LRUCache<String, String> cache) { cache.get(key); }
    }

    static class RemoveOperation implements Operation {
        final String key;
        RemoveOperation(String key) { this.key = key; }
        @Override public void apply(LRUCache<String, String> cache) { cache.remove(key); }
    }
}
