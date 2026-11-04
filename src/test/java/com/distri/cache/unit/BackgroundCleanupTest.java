package com.distri.cache.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.LRUCache;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class BackgroundCleanupTest {

    /** Polls instead of sleeping a fixed time, so the test is not timing-fragile. */
    private static boolean eventually(long timeoutMs, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(10);
        }
        return cond.getAsBoolean();
    }

    @Test
    void backgroundTaskRemovesUntouchedExpiredEntries() throws Exception {
        try (LRUCache<String, Integer> c = new LRUCache<>(10, 20)) {
            c.put("a", 1, 30);
            c.put("b", 2, 30);
            assertTrue(eventually(3_000, () -> c.physicalSize() == 0));
        }
    }

    @Test
    void backgroundTaskKeepsValidEntries() throws Exception {
        try (LRUCache<String, Integer> c = new LRUCache<>(10, 20)) {
            c.put("keep", 1);
            c.put("gone", 2, 30);
            assertTrue(eventually(3_000, () -> c.physicalSize() == 1));
            assertEquals(1, c.get("keep").orElseThrow());
        }
    }

    @Test
    void shutdownStopsBackgroundButCacheStillWorks() throws Exception {
        LRUCache<String, Integer> c = new LRUCache<>(10, 20);
        c.shutdown();
        c.shutdown(); // idempotent
        c.put("a", 1, 30);
        Thread.sleep(150);
        assertEquals(1, c.physicalSize(), "no background purge after shutdown");
        assertTrue(c.get("a").isEmpty(), "lazy expiry still works");
    }

    @Test
    void manyCachesDoNotCreateManyThreads() {
        int before = Thread.activeCount();
        for (int i = 0; i < 500; i++) {
            new LRUCache<Integer, Integer>(4, 50); // intentionally never shut down
        }
        assertTrue(Thread.activeCount() - before < 10,
                "cleanup must use one shared thread, not one per cache");
    }
}
