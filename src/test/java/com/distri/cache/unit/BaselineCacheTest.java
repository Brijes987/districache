package com.distri.cache.unit;

import com.distri.cache.Cache;
import com.distri.cache.BaselineCache;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class BaselineCacheTest {

    @Test
    void baselineCacheSupportsBasicPutGetAndRemove() {
        Cache<String, String> cache = new BaselineCache<>();

        cache.put("k1", "v1");
        cache.put("k2", "v2");

        assertEquals(Optional.of("v1"), cache.get("k1"));
        assertEquals(Optional.of("v2"), cache.get("k2"));
        assertEquals(2, cache.size());

        cache.remove("k1");

        assertEquals(Optional.empty(), cache.get("k1"));
        assertEquals(1, cache.size());
    }

    @Test
    void baselineCacheHasNoCapacityLimit() {
        Cache<String, String> cache = new BaselineCache<>();

        assertEquals(-1, cache.capacity());
    }
}
