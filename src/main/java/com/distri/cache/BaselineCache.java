package com.distri.cache;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple baseline cache implementation for benchmarking comparisons.
 *
 * <p>This implementation intentionally provides no eviction policy and no TTL support.
 * It relies on {@link ConcurrentHashMap} for thread-safe key/value storage and returns
 * {@code -1} from {@link #capacity()} to indicate that capacity is unbounded.</p>
 */
public class BaselineCache<K, V> implements Cache<K, V> {

    private final ConcurrentMap<K, V> map;

    public BaselineCache() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public void put(K key, V value) {
        validateKey(key);
        validateValue(value);
        map.put(key, value);
    }

    @Override
    public void put(K key, V value, long ttlMillis) {
        validateKey(key);
        validateValue(value);
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("TTL must be positive");
        }
        map.put(key, value);
    }

    @Override
    public Optional<V> get(K key) {
        validateKey(key);
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void remove(K key) {
        validateKey(key);
        map.remove(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public int capacity() {
        return -1;
    }

    @Override
    public void shutdown() {
        // No background resources to shut down.
    }

    private void validateKey(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
    }

    private void validateValue(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
    }
}
