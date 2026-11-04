package com.distri.cache;

import java.util.Optional;

/**
 * Cache interface defining core operations for an in-memory key-value store.
 * 
 * <p>This interface provides basic cache operations including put, get, and remove,
 * with support for TTL (Time To Live) expiration. All implementations must be thread-safe
 * and handle null keys/values by throwing IllegalArgumentException.
 * 
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of values maintained by this cache
 */
public interface Cache<K, V> {
    
    /**
     * Stores a key-value pair without expiration.
     * 
     * <p>If the key already exists, the value is updated and the entry's recency is refreshed
     * for LRU tracking. If the cache is at capacity, the least recently used entry may be evicted.
     * 
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     * @throws IllegalArgumentException if key or value is null
     */
    void put(K key, V value);
    
    /**
     * Stores a key-value pair with TTL expiration.
     * 
     * <p>The entry will expire after ttlMillis milliseconds from the time of insertion.
     * Expired entries return empty Optional on get() and do not count toward capacity.
     * 
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     * @param ttlMillis time to live in milliseconds; must be positive
     * @throws IllegalArgumentException if key or value is null, or if ttlMillis <= 0
     */
    void put(K key, V value, long ttlMillis);
    
    /**
     * Retrieves a value by key.
     * 
     * <p>If the key exists and has not expired, returns an Optional containing the value
     * and updates the entry's recency for LRU tracking. If the key does not exist or has
     * expired, returns an empty Optional.
     * 
     * @param key the key whose associated value is to be returned
     * @return Optional containing the value if present and not expired, empty otherwise
     * @throws IllegalArgumentException if key is null
     */
    Optional<V> get(K key);
    
    /**
     * Removes an entry by key.
     * 
     * <p>After removal, subsequent get(key) calls will return empty Optional.
     * 
     * @param key the key whose mapping is to be removed from the cache
     * @throws IllegalArgumentException if key is null
     */
    void remove(K key);
    
    /**
     * Returns current number of entries (excluding logically expired entries).
     * 
     * <p>Expired entries that have not yet been physically removed by background cleanup
     * are not counted in the size.
     * 
     * @return the number of valid (non-expired) entries in the cache
     */
    int size();
    
    /**
     * Returns maximum capacity of the cache.
     * 
     * <p>When the number of valid entries reaches capacity, the least recently used
     * entry will be evicted to make room for new entries.
     * 
     * @return the maximum number of entries the cache can hold
     */
    int capacity();
    
    /**
     * Shuts down any background tasks and releases resources used by the cache.
     * 
     * <p>After calling shutdown, subsequent operations on the cache may throw
     * IllegalStateException or proceed without background cleanup depending on
     * the implementation.
     */
    void shutdown();
}
