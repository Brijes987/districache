// NOTE: This is the earlier implementation of LRUCache, kept ONLY so the benchmarks can compare it with
// the current one. It scans the whole map to count valid entries, so put-at-capacity, size() and cleanup
// are O(n). Do not use it; the production class is LRUCache.
package com.distri.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe LRU Cache implementation with TTL support.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of values maintained by this cache
 */
public class LegacyLRUCache<K, V> implements Cache<K, V> {

    private final int capacity;
    private final Map<K, CacheEntry<K, V>> cacheMap;
    private final ReentrantReadWriteLock lock;
    private final ScheduledExecutorService scheduler;

    // Dummy head and tail nodes for the doubly-linked list
    private final CacheEntry<K, V> head;
    private final CacheEntry<K, V> tail;

    /**
     * Constructs an LegacyLRUCache with the specified capacity.
     *
     * @param capacity the maximum capacity of the cache
     * @throws IllegalArgumentException if capacity <= 0
     */
    public LegacyLRUCache(int capacity) {
        this(capacity, 5000);
    }

    /**
     * Constructs an LegacyLRUCache with the specified capacity and cleanup interval.
     *
     * @param capacity the maximum capacity of the cache
     * @param cleanupIntervalMillis the interval in milliseconds for background cleanup
     * @throws IllegalArgumentException if capacity <= 0 or cleanupIntervalMillis <= 0
     */
    public LegacyLRUCache(int capacity, long cleanupIntervalMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (cleanupIntervalMillis <= 0) {
            throw new IllegalArgumentException("Cleanup interval must be positive");
        }
        this.capacity = capacity;
        this.cacheMap = new HashMap<>(capacity);
        this.lock = new ReentrantReadWriteLock();

        this.head = new CacheEntry<>(null, null);
        this.tail = new CacheEntry<>(null, null);
        head.next = tail;
        tail.prev = head;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lru-cache-cleanup");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::cleanupExpiredEntries,
            cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void put(K key, V value) {
        putInternal(key, value, -1);
    }

    @Override
    public void put(K key, V value, long ttlMillis) {
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("TTL must be positive");
        }
        putInternal(key, value, ttlMillis);
    }

    private void putInternal(K key, V value, long ttlMillis) {
        validateKey(key);
        validateValue(value);

        lock.writeLock().lock();
        try {
            CacheEntry<K, V> existingNode = cacheMap.get(key);

            if (existingNode != null) {
                // Update value and ttl
                existingNode.value = value;
                existingNode.expiryTime = ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : -1;
                // Move to most recently used
                moveToFront(existingNode);
            } else {
                // Evict until logical size (non-expired entries) is below capacity
                while (logicalSize() >= capacity) {
                    evictLeastRecentlyUsed();
                }

                CacheEntry<K, V> newNode = ttlMillis > 0 ?
                        new CacheEntry<>(key, value, ttlMillis) :
                        new CacheEntry<>(key, value);

                cacheMap.put(key, newNode);
                addToFront(newNode);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<V> get(K key) {
        validateKey(key);

        lock.readLock().lock();
        CacheEntry<K, V> node;
        try {
            node = cacheMap.get(key);
            if (node == null) {
                return Optional.empty();
            }
            if (node.isExpired()) {
                // Cannot remove under read lock, must upgrade
            } else if (head.next == node) {
                // Fast path: already at front, no LRU update needed
                return Optional.of(node.value);
            }
        } finally {
            lock.readLock().unlock();
        }

        // Upgrade to write lock for mutation (LRU update or eviction)
        lock.writeLock().lock();
        try {
            // Re-check state under write lock
            node = cacheMap.get(key);
            if (node == null) {
                return Optional.empty();
            }

            if (node.isExpired()) {
                removeNode(node);
                cacheMap.remove(key);
                return Optional.empty();
            }

            moveToFront(node);
            return Optional.of(node.value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void remove(K key) {
        validateKey(key);

        lock.writeLock().lock();
        try {
            CacheEntry<K, V> node = cacheMap.remove(key);
            if (node != null) {
                removeNode(node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            int count = 0;
            // Iterate over map to count non-expired entries
            for (CacheEntry<K, V> node : cacheMap.values()) {
                if (!node.isExpired()) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void cleanupExpiredEntries() {
        List<K> expiredKeys = new ArrayList<>();

        // Find expired keys with read lock
        lock.readLock().lock();
        try {
            for (CacheEntry<K, V> node : cacheMap.values()) {
                if (node.isExpired()) {
                    expiredKeys.add(node.key);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        if (expiredKeys.isEmpty()) {
            return;
        }

        // Remove expired keys with write lock
        lock.writeLock().lock();
        try {
            for (K key : expiredKeys) {
                CacheEntry<K, V> node = cacheMap.get(key);
                // Double-check expiration in case it was updated
                if (node != null && node.isExpired()) {
                    removeNode(node);
                    cacheMap.remove(key);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void removeExpiredEntriesInternal() {
        List<K> expiredKeys = new ArrayList<>();
        for (CacheEntry<K, V> node : cacheMap.values()) {
            if (node.isExpired()) {
                expiredKeys.add(node.key);
            }
        }

        for (K key : expiredKeys) {
            CacheEntry<K, V> node = cacheMap.get(key);
            if (node != null && node.isExpired()) {
                removeNode(node);
                cacheMap.remove(key);
            }
        }
    }

    /**
     * Returns the logical size of the cache (count of non-expired entries).
     * Must be called under write lock.
     */
    private int logicalSize() {
        int count = 0;
        for (CacheEntry<K, V> node : cacheMap.values()) {
            if (!node.isExpired()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Evicts the least recently used entry. Preferentially removes expired
     * entries if any exist; otherwise removes the least recently used valid entry.
     * Must be called under write lock.
     */
    private void evictLeastRecentlyUsed() {
        if (tail.prev == head) {
            return; // Empty list
        }

        // Scan from tail towards head to find first expired entry
        CacheEntry<K, V> current = tail.prev;
        while (current != head) {
            if (current.isExpired()) {
                removeNode(current);
                cacheMap.remove(current.key);
                return;
            }
            current = current.prev;
        }

        // No expired entries found, evict LRU (tail.prev)
        CacheEntry<K, V> lru = tail.prev;
        if (lru != head) {
            removeNode(lru);
            cacheMap.remove(lru.key);
        }
    }

    // --- Linked List Helper Methods (must be called under write lock) ---

    private void addToFront(CacheEntry<K, V> node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(CacheEntry<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    private void moveToFront(CacheEntry<K, V> node) {
        removeNode(node);
        addToFront(node);
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
