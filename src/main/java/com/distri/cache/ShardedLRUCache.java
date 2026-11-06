package com.distri.cache;

import java.util.Optional;

/**
 * A cache split into independently locked shards, each of which is an {@link LRUCache}.
 *
 * <p>A key always maps to the same shard, so threads working on keys in different shards do not contend
 * on one lock. The price is that eviction order is <b>per shard</b>, not globally strict: a shard can
 * evict its least recently used entry while another shard still has free room, so the cache may hold
 * fewer than {@code capacity} entries at the moment an eviction happens. For uniformly distributed keys
 * this is usually unnoticeable; for heavily skewed keys it is not.
 *
 * <p>The total capacity is divided exactly among the shards (they differ by at most one entry), so
 * {@code size() <= capacity()} always holds. {@link #size()} adds up the shards one at a time and is
 * not an atomic snapshot while other threads are writing.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ShardedLRUCache<K, V> implements Cache<K, V>, AutoCloseable {

    public static final int DEFAULT_SHARDS = 16;

    private final int capacity;
    private final LRUCache<K, V>[] shards;
    private final int shift; // 32 - log2(shard count); unused when there is a single shard

    /** {@value #DEFAULT_SHARDS} shards and the default background cleanup interval. */
    public ShardedLRUCache(int capacity) {
        this(capacity, DEFAULT_SHARDS, LRUCache.DEFAULT_CLEANUP_INTERVAL_MILLIS);
    }

    /**
     * @param capacity total maximum number of valid entries
     * @param shardCount requested number of shards; rounded down to a power of two and capped so every
     *     shard holds at least one entry (see {@link #shardCount()} for the actual number)
     * @param cleanupIntervalMillis background cleanup period for each shard; {@code <= 0} disables it
     * @throws IllegalArgumentException if capacity or shardCount is not positive
     */
    @SuppressWarnings("unchecked")
    public ShardedLRUCache(int capacity, int shardCount, long cleanupIntervalMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("Shard count must be positive");
        }
        int n = Integer.highestOneBit(Math.min(shardCount, capacity));
        this.capacity = capacity;
        this.shift = 32 - Integer.numberOfTrailingZeros(n);
        this.shards = (LRUCache<K, V>[]) new LRUCache<?, ?>[n];
        int base = capacity / n;
        int extra = capacity % n;
        for (int i = 0; i < n; i++) {
            shards[i] = new LRUCache<>(base + (i < extra ? 1 : 0), cleanupIntervalMillis);
        }
    }

    @Override
    public void put(K key, V value) {
        shardFor(key).put(key, value);
    }

    @Override
    public void put(K key, V value, long ttlMillis) {
        shardFor(key).put(key, value, ttlMillis);
    }

    @Override
    public Optional<V> get(K key) {
        return shardFor(key).get(key);
    }

    @Override
    public void remove(K key) {
        shardFor(key).remove(key);
    }

    /** Removes the key and reports whether a valid entry existed; exact within the key's shard. */
    public boolean delete(K key) {
        return shardFor(key).delete(key);
    }

    /** Sum of the shards' valid sizes; not an atomic snapshot under concurrent writes. */
    @Override
    public int size() {
        int total = 0;
        for (LRUCache<K, V> shard : shards) {
            total += shard.size();
        }
        return total;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    /** Stops background cleanup in every shard. The cache keeps working. Safe to call repeatedly. */
    public void shutdown() {
        for (LRUCache<K, V> shard : shards) {
            shard.shutdown();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /** Removes expired entries from every shard now; returns how many were removed. */
    public int cleanupExpiredEntries() {
        int removed = 0;
        for (LRUCache<K, V> shard : shards) {
            removed += shard.cleanupExpiredEntries();
        }
        return removed;
    }

    /** Entries physically stored, including expired ones not yet purged. For tests and diagnostics. */
    public int physicalSize() {
        int total = 0;
        for (LRUCache<K, V> shard : shards) {
            total += shard.physicalSize();
        }
        return total;
    }

    /**
     * Copies every live entry, shard by shard, each shard from least to most recently used.
     * The result is not a single point-in-time view of the whole cache: shards are copied one
     * after another while other threads may still be writing.
     */
    public java.util.List<CacheSnapshotEntry<K, V>> snapshot() {
        java.util.ArrayList<CacheSnapshotEntry<K, V>> out = new java.util.ArrayList<>();
        for (LRUCache<K, V> shard : shards) {
            out.addAll(shard.snapshotLruToMru());
        }
        return out;
    }

    /** Checks every shard's internal invariants; a diagnostic hook for tests. */
    public void verifyInternalConsistency() {
        for (LRUCache<K, V> shard : shards) {
            shard.verifyInternalConsistency();
        }
    }

    /** Actual number of shards (a power of two). */
    public int shardCount() {
        return shards.length;
    }

    /** Capacity of one shard. */
    public int shardCapacity(int shardIndex) {
        return shards[shardIndex].capacity();
    }

    /** Index of the shard that holds {@code key}. */
    public int shardIndexOf(K key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        if (shards.length == 1) {
            return 0;
        }
        // Pick the shard from the HIGH bits of a multiplicatively mixed hash. Each shard's HashMap
        // picks its buckets from the LOW bits; if the shard were chosen from the low bits too, all
        // keys in one shard would share them and pile into a few buckets (this was measured: it made
        // puts about 2.5x slower).
        return (key.hashCode() * 0x9E3779B9) >>> shift;
    }

    private LRUCache<K, V> shardFor(K key) {
        return shards[shardIndexOf(key)];
    }
}
