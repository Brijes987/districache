package com.distri.cache;

/**
 * A cache entry: key, value, optional expiry time, and the prev/next links of the LRU list.
 *
 * <p>Package-private on purpose: only cache implementations touch it, always while holding the
 * cache's lock, so none of the fields are volatile.
 */
class CacheEntry<K, V> {

    /** The cache key (immutable). */
    final K key;

    /** The cached value (mutable so an update can reuse the node). */
    V value;

    /** Epoch milliseconds at which the entry expires, or -1 for "never expires". */
    long expiryTime;

    /**
     * Tie-breaker used by the cache's expiry index so two entries with the same expiry time still
     * have a strict ordering. Assigned by the owning cache; meaningless otherwise.
     */
    long expirySeq;

    /** Neighbour towards the most-recently-used end of the list. */
    CacheEntry<K, V> prev;

    /** Neighbour towards the least-recently-used end of the list. */
    CacheEntry<K, V> next;

    /** Creates an entry that never expires. */
    CacheEntry(K key, V value) {
        this.key = key;
        this.value = value;
        this.expiryTime = -1;
    }

    /**
     * Creates an entry that expires {@code ttlMillis} from now.
     *
     * <p>The caller ({@code LRUCache.put}) is responsible for rejecting {@code ttlMillis <= 0}
     * before constructing an entry; the public contract lives on {@link Cache#put(Object, Object,
     * long)}. For defensiveness a non-positive value is treated as "no expiration" here.
     */
    CacheEntry(K key, V value, long ttlMillis) {
        this.key = key;
        this.value = value;
        this.expiryTime = ttlMillis > 0 ? expiryAt(System.currentTimeMillis(), ttlMillis) : -1;
    }

    /** now + ttl, saturating at Long.MAX_VALUE instead of overflowing. */
    static long expiryAt(long nowMillis, long ttlMillis) {
        return ttlMillis > Long.MAX_VALUE - nowMillis ? Long.MAX_VALUE : nowMillis + ttlMillis;
    }

    boolean isExpired() {
        return isExpired(System.currentTimeMillis());
    }

    /** An entry is expired once the clock is strictly past its expiry time. */
    boolean isExpired(long nowMillis) {
        return expiryTime > 0 && nowMillis > expiryTime;
    }
}
