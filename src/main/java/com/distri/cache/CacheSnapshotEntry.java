package com.distri.cache;

/**
 * An immutable copy of one live cache entry, used to write a snapshot.
 *
 * @param expiresAtMillis absolute expiry time in epoch milliseconds, or -1 if the entry never expires
 */
public record CacheSnapshotEntry<K, V>(K key, V value, long expiresAtMillis) {}
