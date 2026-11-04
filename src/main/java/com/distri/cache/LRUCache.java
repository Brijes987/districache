package com.distri.cache;

import java.lang.ref.WeakReference;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe, fixed-capacity LRU cache with optional per-entry TTL.
 *
 * <h2>Data structures</h2>
 * <ul>
 *   <li>{@code cacheMap}: HashMap key -> entry, O(1) lookup.</li>
 *   <li>A hand-rolled doubly-linked list between two sentinel nodes ({@code head} = most recently
 *       used side, {@code tail} = least recently used side). Moving or removing a node is O(1).</li>
 *   <li>{@code expiryIndex}: a TreeSet of only the entries that have a TTL, ordered by (expiry
 *       time, sequence). Its first element is always the entry that expires soonest, so expired
 *       entries can be reclaimed in O(log n) each without scanning the whole cache.</li>
 * </ul>
 *
 * <h2>Locking</h2>
 * A single {@link ReentrantReadWriteLock} guards all three structures.
 * <ul>
 *   <li>{@code get()} first takes the READ lock: a miss, or a hit on the entry that is already the
 *       most recently used one, needs no mutation and so runs concurrently with other readers.</li>
 *   <li>Any hit that must reorder the list needs the WRITE lock. A ReentrantReadWriteLock cannot
 *       be upgraded (a thread holding the read lock that asks for the write lock deadlocks), so
 *       the read lock is released first, the write lock acquired, and the state RE-CHECKED because
 *       another thread may have removed, expired or replaced the entry in between.</li>
 *   <li>put, remove, and the background cleanup take the WRITE lock.</li>
 *   <li>size() takes the READ lock; it only reads.</li>
 * </ul>
 * Honest limitation: with a hit rate where most hits are NOT already at the head, most gets end up
 * on the write path, so read concurrency is limited. That is the cost of strict LRU ordering.
 *
 * <h2>Expiry</h2>
 * <ul>
 *   <li>Lazy: get() treats an expired entry as absent and removes it.</li>
 *   <li>On write: every put first reclaims all expired entries (via {@code expiryIndex}), so
 *       expired entries never count against capacity and are always removed before a still-valid
 *       entry is evicted.</li>
 *   <li>Background: a periodic task reclaims expired entries nobody touches. One shared daemon
 *       thread serves all caches and holds only weak references, so caches that are dropped
 *       without calling {@link #shutdown()} do not leak threads.</li>
 * </ul>
 * Expiry uses {@link System#currentTimeMillis()}, so a wall-clock jump moves expiry times.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of values maintained by this cache
 */
public final class LRUCache<K, V> implements Cache<K, V>, AutoCloseable {

    /** Default period of the background cleanup task. */
    public static final long DEFAULT_CLEANUP_INTERVAL_MILLIS = 1_000;

    private static final System.Logger LOG = System.getLogger(LRUCache.class.getName());

    private final int capacity;
    private final Map<K, CacheEntry<K, V>> cacheMap;
    private final ReentrantReadWriteLock lock;

    // Sentinel nodes of the doubly-linked list. head.next is the most recently used entry,
    // tail.prev the least recently used one.
    private final CacheEntry<K, V> head;
    private final CacheEntry<K, V> tail;

    // Entries that have a TTL, ordered by soonest expiry first. Guarded by `lock`.
    private final TreeSet<CacheEntry<K, V>> expiryIndex;
    private long nextExpirySeq;

    // Null when background cleanup is disabled.
    private final ScheduledFuture<?> cleanupTask;

    /** Creates a cache with background cleanup every {@value #DEFAULT_CLEANUP_INTERVAL_MILLIS} ms. */
    public LRUCache(int capacity) {
        this(capacity, DEFAULT_CLEANUP_INTERVAL_MILLIS);
    }

    /**
     * @param capacity maximum number of valid entries
     * @param cleanupIntervalMillis period of the background cleanup; a value &lt;= 0 disables it
     *     (expired entries are then reclaimed only lazily and on put/cleanupExpiredEntries)
     * @throws IllegalArgumentException if capacity &lt;= 0
     */
    public LRUCache(int capacity, long cleanupIntervalMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.cacheMap = new HashMap<>(capacity);
        this.lock = new ReentrantReadWriteLock();

        this.head = new CacheEntry<>(null, null);
        this.tail = new CacheEntry<>(null, null);
        head.next = tail;
        tail.prev = head;

        this.expiryIndex = new TreeSet<>(
                Comparator.comparingLong((CacheEntry<K, V> e) -> e.expiryTime)
                        .thenComparingLong(e -> e.expirySeq));

        // Must stay the last statement: the task may run as soon as it is scheduled.
        this.cleanupTask = cleanupIntervalMillis > 0
                ? Cleaner.EXECUTOR.scheduleWithFixedDelay(
                        new CleanupTask(this),
                        cleanupIntervalMillis,
                        cleanupIntervalMillis,
                        TimeUnit.MILLISECONDS)
                : null;
    }

    // ------------------------------------------------------------------ Cache operations

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
            long now = System.currentTimeMillis();
            // Reclaim expired entries first: they must not count against capacity, and must be
            // removed before any still-valid entry is evicted. An expired entry for this very key
            // is removed here too, so the key is then handled as a fresh insert.
            purgeExpiredLocked(now);

            CacheEntry<K, V> existing = cacheMap.get(key);
            if (existing != null) {
                // Remove from the expiry index BEFORE changing expiryTime: the TreeSet orders by it.
                unindexExpiry(existing);
                existing.value = value;
                existing.expiryTime = ttlMillis > 0 ? CacheEntry.expiryAt(now, ttlMillis) : -1;
                indexExpiry(existing);
                moveToFront(existing);
            } else {
                if (cacheMap.size() >= capacity) {
                    // After purgeExpiredLocked every remaining entry is valid, so this really is
                    // the least recently used valid entry.
                    evictLeastRecentlyUsed();
                }
                CacheEntry<K, V> entry = ttlMillis > 0
                        ? new CacheEntry<>(key, value, ttlMillis)
                        : new CacheEntry<>(key, value);
                cacheMap.put(key, entry);
                addToFront(entry);
                indexExpiry(entry);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<V> get(K key) {
        validateKey(key);

        // Phase 1: read lock. Misses, and hits that are already most-recently-used, need no change.
        lock.readLock().lock();
        try {
            CacheEntry<K, V> node = cacheMap.get(key);
            if (node == null) {
                return Optional.empty();
            }
            if (!node.isExpired() && head.next == node) {
                return Optional.of(node.value);
            }
        } finally {
            lock.readLock().unlock();
        }

        // Phase 2: the read lock is RELEASED (no upgrade is possible). Take the write lock and
        // re-check everything: the entry may have been removed, expired or replaced meanwhile.
        lock.writeLock().lock();
        try {
            CacheEntry<K, V> node = cacheMap.get(key);
            if (node == null) {
                return Optional.empty();
            }
            if (node.isExpired()) {
                discard(node);
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
                unindexExpiry(node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Number of valid (non-expired) entries. Runs under the read lock without mutating anything:
     * it subtracts the entries at the front of the expiry index that are already past their expiry
     * time, which costs O(k) for k expired-but-not-yet-purged entries (0 once cleanup has run).
     */
    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return cacheMap.size() - countExpiredLocked(System.currentTimeMillis());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    // ------------------------------------------------------------------ Expiry / cleanup

    /**
     * Physically removes every expired entry now.
     *
     * @return how many entries were removed
     */
    public int cleanupExpiredEntries() {
        lock.writeLock().lock();
        try {
            return purgeExpiredLocked(System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Stops the periodic background cleanup. The cache keeps working afterwards; expired entries
     * are then reclaimed lazily and on writes. A cleanup run already in progress finishes normally.
     * Safe to call more than once.
     */
    public void shutdown() {
        if (cleanupTask != null) {
            cleanupTask.cancel(false);
        }
    }

    /** Same as {@link #shutdown()}. */
    @Override
    public void close() {
        shutdown();
    }

    /**
     * Number of entries physically stored, including expired ones not yet purged. Intended for
     * tests and diagnostics; use {@link #size()} for the logical size.
     */
    public int physicalSize() {
        lock.readLock().lock();
        try {
            return cacheMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Walks the whole structure and checks its invariants. O(n); a diagnostic hook for tests.
     *
     * @throws IllegalStateException describing the first violated invariant
     */
    public void verifyInternalConsistency() {
        lock.readLock().lock();
        try {
            int count = 0;
            int withTtl = 0;
            CacheEntry<K, V> prev = head;
            CacheEntry<K, V> node = head.next;
            while (node != tail) {
                if (node == null) {
                    throw new IllegalStateException("list is broken: null link after " + prev.key);
                }
                if (++count > cacheMap.size()) {
                    throw new IllegalStateException("list has more nodes than the map has entries");
                }
                if (node.prev != prev) {
                    throw new IllegalStateException("prev link mismatch at key " + node.key);
                }
                if (cacheMap.get(node.key) != node) {
                    throw new IllegalStateException("list node not in map: " + node.key);
                }
                if (node.expiryTime > 0) {
                    withTtl++;
                    if (!expiryIndex.contains(node)) {
                        throw new IllegalStateException("TTL entry missing from expiry index: " + node.key);
                    }
                }
                prev = node;
                node = node.next;
            }
            if (tail.prev != prev) {
                throw new IllegalStateException("tail.prev does not point at the last node");
            }
            if (count != cacheMap.size()) {
                throw new IllegalStateException(
                        "list has " + count + " nodes but map has " + cacheMap.size() + " entries");
            }
            if (expiryIndex.size() != withTtl) {
                throw new IllegalStateException(
                        "expiry index has " + expiryIndex.size() + " entries but " + withTtl + " have a TTL");
            }
            if (cacheMap.size() > capacity) {
                throw new IllegalStateException("size " + cacheMap.size() + " exceeds capacity " + capacity);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------ internals (lock held)

    /** Removes all expired entries; caller holds the write lock. Returns how many were removed. */
    private int purgeExpiredLocked(long now) {
        int removed = 0;
        while (!expiryIndex.isEmpty()) {
            CacheEntry<K, V> first = expiryIndex.first();
            if (!first.isExpired(now)) {
                break;
            }
            expiryIndex.pollFirst();
            removeNode(first);
            cacheMap.remove(first.key);
            removed++;
        }
        return removed;
    }

    /** Counts expired entries without removing them; caller holds at least the read lock. */
    private int countExpiredLocked(long now) {
        if (expiryIndex.isEmpty()) {
            return 0;
        }
        // Probe sorts before every real entry with expiryTime == now, so the head set contains
        // exactly the entries with expiryTime < now, i.e. the expired ones.
        CacheEntry<K, V> probe = new CacheEntry<>(null, null);
        probe.expiryTime = now;
        probe.expirySeq = Long.MIN_VALUE;
        return expiryIndex.headSet(probe, false).size();
    }

    private void indexExpiry(CacheEntry<K, V> entry) {
        if (entry.expiryTime > 0) {
            entry.expirySeq = nextExpirySeq++;
            expiryIndex.add(entry);
        }
    }

    private void unindexExpiry(CacheEntry<K, V> entry) {
        if (entry.expiryTime > 0) {
            expiryIndex.remove(entry);
        }
    }

    /** Fully removes an entry from the list, the map and the expiry index. */
    private void discard(CacheEntry<K, V> node) {
        removeNode(node);
        cacheMap.remove(node.key);
        unindexExpiry(node);
    }

    // --- Linked list helpers (must be called under the write lock) ---

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

    private void evictLeastRecentlyUsed() {
        if (tail.prev == head) {
            return; // empty list
        }
        discard(tail.prev);
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

    // ------------------------------------------------------------------ background cleanup

    /** Lazily created so that merely loading LRUCache does not start a thread. */
    private static final class Cleaner {
        static final ScheduledThreadPoolExecutor EXECUTOR = create();

        private static ScheduledThreadPoolExecutor create() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
                Thread thread = new Thread(runnable, "lrucache-cleaner");
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }
    }

    /** Holds the cache weakly so an abandoned cache can be garbage-collected. */
    private static final class CleanupTask implements Runnable {
        private final WeakReference<LRUCache<?, ?>> cacheRef;

        CleanupTask(LRUCache<?, ?> cache) {
            this.cacheRef = new WeakReference<>(cache);
        }

        @Override
        public void run() {
            LRUCache<?, ?> cache = cacheRef.get();
            if (cache == null) {
                // Throwing from a scheduled task suppresses all further runs: the idiom for
                // "stop repeating", used here because the cache no longer exists.
                throw new CancellationException("cache was garbage collected");
            }
            try {
                cache.cleanupExpiredEntries();
            } catch (RuntimeException e) {
                // Keep the schedule alive; one failed run must not stop future cleanups.
                LOG.log(System.Logger.Level.WARNING, "background cleanup failed", e);
            }
        }
    }
}
