package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Makes "apply a write to the cache, then record it" one step that no other write can interleave with.
 *
 * <p>The record goes to the append-only log (if persistence is on) and to the replication stream (if this server
 * is a primary), so both see the writes in exactly the order they took effect. A replica's full resync also takes
 * this lock while it copies the cache and reads the stream offset, which is what makes the snapshot and the offset
 * agree.
 *
 * <p>The price is that writes are serialised while persistence or replication is on. Reads are not affected.
 * A server with neither has no coordinator and keeps the cache's per-shard write concurrency.
 */
final class WriteCoordinator {

    private final ReentrantLock lock = new ReentrantLock();
    private final ShardedLRUCache<String, String> cache;
    private final AppendOnlyLog log;          // null: no persistence
    private final ReplicationHub hub;         // null: replication not enabled

    WriteCoordinator(ShardedLRUCache<String, String> cache, AppendOnlyLog log, ReplicationHub hub) {
        this.cache = cache;
        this.log = log;
        this.hub = hub;
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    boolean hasLog() {
        return log != null;
    }

    /**
     * Records a write that has just been applied to the cache. The caller holds {@link #lock()}. The replication
     * stream gets it first because publishing cannot fail; if the log then fails, the IOException tells the client.
     */
    void record(String... args) throws IOException {
        byte[] encoded = AppendOnlyLog.encode(args);
        if (hub != null && hub.isPrimary()) {
            hub.publish(encoded);
        }
        if (log != null) {
            log.append(encoded);
            log.maybeCompact(cache);
        }
    }

    /** Heartbeat for the replication stream only (not logged). Takes the lock itself. */
    void publishHeartbeat() {
        lock.lock();
        try {
            if (hub != null && hub.isPrimary()) {
                hub.publish(AppendOnlyLog.encode("PING"));
            }
        } finally {
            lock.unlock();
        }
    }

    /** Rewrites the log from the cache. Takes the lock itself. */
    void compactNow() throws IOException {
        lock.lock();
        try {
            if (log == null) {
                throw new IOException("persistence is not enabled");
            }
            log.compact(cache);
        } finally {
            lock.unlock();
        }
    }

    /** A replica applying one record from its primary: update the cache and the local log, not the stream. */
    void applyReplicated(List<byte[]> command) throws IOException {
        lock.lock();
        try {
            AppendOnlyLog.applyRecord(command, cache);
            if (log != null && !new String(command.get(0), java.nio.charset.StandardCharsets.UTF_8).equalsIgnoreCase("PING")) {
                log.append(AppendOnlyLog.encodeRaw(command));
                log.maybeCompact(cache);
            }
        } finally {
            lock.unlock();
        }
    }

    /** A replica discards everything before a full resync. */
    void clearForResync() {
        lock.lock();
        try {
            cache.clear();
        } finally {
            lock.unlock();
        }
    }

    /** After a full resync the local log is rewritten from the new contents. */
    void compactAfterResync() throws IOException {
        lock.lock();
        try {
            if (log != null) {
                log.compact(cache);
            }
        } finally {
            lock.unlock();
        }
    }
}
