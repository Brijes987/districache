package com.distri.cache.server;

import com.distri.cache.CacheSnapshotEntry;
import com.distri.cache.ShardedLRUCache;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An append-only log of write commands, so the cache can be rebuilt after a restart.
 *
 * <p>Each record is one command in the same wire format the server reads ({@code *4 $3 SET ...}),
 * so the log is replayed with the same parser. Records are {@code SET key value} (with
 * {@code PXAT <absolute epoch millis>} when the entry has a TTL, so replaying later does not stretch
 * the TTL) and {@code DEL key ...}.
 *
 * <p>Guarantees and limits:
 * <ul>
 *   <li>One lock serialises "apply to the cache + append to the log", so the log order is the
 *       order in which the commands took effect. This limits write concurrency when persistence is
 *       on; reads are not affected.</li>
 *   <li>A record cut off at the end of the file (a crash in the middle of an append) is dropped on
 *       startup and the file is truncated to the last whole record. A malformed record in the
 *       middle of the file makes startup fail instead of silently dropping what follows. There are
 *       no checksums, so corruption that still parses is not detected.</li>
 *   <li>{@link FsyncPolicy#ALWAYS} forces each record to disk before the reply; {@code EVERYSEC}
 *       forces once a second (up to about a second of writes can be lost on a crash); {@code NO}
 *       leaves it to the operating system.</li>
 *   <li>Evictions are not logged. After a restart the cache holds the replayed writes subject to
 *       capacity, and recency from reads is not preserved.</li>
 *   <li>{@link #compact} rewrites the log from the live entries (least recently used first) and
 *       swaps the file in with a rename. Writers wait while it runs.</li>
 * </ul>
 */
final class AppendOnlyLog implements AutoCloseable {

    enum FsyncPolicy {
        ALWAYS, EVERYSEC, NO;

        static FsyncPolicy parse(String text) {
            return switch (text.toLowerCase(Locale.ROOT)) {
                case "always" -> ALWAYS;
                case "everysec" -> EVERYSEC;
                case "no" -> NO;
                default -> throw new IllegalArgumentException("fsync policy must be always, everysec or no");
            };
        }
    }

    /** What startup recovery found. */
    record ReplayResult(long records, long truncatedBytes) {}

    /** Compact automatically once the log is this big and has doubled since the last compaction. */
    static final long DEFAULT_COMPACTION_THRESHOLD_BYTES = 64L << 20;

    private final Path path;
    private final FsyncPolicy policy;
    private final long compactionThresholdBytes;
    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService syncer;

    private FileChannel channel;
    private long size;
    private long sizeAfterLastCompaction;
    private boolean dirty;
    private boolean closed;
    private ReplayResult replayResult = new ReplayResult(0, 0);

    private AppendOnlyLog(Path path, FsyncPolicy policy, long compactionThresholdBytes) {
        this.path = path;
        this.policy = policy;
        this.compactionThresholdBytes = compactionThresholdBytes;
        if (policy == FsyncPolicy.EVERYSEC) {
            this.syncer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "districache-aof-sync");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.syncer = null;
        }
    }

    /**
     * Opens (creating if needed) the log at {@code path}, replays it into {@code cache}, and
     * returns it ready for appends.
     *
     * @throws IOException if the file cannot be read or contains a malformed record that is not
     *     just an incomplete last record
     */
    static AppendOnlyLog open(Path path, FsyncPolicy policy, long compactionThresholdBytes,
            ShardedLRUCache<String, String> cache) throws IOException {
        AppendOnlyLog log = new AppendOnlyLog(path, policy, compactionThresholdBytes);
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.exists(path)) {
            log.replayResult = replay(path, cache);
        }
        log.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        log.size = log.channel.size();
        log.sizeAfterLastCompaction = log.size;
        if (log.syncer != null) {
            log.syncer.scheduleWithFixedDelay(log::syncIfDirty, 1, 1, TimeUnit.SECONDS);
        }
        return log;
    }

    ReplayResult replayResult() {
        return replayResult;
    }

    Path path() {
        return path;
    }

    /** Held by the caller around "apply to cache, then {@link #append}". Re-entrant. */
    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    /** Current size of the log file in bytes. */
    long sizeBytes() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends one command. The caller must hold {@link #lock()} so that the log order matches the
     * order the commands were applied to the cache. Honors the fsync policy.
     */
    void append(String... args) throws IOException {
        append(encode(args));
    }

    private void append(byte[] record) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("log is closed");
            }
            ByteBuffer buf = ByteBuffer.wrap(record);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            size += record.length;
            if (policy == FsyncPolicy.ALWAYS) {
                channel.force(false);
            } else {
                dirty = true;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Compacts if the log has grown past the threshold and at least doubled since the last
     * compaction. The caller holds {@link #lock()}. Failures are returned to the caller as
     * IOException; the log stays usable because the old file is only replaced by a complete new one.
     */
    void maybeCompact(ShardedLRUCache<String, String> cache) throws IOException {
        lock.lock();
        try {
            if (compactionThresholdBytes > 0 && size >= compactionThresholdBytes
                    && size >= 2 * Math.max(sizeAfterLastCompaction, 1)) {
                compact(cache);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rewrites the log so it holds one record per live entry. Blocks appends while it runs.
     * The new file is written and forced under a temporary name, then renamed over the old one, so a
     * crash at any point leaves either the whole old log or the whole new one.
     */
    void compact(ShardedLRUCache<String, String> cache) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("log is closed");
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".rewrite.tmp");
            long written = 0;
            try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                for (CacheSnapshotEntry<String, String> e : cache.snapshot()) {
                    byte[] record = e.expiresAtMillis() > 0
                            ? encode("SET", e.key(), e.value(), "PXAT", Long.toString(e.expiresAtMillis()))
                            : encode("SET", e.key(), e.value());
                    ByteBuffer buf = ByteBuffer.wrap(record);
                    while (buf.hasRemaining()) {
                        out.write(buf);
                    }
                    written += record.length;
                }
                out.force(true);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
            // The old file must be closed before it is replaced (Windows refuses otherwise).
            channel.force(false);
            channel.close();
            try {
                try {
                    Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                // Whether or not the move worked, reopen whatever is at `path` so the log stays usable.
                channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND);
                size = channel.size();
                sizeAfterLastCompaction = size;
                dirty = false;
            }
            syncDirectory(path);
        } finally {
            lock.unlock();
        }
    }

    private void syncIfDirty() {
        lock.lock();
        try {
            if (dirty && !closed) {
                channel.force(false);
                dirty = false;
            }
        } catch (IOException e) {
            System.err.println("DistriCache: fsync of the append-only log failed: " + e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (syncer != null) {
                syncer.shutdownNow();
            }
            try {
                channel.force(false);
            } finally {
                channel.close();
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ replay

    /**
     * Applies every whole record in the file to {@code cache}. A record cut off by the end of the
     * file is dropped and the file is truncated to the end of the last whole record.
     */
    static ReplayResult replay(Path path, ShardedLRUCache<String, String> cache) throws IOException {
        long fileSize = Files.size(path);
        long goodEnd = 0;
        long records = 0;
        try (java.io.InputStream raw = new BufferedInputStream(Files.newInputStream(path), 1 << 16)) {
            RespReader reader = new RespReader(raw);
            while (true) {
                List<byte[]> command;
                try {
                    command = reader.readCommand();
                } catch (EOFException e) {
                    break; // the last record was cut off
                } catch (RespProtocolException e) {
                    throw new IOException("append-only log " + path + " is corrupt near byte " + goodEnd
                            + ": " + e.getMessage() + " (not an incomplete last record, so not repaired automatically)", e);
                }
                if (command == null) {
                    break; // clean end of file
                }
                if (command.isEmpty()) {
                    goodEnd = reader.position();
                    continue;
                }
                try {
                    apply(command, cache);
                } catch (IllegalArgumentException e) {
                    throw new IOException("append-only log " + path + " has an invalid record near byte "
                            + goodEnd + ": " + e.getMessage(), e);
                }
                records++;
                goodEnd = reader.position();
            }
        }
        long truncated = fileSize - goodEnd;
        if (truncated > 0) {
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                ch.truncate(goodEnd);
                ch.force(true);
            }
        }
        return new ReplayResult(records, truncated);
    }

    private static void apply(List<byte[]> command, ShardedLRUCache<String, String> cache) {
        String name = text(command.get(0)).toUpperCase(Locale.ROOT);
        switch (name) {
            case "SET" -> {
                int n = command.size();
                if (n != 3 && n != 5) {
                    throw new IllegalArgumentException("SET with " + n + " arguments");
                }
                String key = text(command.get(1));
                String value = text(command.get(2));
                if (n == 3) {
                    cache.put(key, value);
                    return;
                }
                if (!text(command.get(3)).equalsIgnoreCase("PXAT")) {
                    throw new IllegalArgumentException("unexpected SET option");
                }
                long at;
                try {
                    at = Long.parseLong(text(command.get(4)));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("bad PXAT value");
                }
                long ttl = at - System.currentTimeMillis();
                if (ttl > 0) {
                    cache.put(key, value, ttl);
                } else {
                    cache.delete(key); // already expired: it replaces any earlier value by nothing
                }
            }
            case "DEL" -> {
                for (int i = 1; i < command.size(); i++) {
                    cache.delete(text(command.get(i)));
                }
            }
            default -> throw new IllegalArgumentException("unknown command " + name);
        }
    }

    // ------------------------------------------------------------------ encoding

    static byte[] encode(String... args) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(64);
        try {
            write(out, "*" + args.length + "\r\n");
            for (String arg : args) {
                byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
                write(out, "$" + bytes.length + "\r\n");
                out.write(bytes);
                write(out, "\r\n");
            }
        } catch (IOException e) {
            throw new IllegalStateException(e); // ByteArrayOutputStream does not throw
        }
        return out.toByteArray();
    }

    private static void write(OutputStream out, String ascii) throws IOException {
        out.write(ascii.getBytes(StandardCharsets.US_ASCII));
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void syncDirectory(Path file) {
        Path dir = file.toAbsolutePath().getParent();
        if (dir == null) {
            return;
        }
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Not possible on every platform (for example Windows); the rename itself is still atomic.
        }
    }
}
