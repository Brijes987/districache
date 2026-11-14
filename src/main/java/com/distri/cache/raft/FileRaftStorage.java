package com.distri.cache.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Raft state on disk: the current term and vote in one small file, the log in another.
 *
 * <p>Every method that changes state forces it to disk before returning, because the node sends votes and
 * acknowledgements right afterwards and a restart must not break those promises.
 *
 * <p>Formats:
 * <ul>
 *   <li>{@code raft.meta}: term (8 bytes), votedFor (4), CRC-32 of those 12 bytes (4). Replaced with
 *       write-temp, force, atomic rename, so it is either the old or the new value, never a mixture.</li>
 *   <li>{@code raft.log}: records of {@code length (4) | CRC-32 of body (4) | body}, where the body is
 *       {@code term (8) | command bytes}. A log that has been compacted starts with a 16-byte header
 *       {@code magic (4) | base index (8) | CRC-32 (4)}; its first record is entry {@code base + 1}. A file without
 *       the header is a log that starts at entry 1.</li>
 *   <li>{@code raft.snapshot}: {@code magic (4) | index (8) | term (8) | length (4) | state | CRC-32 (4)}, replaced
 *       with write-temp, force, atomic rename. Written BEFORE the log is compacted, so a crash in between leaves
 *       a snapshot and a longer log, and opening finishes the compaction.</li>
 * </ul>
 *
 * <p>Recovery on open: a record cut off at the end (crash during an append), a last record whose checksum fails, or a
 * zero-filled tail is a torn write and is dropped by truncating the file. A bad record <em>followed by more data</em>
 * cannot be a torn write, so opening fails instead of silently discarding the rest of the log.
 *
 * <p>Limits: an I/O error throws {@link UncheckedIOException}; the node must stop (fail-stop) because it can no longer
 * keep its promises. This class is not thread-safe, like {@link RaftNode}. The whole (uncompacted) log is also kept in
 * memory; {@link #installSnapshot} is what keeps it, and the file, from growing without bound. Whether {@code force} really reaches the platters depends on the
 * disk and file system; this has been tested with simulated torn and corrupted files, not with power loss.
 */
public final class FileRaftStorage implements RaftStorage, AutoCloseable {

    static final int MAX_COMMAND_BYTES = 16 * 1024 * 1024;
    private static final int HEADER = 8;
    private static final int LOG_MAGIC = 0x52414654;      // "RAFT"; larger than any valid record length
    private static final int SNAPSHOT_MAGIC = 0x52534e50; // "RSNP"
    private static final int LOG_HEADER = 16;

    private final Path dir;
    private final boolean fsync;
    private final Path metaFile;
    private final Path logFile;
    private final Path snapshotFile;
    private FileChannel logChannel;
    private final List<LogEntry> entries = new ArrayList<>(); // entry at index i is entries.get(i - base - 1)
    private final List<Long> offsets = new ArrayList<>(); // start position of each record
    private long base;          // index of the last entry covered by the snapshot (0: none)
    private long baseTerm;
    private long logBase;       // base index recorded in the log file's header (0 without one)
    private long endOffset;
    private long term;
    private int votedFor = -1;
    private boolean closed;

    public FileRaftStorage(Path dir) throws IOException {
        this(dir, true);
    }

    /**
     * @param fsync force every change to disk before returning. Leave it on. Switching it off is only for tests that
     *     exercise recovery logic and would otherwise spend their time waiting for the disk; with it off, nothing
     *     promises survival of a power failure.
     */
    public FileRaftStorage(Path dir, boolean fsync) throws IOException {
        this.fsync = fsync;
        this.dir = dir;
        Files.createDirectories(dir);
        this.metaFile = dir.resolve("raft.meta");
        Files.deleteIfExists(dir.resolve("raft.meta.tmp"));
        Files.deleteIfExists(dir.resolve("raft.snapshot.tmp"));
        Files.deleteIfExists(dir.resolve("raft.log.tmp"));
        try (java.nio.file.DirectoryStream<Path> leftovers = Files.newDirectoryStream(dir, "raft.snapshot.bg.*.tmp")) {
            for (Path leftover : leftovers) { // a background snapshot that was never committed
                Files.deleteIfExists(leftover);
            }
        }
        this.logFile = dir.resolve("raft.log");
        this.snapshotFile = dir.resolve("raft.snapshot");
        loadMeta();
        loadSnapshotHeader();
        this.logChannel = FileChannel.open(logFile, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            loadLog();
            if (base > logBase) {
                finishInterruptedCompaction();
            } else if (base < logBase) {
                throw new IOException("raft.log starts after entry " + logBase + " but the newest snapshot only covers "
                        + base + ": the snapshot file is missing or old, refusing to guess");
            }
        } catch (IOException | RuntimeException e) {
            logChannel.close();
            throw e;
        }
    }

    // ------------------------------------------------------------------ term and vote

    private void loadMeta() throws IOException {
        if (!Files.exists(metaFile)) {
            return;
        }
        byte[] bytes = Files.readAllBytes(metaFile);
        if (bytes.length != 16) {
            throw new IOException("raft.meta has " + bytes.length + " bytes, expected 16: refusing to guess");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long t = buf.getLong();
        int v = buf.getInt();
        int crc = buf.getInt();
        if (crc != crc32(bytes, 0, 12)) {
            throw new IOException("raft.meta checksum mismatch: the file is corrupt");
        }
        term = t;
        votedFor = v;
    }

    @Override
    public long currentTerm() {
        return term;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    @Override
    public void saveTermAndVote(long newTerm, int newVotedFor) {
        ensureOpen();
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(newTerm).putInt(newVotedFor);
        buf.putInt(crc32(buf.array(), 0, 12));
        Path tmp = dir.resolve("raft.meta.tmp");
        try {
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                buf.flip();
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                force(ch);
            }
            try {
                Files.move(tmp, metaFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING);
            }
            syncDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist term and vote", e);
        }
        term = newTerm;
        votedFor = newVotedFor;
    }

    /** Makes the rename itself durable. Not possible on every platform (Windows), so a failure is ignored. */
    private void syncDirectory() {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            force(ch);
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort
        }
    }

    // ------------------------------------------------------------------ log

    private void loadLog() throws IOException {
        long size = logChannel.size();
        ByteBuffer all = ByteBuffer.allocate(Math.toIntExact(size));
        while (all.hasRemaining()) {
            if (logChannel.read(all, all.position()) < 0) {
                break;
            }
        }
        byte[] data = all.array();
        long pos = 0;
        logBase = 0;
        if (size >= 4 && ByteBuffer.wrap(data, 0, 4).getInt() == LOG_MAGIC) {
            if (size < LOG_HEADER) {
                throw new IOException("raft.log header is cut short: refusing to guess");
            }
            ByteBuffer h = ByteBuffer.wrap(data, 0, LOG_HEADER);
            h.getInt();
            long b = h.getLong();
            int crc = h.getInt();
            if (crc != crc32(data, 4, 8) || b <= 0) {
                throw new IOException("raft.log header is corrupt");
            }
            logBase = b;
            pos = LOG_HEADER;
        }
        endOffset = pos;
        while (pos < size) {
            long remaining = size - pos;
            if (remaining < HEADER) {
                dropTail(pos, "partial record header");
                return;
            }
            ByteBuffer header = ByteBuffer.wrap(data, (int) pos, HEADER);
            int length = header.getInt();
            int crc = header.getInt();
            if (length < 8 || length > MAX_COMMAND_BYTES + 8) {
                if (allZero(data, (int) pos, (int) size)) {
                    dropTail(pos, "zero-filled tail");
                    return;
                }
                throw new IOException("raft.log is corrupt at byte " + pos + ": impossible record length " + length);
            }
            if (length > remaining - HEADER) {
                dropTail(pos, "record cut off by a crash");
                return;
            }
            int bodyStart = (int) pos + HEADER;
            if (crc32(data, bodyStart, length) != crc) {
                if (pos + HEADER + length == size) {
                    dropTail(pos, "last record failed its checksum");
                    return;
                }
                throw new IOException("raft.log is corrupt at byte " + pos
                        + ": checksum mismatch in a record that is followed by more data");
            }
            ByteBuffer body = ByteBuffer.wrap(data, bodyStart, length);
            long entryTerm = body.getLong();
            byte[] command = new byte[length - 8];
            body.get(command);
            if (entryTerm <= 0) {
                throw new IOException("raft.log is corrupt at byte " + pos + ": non-positive term");
            }
            entries.add(new LogEntry(entryTerm, command));
            offsets.add(pos);
            pos += HEADER + length;
        }
        endOffset = pos;
    }

    private void dropTail(long pos, String why) throws IOException {
        logChannel.truncate(pos);
        force(logChannel);
        endOffset = pos;
        droppedTailReason = why;
    }

    private String droppedTailReason;

    /** Why the end of the log was cut on open, or null if the file was clean. For tests and diagnostics. */
    public String droppedTailReason() {
        return droppedTailReason;
    }

    private static boolean allZero(byte[] data, int from, int to) {
        for (int i = from; i < to; i++) {
            if (data[i] != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long lastIndex() {
        return logBase + entries.size();
    }

    @Override
    public LogEntry entry(long index) {
        if (index <= logBase || index > lastIndex()) {
            throw new IndexOutOfBoundsException("no log entry " + index + " (log holds " + (logBase + 1) + ".."
                    + lastIndex() + ")");
        }
        return entries.get((int) (index - logBase - 1));
    }

    @Override
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index == logBase) {
            return baseTerm;
        }
        return entry(index).term();
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        ensureOpen();
        if (newEntries.isEmpty()) {
            return;
        }
        long pos = endOffset;
        List<Long> newOffsets = new ArrayList<>(newEntries.size());
        try {
            for (LogEntry e : newEntries) {
                byte[] command = e.command();
                if (command.length > MAX_COMMAND_BYTES) {
                    throw new IllegalArgumentException("command of " + command.length + " bytes is too large");
                }
                ByteBuffer body = ByteBuffer.allocate(8 + command.length);
                body.putLong(e.term()).put(command);
                ByteBuffer record = ByteBuffer.allocate(HEADER + body.capacity());
                record.putInt(body.capacity()).putInt(crc32(body.array(), 0, body.capacity())).put(body.array());
                record.flip();
                long writeAt = pos;
                while (record.hasRemaining()) {
                    writeAt += logChannel.write(record, writeAt);
                }
                newOffsets.add(pos);
                pos = writeAt;
            }
            force(logChannel);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append to the raft log", e);
        }
        entries.addAll(newEntries);
        offsets.addAll(newOffsets);
        endOffset = pos;
    }

    @Override
    public void truncateFrom(long fromIndex) {
        ensureOpen();
        if (fromIndex < 1) {
            throw new IllegalArgumentException("index must be >= 1");
        }
        if (fromIndex <= logBase) {
            throw new IllegalStateException("cannot truncate from " + fromIndex + ": entries up to " + logBase
                    + " are covered by a snapshot");
        }
        if (fromIndex > lastIndex()) {
            return;
        }
        long cut = offsets.get((int) (fromIndex - logBase - 1));
        try {
            logChannel.truncate(cut);
            force(logChannel);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot truncate the raft log", e);
        }
        while (lastIndex() >= fromIndex) {
            entries.remove(entries.size() - 1);
            offsets.remove(offsets.size() - 1);
        }
        endOffset = cut;
    }

    // ------------------------------------------------------------------ snapshot

    @Override
    public long snapshotIndex() {
        return base;
    }

    @Override
    public long snapshotTerm() {
        return baseTerm;
    }

    /** Reads only enough of raft.snapshot to learn its index and term (and checks the whole file's checksum). */
    private void loadSnapshotHeader() throws IOException {
        Snapshot snap = readSnapshotFile();
        if (snap != null) {
            base = snap.index();
            baseTerm = snap.term();
        }
    }

    private Snapshot readSnapshotFile() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(snapshotFile);
        if (bytes.length < 28) {
            throw new IOException("raft.snapshot is too short: refusing to guess");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        if (buf.getInt() != SNAPSHOT_MAGIC) {
            throw new IOException("raft.snapshot has the wrong magic number");
        }
        long index = buf.getLong();
        long snapTerm = buf.getLong();
        int length = buf.getInt();
        if (index <= 0 || snapTerm <= 0 || length < 0 || length > MAX_SNAPSHOT_BYTES || bytes.length != 28 + length) {
            throw new IOException("raft.snapshot is malformed: refusing to guess");
        }
        byte[] state = new byte[length];
        buf.get(state);
        int crc = buf.getInt();
        if (crc != crc32(bytes, 0, bytes.length - 4)) {
            throw new IOException("raft.snapshot checksum mismatch: the file is corrupt");
        }
        return new Snapshot(index, snapTerm, state);
    }

    @Override
    public Snapshot snapshot() {
        if (base == 0) {
            return null;
        }
        try {
            return readSnapshotFile();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the snapshot", e);
        }
    }

    @Override
    public void installSnapshot(long index, long term, byte[] state) {
        ensureOpen();
        if (index <= base) {
            return;
        }
        if (state.length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("snapshot of " + state.length + " bytes is too large");
        }
        if (term <= 0) {
            throw new IllegalArgumentException("term must be positive");
        }
        ByteBuffer buf = ByteBuffer.allocate(28 + state.length);
        buf.putInt(SNAPSHOT_MAGIC).putLong(index).putLong(term).putInt(state.length).put(state);
        buf.putInt(crc32(buf.array(), 0, 24 + state.length));
        try {
            writeAtomically(snapshotFile, dir.resolve("raft.snapshot.tmp"), buf.array());
            base = index; // the snapshot is durable: from now on it is the truth
            baseTerm = term;
            compactLogTo(index, term);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist the snapshot", e);
        }
    }

    private record FilePrepared(long index, long term, Path file) implements PreparedSnapshot {}

    private final java.util.concurrent.atomic.AtomicLong preparedCounter = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Writes and forces the snapshot file under a name of its own, so it can run on another thread while the node's thread
     * goes on appending to the log. Nothing the storage reports changes until {@link #commitSnapshot}. A crash before that
     * leaves a stray file that the next start deletes.
     */
    @Override
    public PreparedSnapshot prepareSnapshot(long index, long term, byte[] state) {
        if (state.length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("snapshot of " + state.length + " bytes is too large");
        }
        if (term <= 0) {
            throw new IllegalArgumentException("term must be positive");
        }
        ByteBuffer buf = ByteBuffer.allocate(28 + state.length);
        buf.putInt(SNAPSHOT_MAGIC).putLong(index).putLong(term).putInt(state.length).put(state);
        buf.putInt(crc32(buf.array(), 0, 24 + state.length));
        Path tmp = dir.resolve("raft.snapshot.bg." + index + "." + preparedCounter.incrementAndGet() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer out = ByteBuffer.wrap(buf.array());
            while (out.hasRemaining()) {
                ch.write(out);
            }
            force(ch);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // nothing more to do
            }
            throw new UncheckedIOException("cannot write the snapshot", e);
        }
        return new FilePrepared(index, term, tmp);
    }

    @Override
    public boolean commitSnapshot(PreparedSnapshot prepared) {
        ensureOpen();
        FilePrepared p = (FilePrepared) prepared;
        if (p.index() <= base) {
            discardSnapshot(prepared);
            return false;
        }
        try {
            try {
                Files.move(p.file(), snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(p.file(), snapshotFile, StandardCopyOption.REPLACE_EXISTING);
            }
            syncDirectory();
            base = p.index(); // the snapshot is durable: from now on it is the truth
            baseTerm = p.term();
            compactLogTo(p.index(), p.term());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist the snapshot", e);
        }
        return true;
    }

    @Override
    public void discardSnapshot(PreparedSnapshot prepared) {
        try {
            Files.deleteIfExists(((FilePrepared) prepared).file());
        } catch (IOException ignored) {
            // a stray file is deleted at the next start
        }
    }

    /** Completes a compaction that a crash interrupted after the snapshot was written. */
    private void finishInterruptedCompaction() throws IOException {
        compactLogTo(base, baseTerm);
    }

    /**
     * Rewrites the log file so that it starts after {@code index}: keeping the entries after it if this log holds the
     * entry at {@code index} with {@code term}, otherwise keeping nothing. Write temp, force, atomic rename.
     */
    private void compactLogTo(long index, long term) throws IOException {
        boolean keep = index <= lastIndex() && index > logBase && entries.get((int) (index - logBase - 1)).term() == term;
        List<LogEntry> retained = keep
                ? new ArrayList<>(entries.subList((int) (index - logBase), entries.size())) : List.of();
        ByteBuffer header = ByteBuffer.allocate(LOG_HEADER);
        header.putInt(LOG_MAGIC).putLong(index);
        header.putInt(crc32(header.array(), 4, 8));
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(header.array());
        List<Long> newOffsets = new ArrayList<>(retained.size());
        long pos = LOG_HEADER;
        for (LogEntry e : retained) {
            byte[] record = encodeRecord(e);
            newOffsets.add(pos);
            out.write(record);
            pos += record.length;
        }
        logChannel.close();
        try {
            writeAtomically(logFile, dir.resolve("raft.log.tmp"), out.toByteArray());
        } finally {
            logChannel = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        }
        entries.clear();
        entries.addAll(retained);
        offsets.clear();
        offsets.addAll(newOffsets);
        logBase = index;
        endOffset = pos;
    }

    private static byte[] encodeRecord(LogEntry e) {
        byte[] command = e.command();
        ByteBuffer body = ByteBuffer.allocate(8 + command.length);
        body.putLong(e.term()).put(command);
        ByteBuffer record = ByteBuffer.allocate(HEADER + body.capacity());
        record.putInt(body.capacity()).putInt(crc32(body.array(), 0, body.capacity())).put(body.array());
        return record.array();
    }

    private void writeAtomically(Path target, Path tmp, byte[] bytes) throws IOException {
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            force(ch);
        }
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        syncDirectory();
    }

    // ------------------------------------------------------------------ housekeeping

    private void force(FileChannel channel) throws IOException {
        if (fsync) {
            channel.force(true);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("storage is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            logChannel.close();
        }
    }

    private static int crc32(byte[] data, int off, int len) {
        CRC32 crc = new CRC32();
        crc.update(data, off, len);
        return (int) crc.getValue();
    }
}
