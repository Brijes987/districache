package com.distri.cache.raft;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a node's real storage for the simulator. The node sees exactly the real behaviour (entries covered by a
 * snapshot are gone), while the simulator's checkers can still read, through {@link #historic}, entries this node once
 * held and then compacted. An entry a node never held (it arrived inside a snapshot) is unknown here: null.
 */
final class AuditStorage implements RaftStorage {

    final RaftStorage inner;
    private final Map<Long, LogEntry> archive = new HashMap<>();

    AuditStorage(RaftStorage inner) {
        this.inner = inner;
    }

    /** The entry at {@code index} if this node ever held it, else null. */
    LogEntry historic(long index) {
        if (index > inner.snapshotIndex() && index <= inner.lastIndex()) {
            return inner.entry(index);
        }
        return archive.get(index);
    }

    @Override public long currentTerm() { return inner.currentTerm(); }
    @Override public int votedFor() { return inner.votedFor(); }
    @Override public void saveTermAndVote(long term, int votedFor) { inner.saveTermAndVote(term, votedFor); }
    @Override public long lastIndex() { return inner.lastIndex(); }
    @Override public LogEntry entry(long index) { return inner.entry(index); }
    @Override public long termAt(long index) { return inner.termAt(index); }
    @Override public void append(List<LogEntry> entries) { inner.append(entries); }
    @Override public void truncateFrom(long fromIndex) { inner.truncateFrom(fromIndex); }
    @Override public long snapshotIndex() { return inner.snapshotIndex(); }
    @Override public long snapshotTerm() { return inner.snapshotTerm(); }
    @Override public Snapshot snapshot() { return inner.snapshot(); }

    @Override
    public void installSnapshot(long index, long term, byte[] state) {
        // Only a snapshot that continues THIS log makes the entries below it part of this node's history. One that
        // replaces a diverged log (the entry at its index is missing or has another term) makes them stale, uncommitted
        // leftovers that must not be mistaken for committed ones.
        if (index > inner.snapshotIndex() && index <= inner.lastIndex() && inner.termAt(index) == term) {
            for (long i = inner.snapshotIndex() + 1; i <= index; i++) {
                archive.put(i, inner.entry(i));
            }
        }
        inner.installSnapshot(index, term, state);
    }
}
