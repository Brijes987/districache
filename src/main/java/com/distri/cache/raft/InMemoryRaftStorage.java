package com.distri.cache.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory. It survives a simulated crash because the test keeps this object and builds a new
 * {@link RaftNode} on top of it; it does not survive the process.
 */
public final class InMemoryRaftStorage implements RaftStorage {

    private final List<LogEntry> log = new ArrayList<>(); // entry at index i is log.get(i - base - 1)
    private long term;
    private int votedFor = -1;
    private long base;          // snapshot index: entries up to here are gone
    private long baseTerm;
    private byte[] baseState;

    @Override
    public long currentTerm() {
        return term;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public long lastIndex() {
        return base + log.size();
    }

    @Override
    public LogEntry entry(long index) {
        if (index <= base || index > lastIndex()) {
            throw new IndexOutOfBoundsException("no log entry " + index + " (log holds " + (base + 1) + ".."
                    + lastIndex() + ")");
        }
        return log.get((int) (index - base - 1));
    }

    @Override
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index == base) {
            return baseTerm;
        }
        return entry(index).term();
    }

    @Override
    public void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public void truncateFrom(long fromIndex) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("index must be >= 1");
        }
        if (fromIndex <= base) {
            throw new IllegalStateException("cannot truncate from " + fromIndex + ": entries up to " + base
                    + " are covered by a snapshot");
        }
        while (lastIndex() >= fromIndex) {
            log.remove(log.size() - 1);
        }
    }

    @Override
    public long snapshotIndex() {
        return base;
    }

    @Override
    public long snapshotTerm() {
        return baseTerm;
    }

    @Override
    public Snapshot snapshot() {
        return base == 0 ? null : new Snapshot(base, baseTerm, baseState.clone());
    }

    @Override
    public void installSnapshot(long index, long snapshotTerm, byte[] state) {
        if (index <= base) {
            return;
        }
        if (state.length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("snapshot of " + state.length + " bytes is too large");
        }
        boolean keepSuffix = index <= lastIndex() && termAt(index) == snapshotTerm;
        if (keepSuffix) {
            log.subList(0, (int) (index - base)).clear();
        } else {
            log.clear();
        }
        base = index;
        baseTerm = snapshotTerm;
        baseState = state.clone();
    }
}
