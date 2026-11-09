package com.distri.cache.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory. It survives a simulated crash because the test keeps this object and builds a new
 * {@link RaftNode} on top of it; it does not survive the process.
 */
public final class InMemoryRaftStorage implements RaftStorage {

    private final List<LogEntry> log = new ArrayList<>();
    private long term;
    private int votedFor = -1;

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
        return log.size();
    }

    @Override
    public LogEntry entry(long index) {
        if (index < 1 || index > log.size()) {
            throw new IndexOutOfBoundsException("no log entry " + index + " (last is " + log.size() + ")");
        }
        return log.get((int) index - 1);
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
        while (log.size() >= fromIndex) {
            log.remove(log.size() - 1);
        }
    }
}
