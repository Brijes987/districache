package com.distri.cache.raft;

import java.util.List;

/**
 * The state Raft must keep across restarts: the current term, who this node voted for in it, and the log. A method
 * must not return before its change is durable, because the node answers other servers right afterwards and its
 * answers (a vote, an acknowledged entry) are promises that a restart must not break.
 *
 * <p>Log indexes start at 1; index 0 means "before the first entry" and has term 0.
 */
public interface RaftStorage {

    /**
     * Largest snapshot that is taken, sent or stored. A snapshot is held in memory as one array on the leader (while it
     * is sent) and on a follower (while it is assembled), so this is a memory limit, not a wire limit: it travels in
     * chunks of at most {@link #MAX_SNAPSHOT_CHUNK_BYTES}.
     */
    int MAX_SNAPSHOT_BYTES = 128 * 1024 * 1024;

    /** Largest piece of a snapshot in one message; also what the wire format accepts. */
    int MAX_SNAPSHOT_CHUNK_BYTES = 4 * 1024 * 1024;

    /** The state machine as of log index {@code index} (whose entry had term {@code term}). */
    record Snapshot(long index, long term, byte[] state) {}

    /** Index of the last log entry covered by the snapshot (those entries are gone from the log), or 0 for none. */
    default long snapshotIndex() {
        return 0;
    }

    /** Term of the entry at {@link #snapshotIndex()}, or 0. */
    default long snapshotTerm() {
        return 0;
    }

    /** The stored snapshot, or null if there is none. */
    default Snapshot snapshot() {
        return null;
    }

    /**
     * Durably records a snapshot covering every entry up to {@code index} and drops those entries from the log. If this
     * log already holds the entry at {@code index} with term {@code term}, the entries after it are kept; otherwise (a
     * snapshot received from a leader that is ahead) the whole log is discarded and {@link #lastIndex()} becomes
     * {@code index}. An {@code index} at or below {@link #snapshotIndex()} is ignored. Entries up to {@code index}
     * must already be committed: they can no longer be inspected or changed.
     */
    default void installSnapshot(long index, long term, byte[] state) {
        throw new UnsupportedOperationException("this storage cannot hold snapshots");
    }

    long currentTerm();

    /** The candidate voted for in {@link #currentTerm()}, or -1. */
    int votedFor();

    void saveTermAndVote(long term, int votedFor);

    /** Index of the last entry (the snapshot's index if the log holds none after it), 0 for an empty log. */
    long lastIndex();

    /** The entry at {@code index} ({@code snapshotIndex() < index <= lastIndex}). */
    LogEntry entry(long index);

    /** Term of the entry at {@code index}; 0 for index 0; the snapshot's term for {@link #snapshotIndex()}. */
    default long termAt(long index) {
        return index == 0 ? 0 : entry(index).term();
    }

    void append(List<LogEntry> entries);

    /** Removes the entry at {@code fromIndex} and everything after it. */
    void truncateFrom(long fromIndex);

    /** Entries {@code from..to} inclusive. */
    default List<LogEntry> slice(long from, long to) {
        java.util.ArrayList<LogEntry> out = new java.util.ArrayList<>();
        for (long i = from; i <= to; i++) {
            out.add(entry(i));
        }
        return out;
    }
}
