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

    long currentTerm();

    /** The candidate voted for in {@link #currentTerm()}, or -1. */
    int votedFor();

    void saveTermAndVote(long term, int votedFor);

    /** Index of the last entry, 0 for an empty log. */
    long lastIndex();

    /** The entry at {@code index} (1 &lt;= index &lt;= lastIndex). */
    LogEntry entry(long index);

    /** Term of the entry at {@code index}; 0 for index 0. */
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
