package com.distri.cache.raft;

import java.util.Arrays;

/**
 * One entry of the replicated log: the leader's term when it created the entry, and an opaque command for the
 * state machine. An empty command is a "no-op" entry a new leader appends to commit entries of earlier terms; it
 * is never passed to the state machine.
 */
public final class LogEntry {

    private final long term;
    private final byte[] command;

    public LogEntry(long term, byte[] command) {
        if (term <= 0) {
            throw new IllegalArgumentException("term must be positive");
        }
        this.term = term;
        this.command = command.clone();
    }

    public static LogEntry noop(long term) {
        return new LogEntry(term, new byte[0]);
    }

    public long term() {
        return term;
    }

    public byte[] command() {
        return command.clone();
    }

    public boolean isNoop() {
        return command.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry other && term == other.term && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(term) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term + ", " + (isNoop() ? "noop" : command.length + " bytes") + "]";
    }
}
