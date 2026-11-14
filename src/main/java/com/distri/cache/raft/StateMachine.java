package com.distri.cache.raft;

/** What a committed command is applied to. Called in log order, once per committed non-empty entry. */
public interface StateMachine {
    void apply(long index, byte[] command);

    /**
     * The whole state after the last applied command, as bytes that {@link #restore} can rebuild it from. Called on the
     * node's thread between commands. State machines that do not support snapshots keep this default.
     */
    default byte[] snapshot() {
        throw new UnsupportedOperationException("this state machine does not support snapshots");
    }

    /** A frozen copy of the state at one moment, from which the snapshot bytes are produced later, on another thread. */
    interface SnapshotCapture {
        /** Must not touch the live state machine: it runs while the node goes on applying commands. */
        byte[] serialize();
    }

    /**
     * Freezes the state as of the last applied command and returns something that turns it into snapshot bytes. Called on
     * the node's thread, so it must be fast: copy references, do not encode. The default does all the work here (the old
     * behaviour: the node's thread is busy for as long as {@link #snapshot()} takes).
     */
    default SnapshotCapture captureSnapshot() {
        byte[] state = snapshot();
        return () -> state;
    }

    /**
     * Replaces the whole state with the one in {@code state}. Must either succeed completely or throw before changing
     * anything: it is also called while a node starts up, and a half-restored state would be silently wrong.
     */
    default void restore(byte[] state) {
        throw new UnsupportedOperationException("this state machine does not support snapshots");
    }
}
