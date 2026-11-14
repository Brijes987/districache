package com.distri.cache.raft;

import java.util.EnumSet;
import java.util.Set;

/**
 * Timing is in <em>ticks</em>: the node counts calls to {@link RaftNode#tick()}, so the same code runs in a
 * simulation (a tick is one simulated step) and for real (a tick is, say, 10 ms).
 *
 * @param electionTimeoutMinTicks a follower starts an election after a random wait in [min, max] ticks without
 *     hearing from a leader
 * @param heartbeatTicks how often a leader sends (possibly empty) AppendEntries; must be well below the election timeout
 * @param maxEntriesPerMessage cap on entries in one AppendEntries
 * @param checkQuorum a leader that has not heard from a majority within an election timeout steps down
 * @param faults deliberate bugs, for testing the tests; empty in real use
 * @param snapshotEvery take a snapshot (and drop the log it covers) once this many entries have been applied since the
 *     last one; 0 turns snapshots off
 * @param snapshotChunkBytes largest piece of a snapshot sent in one message (at most
 *     {@link RaftStorage#MAX_SNAPSHOT_CHUNK_BYTES}); a bigger snapshot is sent as several chunks, one in flight at a time
 */
public record RaftConfig(int electionTimeoutMinTicks, int electionTimeoutMaxTicks, int heartbeatTicks,
        int maxEntriesPerMessage, boolean checkQuorum, Set<Fault> faults, int snapshotEvery, int snapshotChunkBytes) {

    /** Default size of a snapshot chunk. */
    public static final int DEFAULT_SNAPSHOT_CHUNK_BYTES = 1024 * 1024;

    /** With the default chunk size. */
    public RaftConfig(int electionTimeoutMinTicks, int electionTimeoutMaxTicks, int heartbeatTicks,
            int maxEntriesPerMessage, boolean checkQuorum, Set<Fault> faults, int snapshotEvery) {
        this(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage, checkQuorum,
                faults, snapshotEvery, DEFAULT_SNAPSHOT_CHUNK_BYTES);
    }

    /** Without snapshots. */
    public RaftConfig(int electionTimeoutMinTicks, int electionTimeoutMaxTicks, int heartbeatTicks,
            int maxEntriesPerMessage, boolean checkQuorum, Set<Fault> faults) {
        this(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage, checkQuorum, faults,
                0);
    }

    /** Known Raft mistakes that can be switched on so the test harness can prove it notices them. TESTING ONLY. */
    public enum Fault {
        /** A voter ignores whether the candidate's log is at least as up to date as its own. */
        SKIP_LOG_UP_TO_DATE_CHECK,
        /** A leader commits an entry from an earlier term as soon as a majority holds it (the Figure 8 mistake). */
        COMMIT_OLD_TERMS_BY_COUNTING,
        /** A follower deletes its log after prevLogIndex on every AppendEntries instead of only on a conflict. */
        TRUNCATE_ON_EVERY_APPEND,
        /** A follower raises commitIndex to leaderCommit without limiting it to the entries it verified. */
        COMMIT_PAST_VERIFIED_ENTRIES,
        /** A follower installs a snapshot that is older than what it has already committed, rolling its state back. */
        INSTALL_STALE_SNAPSHOT,
        /** A node takes a snapshot of the state BEFORE applying the entry it says the snapshot covers. */
        SNAPSHOT_BEFORE_APPLY,
        /** A leader answers a read-index read without checking that a majority still follows it. */
        READ_SKIP_LEADERSHIP_CHECK,
        /** A read-index read does not wait for the leader's first entry of its term to commit. */
        READ_IGNORE_TERM_START,
        /** A read-index read counts any earlier acknowledgement as proof of leadership, not one sent after the read. */
        READ_COUNT_OLD_ACKS,
        /** A follower appends a snapshot chunk wherever it arrives, ignoring its offset (duplicates and reordering corrupt it). */
        SNAPSHOT_CHUNK_IGNORE_OFFSET
    }

    public RaftConfig {
        if (electionTimeoutMinTicks <= 0 || electionTimeoutMaxTicks < electionTimeoutMinTicks) {
            throw new IllegalArgumentException("bad election timeout range");
        }
        if (heartbeatTicks <= 0 || heartbeatTicks * 2 > electionTimeoutMinTicks) {
            throw new IllegalArgumentException("heartbeat must be at most half the minimum election timeout");
        }
        if (maxEntriesPerMessage <= 0) {
            throw new IllegalArgumentException("maxEntriesPerMessage must be positive");
        }
        if (snapshotEvery < 0) {
            throw new IllegalArgumentException("snapshotEvery must not be negative");
        }
        if (snapshotChunkBytes <= 0 || snapshotChunkBytes > RaftStorage.MAX_SNAPSHOT_CHUNK_BYTES) {
            throw new IllegalArgumentException("snapshotChunkBytes must be in 1.." + RaftStorage.MAX_SNAPSHOT_CHUNK_BYTES);
        }
        faults = Set.copyOf(faults);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(10, 20, 2, 64, true, EnumSet.noneOf(Fault.class));
    }

    public RaftConfig withFaults(Fault... faults) {
        Set<Fault> set = EnumSet.noneOf(Fault.class);
        set.addAll(java.util.Arrays.asList(faults));
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                checkQuorum, set, snapshotEvery, snapshotChunkBytes);
    }

    public RaftConfig withCheckQuorum(boolean value) {
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                value, faults, snapshotEvery, snapshotChunkBytes);
    }

    /** Snapshot after every {@code entries} applied entries (0 = never). */
    public RaftConfig withSnapshotEvery(int entries) {
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                checkQuorum, faults, entries, snapshotChunkBytes);
    }

    /** Send snapshots in chunks of at most {@code bytes}. */
    public RaftConfig withSnapshotChunkBytes(int bytes) {
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                checkQuorum, faults, snapshotEvery, bytes);
    }
}
