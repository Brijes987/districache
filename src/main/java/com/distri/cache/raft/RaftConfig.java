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
 */
public record RaftConfig(int electionTimeoutMinTicks, int electionTimeoutMaxTicks, int heartbeatTicks,
        int maxEntriesPerMessage, boolean checkQuorum, Set<Fault> faults) {

    /** Known Raft mistakes that can be switched on so the test harness can prove it notices them. TESTING ONLY. */
    public enum Fault {
        /** A voter ignores whether the candidate's log is at least as up to date as its own. */
        SKIP_LOG_UP_TO_DATE_CHECK,
        /** A leader commits an entry from an earlier term as soon as a majority holds it (the Figure 8 mistake). */
        COMMIT_OLD_TERMS_BY_COUNTING,
        /** A follower deletes its log after prevLogIndex on every AppendEntries instead of only on a conflict. */
        TRUNCATE_ON_EVERY_APPEND,
        /** A follower raises commitIndex to leaderCommit without limiting it to the entries it verified. */
        COMMIT_PAST_VERIFIED_ENTRIES
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
        faults = Set.copyOf(faults);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(10, 20, 2, 64, true, EnumSet.noneOf(Fault.class));
    }

    public RaftConfig withFaults(Fault... faults) {
        Set<Fault> set = EnumSet.noneOf(Fault.class);
        set.addAll(java.util.Arrays.asList(faults));
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                checkQuorum, set);
    }

    public RaftConfig withCheckQuorum(boolean value) {
        return new RaftConfig(electionTimeoutMinTicks, electionTimeoutMaxTicks, heartbeatTicks, maxEntriesPerMessage,
                value, faults);
    }
}
