package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Randomized fault injection. Every run is determined by its seed; if one fails, the message names the seed and the
 * recent events so the same run can be replayed.
 */
class RaftChaosTest {

    @Test
    void safetyHoldsUnderRandomFailuresAcross60Seeds() {
        for (long seed = 1; seed <= 60; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.normal(), RaftConfig.defaults());
        }
    }

    @Test
    void safetyHoldsUnderBrutalFailuresAcross40Seeds() {
        for (long seed = 1_000; seed < 1_040; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.brutal(), RaftConfig.defaults());
        }
    }

    @Test
    void safetyAlsoHoldsWithoutCheckQuorum() {
        // check-quorum is an availability feature; the safety properties must not depend on it
        for (long seed = 2_000; seed < 2_020; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.brutal(), RaftConfig.defaults().withCheckQuorum(false));
        }
    }

    @Test
    void theSameSeedReplaysTheSameHistory() {
        long first = fingerprintOf(77);
        long second = fingerprintOf(77);
        assertEquals(first, second, "a seed must reproduce the run exactly, or failures could not be replayed");
        assertNotEquals(first, fingerprintOf(78));
    }

    private static long fingerprintOf(long seed) {
        SimCluster cluster = new SimCluster(5, seed, RaftConfig.defaults());
        Chaos chaos = new Chaos(cluster, seed);
        chaos.run(1_500, Chaos.Profile.brutal());
        return cluster.fingerprint();
    }

    @Test
    void theChaosActuallyHappens() {
        // guard against a harness that silently does nothing: the run must elect several leaders, crash nodes,
        // lose messages and commit commands
        SimCluster cluster = new SimCluster(5, 4242, RaftConfig.defaults());
        Chaos chaos = new Chaos(cluster, 4242);
        chaos.run(3_000, Chaos.Profile.brutal());
        long elections = cluster.maxTermSeen();
        assertTrue(elections >= 5, "expected many elections, saw terms up to " + elections);
        assertTrue(cluster.messagesSent > 5_000, "too little traffic: " + cluster.messagesSent);
        assertTrue(cluster.messagesDelivered < cluster.messagesSent, "some messages should have been lost");
        assertTrue(cluster.committedEntries().size() > 20,
                "expected commits during chaos, saw " + cluster.committedEntries().size());
    }

    @Test
    void safetyHoldsWithSnapshotsAndCompactedLogsUnderFailures() {
        for (long seed = 3_000; seed < 3_040; seed++) {
            Chaos.fullRun(seed, 3_000, seed % 2 == 0 ? Chaos.Profile.normal() : Chaos.Profile.brutal(),
                    RaftConfig.defaults().withSnapshotEvery(8));
        }
    }

    @Test
    void safetyHoldsWithSnapshotsAfterEveryEntry() {
        for (long seed = 3_100; seed < 3_120; seed++) {
            Chaos.fullRun(seed, 2_000, Chaos.Profile.brutal(), RaftConfig.defaults().withSnapshotEvery(1));
        }
    }
}
