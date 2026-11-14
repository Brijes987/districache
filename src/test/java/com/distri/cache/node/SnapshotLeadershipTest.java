package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Does a long snapshot on the leader cost it its leadership? A snapshot is taken on the Raft node's own thread, the same
 * thread that sends heartbeats, so while it runs the leader sends none. If that is longer than a follower's election timeout
 * (100 to 200 ms in this cluster: 20 to 40 ticks of 5 ms) the followers conclude the leader is dead and elect another.
 *
 * <p>The long snapshot is simulated with a delay inside {@code snapshot()} (a test hook) so the result does not depend on how
 * fast the machine copies memory; docs/GC_PAUSES.md has real copy times. The delay is applied to the leader only.
 *
 * <p>These tests describe how it behaves TODAY. They are meant to be inverted when the snapshot copy moves off the Raft thread.
 */
class SnapshotLeadershipTest {

    private static final long LONG_SNAPSHOT_MILLIS = 800; // 4 to 8 times the election timeout of 100 to 200 ms

    /** Writes enough entries to make every server take a snapshot (every 10 entries); returns the slowest single write in ms. */
    private static long writeUntilSnapshot(TestCluster cluster) throws Exception {
        long slowest = 0;
        try (ReplicatedClient client = cluster.client(30_000)) {
            for (int i = 0; i < 25; i++) {
                long t0 = System.nanoTime();
                client.set("key" + i, "value" + i);
                slowest = Math.max(slowest, (System.nanoTime() - t0) / 1_000_000);
            }
        }
        return slowest;
    }

    @Test
    void withNormalSnapshotsTheLeaderStaysLeader() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.snapshotEvery = 10;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            long term = cluster.servers[leader].term();
            writeUntilSnapshot(cluster);
            Thread.sleep(1_000);
            assertEquals(leader, cluster.leader(), "no election without the delay");
            assertEquals(term, cluster.servers[leader].term());
        }
    }

    @Test
    void aSnapshotShorterThanTheElectionTimeoutDoesNotCostLeadership() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.snapshotEvery = 10;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            long term = cluster.servers[leader].term();
            cluster.servers[leader].setSnapshotDelayMillisForTesting(40); // under the 100 ms minimum election timeout
            writeUntilSnapshot(cluster);
            Thread.sleep(1_000);
            assertEquals(leader, cluster.leader());
            assertEquals(term, cluster.servers[leader].term());
        }
    }

    @Test
    void todayASnapshotLongerThanTheElectionTimeoutCostsTheLeaderItsLeadership() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.snapshotEvery = 10;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            long term = cluster.servers[leader].term();
            cluster.servers[leader].setSnapshotDelayMillisForTesting(LONG_SNAPSHOT_MILLIS);
            long slowestWrite = writeUntilSnapshot(cluster);
            TestCluster.await(() -> cluster.servers[leader].term() > term, 10_000, "the old leader to see a newer term");
            TestCluster.await(() -> cluster.leader() >= 0 && cluster.leader() != leader, 10_000, "another server to lead");
            assertTrue(cluster.servers[cluster.leader()].term() > term, "the new leader has a higher term");
            System.out.println("SnapshotLeadershipTest: leader " + leader + " term " + term + " -> leader " + cluster.leader()
                    + " term " + cluster.servers[cluster.leader()].term() + "; slowest client write " + slowestWrite
                    + " ms (snapshot delay " + LONG_SNAPSHOT_MILLIS + " ms, election timeout 100 to 200 ms)");
        }
    }
}
