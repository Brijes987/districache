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
 * <p>Before the snapshot work moved off the Raft thread, the 800 ms case below ended in an election (term 1 to 2 in 3 of 3 runs
 * on one machine, 3 to 5 on another); it now does not. Only the cheap freezing of the state still runs on that thread.
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
    void aSnapshotLongerThanTheElectionTimeoutNoLongerCostsTheLeaderItsLeadership() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.snapshotEvery = 10;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            long term = cluster.servers[leader].term();
            cluster.servers[leader].setSnapshotDelayMillisForTesting(LONG_SNAPSHOT_MILLIS);
            long slowestWrite = writeUntilSnapshot(cluster);
            // the snapshot really was taken (in the background, taking about the delay), and nobody noticed
            TestCluster.await(() -> cluster.servers[leader].runtimeForMetrics().snapshotsTaken() >= 1, 15_000,
                    "the slow snapshot to finish");
            Thread.sleep(500);
            assertEquals(leader, cluster.leader(), "the leader kept its leadership");
            assertEquals(term, cluster.servers[leader].term(), "no election took place");
            assertTrue(slowestWrite < LONG_SNAPSHOT_MILLIS,
                    "client writes were not held up by the snapshot: slowest " + slowestWrite + " ms");
            System.out.println("SnapshotLeadershipTest: leader " + leader + " kept term " + term + "; slowest client write "
                    + slowestWrite + " ms (snapshot delay " + LONG_SNAPSHOT_MILLIS + " ms, election timeout 100 to 200 ms)");
        }
    }
}
