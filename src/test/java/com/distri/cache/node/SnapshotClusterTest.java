package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Real servers with real files: the log stays small, and servers come back or catch up through snapshots. */
class SnapshotClusterTest {

    private static Map<String, String> expected(int count) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < count; i++) {
            m.put("key" + i, "value" + i);
        }
        return m;
    }

    private static boolean holds(TestCluster cluster, int server, Map<String, String> want) {
        try {
            return want.equals(cluster.servers[server].localContents());
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Test
    void theLogOnEveryServerStaysBoundedWhileTheDataKeepsGrowing() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false)) {
            cluster.snapshotEvery = 25;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 400; i++) {
                    client.set("key" + i, "value" + i);
                }
            }
            TestCluster.await(() -> {
                for (int i = 0; i < 3; i++) {
                    if (!holds(cluster, i, expected(400))) {
                        return false;
                    }
                }
                return true;
            }, 15_000, "all servers to hold 400 entries");
            for (int i = 0; i < 3; i++) {
                NodeRuntime rt = cluster.servers[i].runtimeForMetrics();
                long inLog = rt.lastLogIndex() - rt.snapshotIndex();
                assertTrue(rt.snapshotsTaken() + rt.snapshotsInstalled() >= 5, "server " + i + " took snapshots");
                assertTrue(inLog <= 25 + 5, "server " + i + " holds " + inLog + " entries in its log");
                Path dir = cluster.root.resolve("node" + i);
                assertTrue(Files.exists(dir.resolve("raft.snapshot")));
                assertTrue(Files.size(dir.resolve("raft.log")) < 4_000,
                        "raft.log of server " + i + " is " + Files.size(dir.resolve("raft.log")) + " bytes");
            }
        }
    }

    @Test
    void aServerRestartedFromItsSnapshotAndLogTailHoldsEverythingAgain() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.snapshotEvery = 20;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int victim = (leader + 1) % 3;
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 130; i++) {
                    client.set("key" + i, "value" + i);
                }
            }
            TestCluster.await(() -> holds(cluster, victim, expected(130)), 15_000, "the victim to hold 130 entries");
            assertTrue(cluster.servers[victim].runtimeForMetrics().snapshotIndex() > 0);
            cluster.kill(victim);
            cluster.start(victim);
            TestCluster.await(() -> holds(cluster, victim, expected(130)), 15_000, "the restarted server to hold all");
            assertTrue(cluster.servers[victim].runtimeForMetrics().snapshotIndex() > 0, "it restarted from a snapshot");
        }
    }

    @Test
    void aServerThatWasDownWhileTheLogWasCompactedIsCaughtUpByASnapshot() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false)) {
            cluster.snapshotEvery = 15;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int victim = (leader + 1) % 3;
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 10; i++) {
                    client.set("key" + i, "value" + i);
                }
                cluster.kill(victim);
                for (int i = 10; i < 200; i++) { // far more than the 15 between snapshots
                    client.set("key" + i, "value" + i);
                }
                cluster.start(victim);
                TestCluster.await(() -> holds(cluster, victim, expected(200)), 20_000,
                        "the restarted server to catch up");
                assertTrue(cluster.servers[victim].runtimeForMetrics().snapshotsInstalled() >= 1,
                        "the entries it needed were gone from the leader's log, so a snapshot must have been sent");
                client.set("after", "x");
                assertEquals(Optional.of("x"), client.get("after"));
                assertEquals(Optional.of("value3"), client.get("key3"));
            }
        }
    }

    @Test
    void theWholeGroupRestartsFromItsSnapshotsWithNothingLostAndKeepsServing() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.snapshotEvery = 10;
            cluster.startAll();
            cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 90; i++) {
                    client.set("key" + i, "value" + i);
                }
            }
            for (int i = 0; i < 3; i++) {
                cluster.kill(i);
            }
            cluster.startAll();
            cluster.awaitLeader(10_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 90; i += 7) {
                    assertEquals(Optional.of("value" + i), client.get("key" + i));
                }
                client.set("fresh", "1");
                assertEquals(Optional.of("1"), client.get("fresh"));
            }
        }
    }

    @Test
    void aGroupRestartedAfterManyOverwritesOfOneKeyStillReturnsTheLatestValue() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false)) {
            cluster.snapshotEvery = 5;
            cluster.startAll();
            cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 40; i++) {
                    client.set("counter-key", "v" + i);
                }
                assertEquals(Optional.of("v39"), client.get("counter-key"));
            }
            for (int i = 0; i < 3; i++) {
                cluster.kill(i);
            }
            cluster.startAll();
            cluster.awaitLeader(10_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                assertEquals(Optional.of("v39"), client.get("counter-key"));
            }
        }
    }
}
