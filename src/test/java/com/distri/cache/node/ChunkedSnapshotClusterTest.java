package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Real servers, real sockets, real files: a lagging server is caught up by a snapshot sent in many chunks. */
class ChunkedSnapshotClusterTest {

    private static String value(int i, int size) {
        StringBuilder sb = new StringBuilder(size);
        String seed = "v" + i + "-";
        while (sb.length() < size) {
            sb.append(seed);
        }
        sb.setLength(size);
        return sb.toString();
    }

    private static boolean holds(TestCluster cluster, int server, int count, int size) {
        try {
            var contents = cluster.servers[server].localContents();
            if (contents.size() < count) {
                return false;
            }
            for (int i = 0; i < count; i++) {
                if (!value(i, size).equals(contents.get("key" + i))) {
                    return false;
                }
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Test
    void aLaggingServerIsCaughtUpThroughManySmallChunks() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false)) {
            cluster.snapshotEvery = 15;
            cluster.snapshotChunkBytes = 16 * 1024;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int victim = (leader + 1) % 3;
            try (ReplicatedClient client = cluster.client(30_000)) {
                cluster.kill(victim);
                for (int i = 0; i < 120; i++) { // about 2.4 MB, far more than the 15 entries between snapshots
                    client.set("key" + i, value(i, 20_000));
                }
                cluster.start(victim);
                TestCluster.await(() -> holds(cluster, victim, 120, 20_000), 30_000, "the restarted server to catch up");
                NodeRuntime rt = cluster.servers[victim].runtimeForMetrics();
                TestCluster.await(() -> rt.snapshotsInstalled() >= 1 && rt.snapshotChunksReceived() > rt.snapshotsInstalled(),
                        10_000, "the counters to show several chunks per installed snapshot");
                assertTrue(rt.snapshotChunksReceived() >= 10, "chunks received: " + rt.snapshotChunksReceived());
                client.set("after", "x");
                assertEquals(Optional.of("x"), client.get("after"));
            }
        }
    }

    @Test
    void aSnapshotLargerThanTheOldSingleMessageLimitCanBeTransferred() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false)) {
            cluster.snapshotEvery = 20;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int victim = (leader + 1) % 3;
            int size = 100_000;
            int count = 420; // 42 MB; a snapshot taken within 20 entries of the end is still above the old 32 MiB (33.5 MB) cap
            try (ReplicatedClient client = cluster.client(60_000)) {
                cluster.kill(victim);
                for (int i = 0; i < count; i++) {
                    client.set("key" + i, value(i, size));
                }
                cluster.start(victim);
                TestCluster.await(() -> holds(cluster, victim, count, size), 90_000, "the restarted server to catch up");
                NodeRuntime rt = cluster.servers[victim].runtimeForMetrics();
                TestCluster.await(() -> rt.snapshotsInstalled() >= 1, 10_000, "an install to be counted");
                assertTrue(rt.snapshotChunksReceived() >= 34, // 34 chunks of 1 MiB > 32 MiB
                         "chunks received: " + rt.snapshotChunksReceived());
            }
        }
    }
}
