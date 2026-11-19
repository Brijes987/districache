package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.metrics.MetricsRegistry;
import static com.distri.cache.metrics.MetricsText.value;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Real sockets, real servers, pre-vote switched on for every node. */
class PreVoteClusterTest {

    @Test
    void aClusterWithPreVoteElectsServesAndSurvivesKillingTheLeader() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(20_000)) {
            cluster.preVote = true;
            cluster.startAll();
            int oldLeader = cluster.awaitLeader(5_000);
            for (int i = 0; i < 30; i++) {
                client.set("key" + i, "v" + i);
            }
            cluster.kill(oldLeader);
            for (int i = 30; i < 60; i++) {
                client.set("key" + i, "v" + i);
            }
            int newLeader = cluster.awaitLeader(5_000);
            assertTrue(newLeader != oldLeader);
            for (int i = 0; i < 60; i++) {
                assertEquals(Optional.of("v" + i), client.get("key" + i), "key" + i);
            }
        }
    }

    @Test
    void thePreVoteMetricsCountTheRoundsSurvivorsStartAfterTheLeaderDies() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.preVote = true;
            MetricsRegistry[] registries = new MetricsRegistry[3];
            for (int i = 0; i < 3; i++) {
                registries[i] = new MetricsRegistry();
                ReplicatedServer s = new ReplicatedServer(cluster.config(i)).enableMetrics(registries[i]);
                s.start();
                cluster.servers[i] = s;
            }
            int oldLeader = cluster.awaitLeader(5_000);
            double before = 0;
            for (int i = 0; i < 3; i++) {
                if (i != oldLeader) {
                    before += value(registries[i].render(), "districache_raft_prevote_rounds");
                }
            }
            cluster.kill(oldLeader);
            int newLeader = cluster.awaitLeader(5_000);
            assertTrue(newLeader != oldLeader);
            double after = 0;
            for (int i = 0; i < 3; i++) {
                if (i != oldLeader) {
                    after += value(registries[i].render(), "districache_raft_prevote_rounds");
                    assertTrue(value(registries[i].render(), "districache_raft_prevotes_denied") >= 0);
                }
            }
            assertTrue(after > before, "survivors started no pre-vote round after the leader died: " + before + " -> " + after);
        }
    }
}
