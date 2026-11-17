package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
