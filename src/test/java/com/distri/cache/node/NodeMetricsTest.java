package com.distri.cache.node;

import static com.distri.cache.metrics.MetricsText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.metrics.MetricsRegistry;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Raft metrics from a real three-server group: what the gauges say must match what the group actually did. */
class NodeMetricsTest {

    private static MetricsRegistry[] startAll(TestCluster cluster) throws Exception {
        MetricsRegistry[] registries = new MetricsRegistry[cluster.servers.length];
        for (int i = 0; i < registries.length; i++) {
            registries[i] = new MetricsRegistry();
            ReplicatedServer s = new ReplicatedServer(cluster.config(i)).enableMetrics(registries[i]);
            s.start();
            cluster.servers[i] = s;
        }
        return registries;
    }

    private static double v(MetricsRegistry r, String series) {
        return value(r.render(), series);
    }

    @Test
    void exactlyOneServerReportsLeaderAndTheNumbersMatchTheWorkDone() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(10_000)) {
            MetricsRegistry[] registries = startAll(cluster);
            int leader = cluster.awaitLeader(5_000);
            for (int i = 0; i < 20; i++) {
                client.set("k" + i, "v" + i);
            }
            assertEquals(Optional.of("v3"), client.get("k3"));
            assertEquals(Optional.empty(), client.get("absent"));

            int leaders = 0;
            for (MetricsRegistry r : registries) {
                leaders += (int) v(r, "districache_raft_is_leader");
            }
            assertEquals(1, leaders);
            MetricsRegistry lead = registries[leader];
            assertEquals(1, v(lead, "districache_raft_role{role=\"leader\"}"), 0);
            assertEquals(0, v(lead, "districache_raft_role{role=\"follower\"}"), 0);
            assertEquals(leader, v(lead, "districache_raft_node_id"), 0);
            assertEquals(leader, v(lead, "districache_raft_leader_id"), 0);
            assertTrue(v(lead, "districache_raft_term") >= 1);
            assertEquals(1, v(lead, "districache_raft_times_became_leader"), 0);

            // 20 sets + 2 gets, each through the log, plus the leader's no-op entry
            TestCluster.await(() -> v(lead, "districache_raft_commit_index") >= 23, 5_000, "commit index");
            String text = lead.render();
            assertEquals(20, value(text, "districache_raft_client_requests_total{op=\"set\",outcome=\"ok\"}"), 0);
            assertEquals(2, value(text, "districache_raft_client_requests_total{op=\"get\",outcome=\"ok\"}"), 0);
            assertEquals(1, value(text, "districache_cache_hits_total"), 0);
            assertEquals(1, value(text, "districache_cache_misses_total"), 0);
            assertEquals(0.5, value(text, "districache_cache_hit_ratio"), 1e-9);
            assertEquals(22, value(text, "districache_raft_client_request_duration_seconds_count"), 0);
            assertTrue(value(text, "districache_raft_client_request_duration_estimated_seconds{quantile=\"0.99\"}") > 0);

            // once idle, nothing is waiting: the commit and apply lags drain to zero on every server
            for (MetricsRegistry r : registries) {
                TestCluster.await(() -> v(r, "districache_raft_commit_lag_entries") == 0
                        && v(r, "districache_raft_apply_lag_entries") == 0
                        && v(r, "districache_raft_commit_index") == v(lead, "districache_raft_commit_index"),
                        5_000, "followers catch up");
            }
        }
    }

    @Test
    void killingTheLeaderShowsUpAsANewLeaderAndAHigherTerm() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(10_000)) {
            MetricsRegistry[] registries = startAll(cluster);
            int first = cluster.awaitLeader(5_000);
            client.set("before", "1");
            double termBefore = v(registries[first], "districache_raft_term");

            cluster.kill(first);
            int second = -1;
            for (int i = 0; i < 100 && second < 0; i++) {
                Thread.sleep(50);
                for (int s = 0; s < registries.length; s++) {
                    if (s != first && cluster.isUp(s) && v(registries[s], "districache_raft_is_leader") == 1) {
                        second = s;
                    }
                }
            }
            assertTrue(second >= 0 && second != first, "a new leader appears");
            assertTrue(v(registries[second], "districache_raft_term") > termBefore);
            assertEquals(1, v(registries[second], "districache_raft_times_became_leader"), 0);
            assertTrue(v(registries[second], "districache_raft_leader_changes") >= 1);
            client.set("after", "2");
            assertEquals(Optional.of("1"), client.get("before"));
            // the request counter on the new leader counts only what it handled itself
            assertTrue(v(registries[second], "districache_raft_client_requests_total{op=\"set\",outcome=\"ok\"}") >= 1);
        }
    }

    @Test
    void aFollowerCountsTheNotLeaderAnswersItGives() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            MetricsRegistry[] registries = startAll(cluster);
            int leader = cluster.awaitLeader(5_000);
            int follower = (leader + 1) % 3;
            TestCluster.await(() -> cluster.servers[follower].commitIndex() >= 1, 5_000, "the follower to learn the leader");
            try (TestCluster.Raw raw = new TestCluster.Raw(cluster.members.get(follower).clientPort(), null)) {
                for (int i = 0; i < 3; i++) {
                    assertEquals(ClientProtocol.NOT_LEADER,
                            raw.call(new ClientProtocol.Request(ClientProtocol.SET, 1, i + 1, "k", "v", -1)).status());
                }
            }
            assertTrue(v(registries[follower], "districache_raft_client_requests_total{op=\"set\",outcome=\"not_leader\"}") == 3);
            assertEquals(0, v(registries[follower], "districache_raft_is_leader"), 0);
        }
    }

    /** The commit lag must be positive while an entry is in the log but cannot be committed (and not just zero when idle). */
    @Test
    void aLeaderCutOffFromItsFollowersShowsUncommittedEntriesAsCommitLag() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            MetricsRegistry[] registries = startAll(cluster);
            int leader = cluster.awaitLeader(5_000);
            TestCluster.await(() -> v(registries[leader], "districache_raft_commit_index") >= 1, 5_000, "the no-op to commit");
            assertEquals(0, v(registries[leader], "districache_raft_commit_lag_entries"), 0);

            java.util.Set<Integer> others = new java.util.HashSet<>(java.util.List.of(0, 1, 2));
            others.remove(leader);
            cluster.servers[leader].blockPeers(others); // the leader can no longer reach a majority
            Thread writer = new Thread(() -> {
                try (TestCluster.Raw raw = new TestCluster.Raw(cluster.members.get(leader).clientPort(), null)) {
                    raw.call(new ClientProtocol.Request(ClientProtocol.SET, 7, 1, "stuck", "v", -1));
                } catch (Exception ignored) {
                    // the connection is closed when the cluster shuts down
                }
            });
            writer.setDaemon(true);
            writer.start();
            TestCluster.await(() -> v(registries[leader], "districache_raft_commit_lag_entries") >= 1
                    || v(registries[leader], "districache_raft_last_log_index") > v(registries[leader], "districache_raft_commit_index"),
                    5_000, "an uncommitted entry");
            double lag = v(registries[leader], "districache_raft_commit_lag_entries");
            assertTrue(lag >= 1, "commit lag should count the stuck entry, was " + lag);
            assertEquals(v(registries[leader], "districache_raft_last_log_index") - v(registries[leader], "districache_raft_commit_index"),
                    v(registries[leader], "districache_raft_commit_lag_entries"), 0);
        }
    }

    @Test
    void metricsMustBeEnabledBeforeStart() throws Exception {
        try (TestCluster cluster = new TestCluster(1)) {
            ReplicatedServer s = new ReplicatedServer(cluster.config(0));
            s.start();
            cluster.servers[0] = s;
            assertThrows(IllegalStateException.class, () -> s.enableMetrics(new MetricsRegistry()));
        }
    }
}
