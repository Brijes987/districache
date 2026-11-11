package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real servers, real sockets, real fsync: concurrent clients must share log writes, and nothing may be lost. */
class GroupCommitClusterTest {

    private static final int CLIENTS = 24;
    private static final int WRITES = 40;

    private static void hammer(TestCluster cluster) throws Exception {
        AtomicInteger failures = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            int id = c;
            Thread t = new Thread(() -> {
                try (ReplicatedClient client = cluster.client(20_000)) {
                    for (int i = 0; i < WRITES; i++) {
                        client.set("c" + id + "-" + i, "v" + id + "-" + i);
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            });
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            t.join(60_000);
        }
        assertEquals(0, failures.get(), "every client must finish its writes");
    }

    private static void assertAllThere(TestCluster cluster) throws Exception {
        try (ReplicatedClient client = cluster.client(20_000)) {
            for (int c = 0; c < CLIENTS; c++) {
                for (int i = 0; i < WRITES; i++) {
                    assertEquals(Optional.of("v" + c + "-" + i), client.get("c" + c + "-" + i), "lost c" + c + "-" + i);
                }
            }
        }
    }

    @Test
    void concurrentClientsShareLogWritesAndEveryAcknowledgedWriteIsThere() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            hammer(cluster);
            assertAllThere(cluster);
            NodeRuntime rt = cluster.servers[leader].runtimeForMetrics();
            long batches = rt.proposalBatches();
            long entries = rt.proposedEntries();
            assertTrue(entries >= (long) CLIENTS * WRITES, "entries went through batches: " + entries);
            assertTrue(entries > batches, "with " + CLIENTS + " concurrent clients and real fsync the mean batch must"
                    + " exceed 1, got " + entries + " entries in " + batches + " batches");
        }
    }

    @Test
    void withMaxBatchOneEveryRequestIsProposedAloneAsBefore() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.maxBatch = 1;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            hammer(cluster);
            assertAllThere(cluster);
            assertEquals(0, cluster.servers[leader].runtimeForMetrics().proposalBatches(), "batching is off");
        }
    }

    @Test
    void aReplicatedGroupWithBatchingSurvivesKillingTheLeaderMidLoad() throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, true)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            Thread killer = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    cluster.kill(leader);
                } catch (InterruptedException ignored) {
                    // done
                }
            });
            killer.start();
            hammer(cluster); // clients retry across the failover; exactly-once sessions keep the writes single
            killer.join();
            assertAllThere(cluster);
        }
    }
}
