package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.metrics.MetricsRegistry;
import static com.distri.cache.metrics.MetricsText.value;
import com.distri.cache.raft.RaftMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Leadership transfer on real servers: the TimeoutNow message on the wire, the admin request, and a handover under load. */
class LeadershipTransferClusterTest {

    @Test
    void timeoutNowRoundTripsAndMalformedOnesAreRejected() throws IOException {
        RaftMessage m = new RaftMessage.TimeoutNow(12);
        assertEquals(m, RaftMessageCodec.decode(RaftMessageCodec.encode(m)));
        assertEquals(9, RaftMessageCodec.encode(m)[0]);
        byte[] good = RaftMessageCodec.encode(m);
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(java.util.Arrays.copyOf(good, good.length - 1)));
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(java.util.Arrays.copyOf(good, good.length + 1)));
        byte[] zeroTerm = good.clone();
        for (int i = 1; i <= 8; i++) {
            zeroTerm[i] = 0;
        }
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(zeroTerm));
    }

    private void handOverOnce(boolean preVote) throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(20_000)) {
            cluster.preVote = preVote;
            cluster.startAll();
            int oldLeader = cluster.awaitLeader(5_000);
            for (int i = 0; i < 20; i++) {
                client.set("key" + i, "v" + i);
            }
            long termBefore = cluster.servers[oldLeader].term();
            int target = (oldLeader + 1) % 3;
            long t0 = System.nanoTime();
            assertTrue(client.transferLeadership(target), "the leader should have started a transfer");
            TestCluster.await(() -> cluster.leader() == target, 5_000, "server " + target + " to become leader");
            long millis = (System.nanoTime() - t0) / 1_000_000;
            System.out.println("handover (preVote=" + preVote + ") took " + millis + " ms, term " + termBefore + " -> "
                    + cluster.servers[target].term());
            assertEquals(termBefore + 1, cluster.servers[target].term(), "exactly one election");
            TestCluster.await(() -> !cluster.servers[oldLeader].isLeader(), 5_000, "the old leader to step down");
            for (int i = 20; i < 40; i++) {
                client.set("key" + i, "v" + i);
            }
            for (int i = 0; i < 40; i++) {
                assertEquals(Optional.of("v" + i), client.get("key" + i), "key" + i);
            }
            assertEquals(termBefore + 1, cluster.servers[target].term(), "no election after the handover");
        }
    }

    @Test
    void anAdminRequestMovesLeadershipToTheChosenServer() throws Exception {
        handOverOnce(false);
    }

    @Test
    void theHandoverAlsoWorksWithPreVoteOn() throws Exception {
        handOverOnce(true);
    }

    @Test
    void badTargetsAreRefusedAndTransferToTheLeaderItselfDoesNothing() throws Exception {
        try (TestCluster cluster = new TestCluster(3); ReplicatedClient client = cluster.client(20_000)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            long term = cluster.servers[leader].term();
            long t0 = System.nanoTime();
            ReplicatedClientException e = assertThrows(ReplicatedClientException.class, () -> client.transferLeadership(42));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 3_000, "a refusal is an answer: it must not be retried until the deadline");
            assertFalse(e.outcomeUnknown(), "the server said no, so nothing happened");
            assertTrue(e.getMessage().contains("not a member"), e.getMessage());
            t0 = System.nanoTime();
            assertFalse(client.transferLeadership(leader), "already the leader: nothing to do");
            System.out.println("self transfer answered after " + (System.nanoTime() - t0) / 1_000_000 + " ms");
            Thread.sleep(300);
            assertEquals(leader, cluster.leader());
            assertEquals(term, cluster.servers[leader].term(), "nothing may have happened");
        }
    }

    @Test
    void handoversUnderLoadLoseNoAcknowledgedWriteAndAnswerClientsThroughout() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger acknowledged = new AtomicInteger();
            AtomicInteger failed = new AtomicInteger();
            List<String> acked = java.util.Collections.synchronizedList(new ArrayList<>());
            Thread writer = new Thread(() -> {
                try (ReplicatedClient c = cluster.client(20_000)) {
                    int i = 0;
                    while (!stop.get()) {
                        String key = "w" + i++;
                        try {
                            c.set(key, "v");
                            acked.add(key);
                            acknowledged.incrementAndGet();
                        } catch (ReplicatedClientException e) {
                            failed.incrementAndGet();
                        }
                    }
                }
            }, "writer");
            writer.start();
            TestCluster.await(() -> acknowledged.get() >= 50, 20_000, "the writer to get going");
            List<Long> millis = new ArrayList<>();
            try (ReplicatedClient admin = cluster.client(20_000)) {
                for (int round = 0; round < 6; round++) {
                    int leader = cluster.awaitLeader(5_000);
                    int target = (leader + 1 + round % 2) % 3;
                    long t0 = System.nanoTime();
                    assertTrue(admin.transferLeadership(target));
                    TestCluster.await(() -> cluster.leader() == target, 5_000, "server " + target + " to become leader");
                    millis.add((System.nanoTime() - t0) / 1_000_000);
                    int before = acknowledged.get();
                    TestCluster.await(() -> acknowledged.get() >= before + 20, 20_000, "writes to continue after the handover");
                }
            }
            stop.set(true);
            writer.join(30_000);
            System.out.println("6 handovers under load, ms until the target led: " + millis + "; writes acknowledged "
                    + acknowledged.get() + ", failed " + failed.get());
            assertEquals(0, failed.get(), "a handover must not make the client give up");
            try (ReplicatedClient reader = cluster.client(20_000)) {
                for (String key : acked) {
                    assertEquals(Optional.of("v"), reader.get(key), key);
                }
            }
        }
    }

    @Test
    void theTransferCounterRisesOnTheLeaderThatStartedIt() throws Exception {
        try (TestCluster cluster = new TestCluster(3)) {
            MetricsRegistry[] registries = new MetricsRegistry[3];
            for (int i = 0; i < 3; i++) {
                registries[i] = new MetricsRegistry();
                ReplicatedServer s = new ReplicatedServer(cluster.config(i)).enableMetrics(registries[i]);
                s.start();
                cluster.servers[i] = s;
            }
            int leader = cluster.awaitLeader(5_000);
            assertEquals(0.0, value(registries[leader].render(), "districache_raft_leadership_transfers"));
            int target = (leader + 1) % 3;
            try (ReplicatedClient client = cluster.client(20_000)) {
                assertTrue(client.transferLeadership(target));
            }
            TestCluster.await(() -> cluster.leader() == target, 5_000, "the handover");
            TestCluster.await(() -> value(registries[leader].render(), "districache_raft_leadership_transfers") == 1,
                    2_000, "the counter to show the transfer");
            assertEquals(0.0, value(registries[target].render(), "districache_raft_leadership_transfers"));
        }
    }
}
