package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.Operation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Real servers over real TCP and real files while servers are killed and restarted at random: three clients run
 * concurrently, every call and answer is recorded with real timestamps, and the checker decides whether the recorded
 * history could have come from one correct key-value map.
 */
class RealKillLinearizabilityTest {

    private static long micros() {
        return System.nanoTime() / 1_000;
    }

    @Test
    void historiesRecordedWhileServersAreKilledAndPartitionedAreLinearizable() throws Exception {
        run(true);
    }

    /**
     * Without check-quorum a leader that lost its peers keeps believing it leads until it hears of a higher term, so
     * correctness must come from the log alone. A server that answered reads from its own copy would fail here.
     */
    @Test
    void theSameHoldsWithoutCheckQuorumWhereACutOffLeaderStaysLeaderForTheWholePartition() throws Exception {
        run(false);
    }

    private void run(boolean checkQuorum) throws Exception {
        List<Operation> history = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger kills = new AtomicInteger();
        AtomicInteger partitions = new AtomicInteger();

        try (TestCluster cluster = new TestCluster(3, null, null, false, checkQuorum)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);

            List<Thread> clients = new ArrayList<>();
            for (int c = 1; c <= 3; c++) {
                int id = c;
                Thread t = new Thread(() -> {
                    Random random = new Random(id * 7919L);
                    // client 1 only reads and waits patiently, so it stays on whichever server it was using; the
                    // others give up on a stuck server after 400 ms and move on, so writes continue on the majority side
                    try (ReplicatedClient client = cluster.client(id == 1 ? 15_000 : 400)) {
                        int n = 0;
                        while (!stop.get()) {
                            String key = "k" + random.nextInt(3);
                            int pick = id == 1 ? 4 + random.nextInt(4) : random.nextInt(10); // 4..7 = GET
                            long invoke = micros();
                            try {
                                if (pick < 4) {
                                    String value = "c" + id + "-" + (++n);
                                    try {
                                        client.set(key, value);
                                        history.add(Operation.set(id, key, value, invoke, micros()));
                                    } catch (ReplicatedClientException e) {
                                        if (e.outcomeUnknown()) {
                                            history.add(Operation.pendingSet(id, key, value, invoke));
                                        }
                                    }
                                } else if (pick < 8) {
                                    try {
                                        Optional<String> v = client.get(key);
                                        history.add(Operation.get(id, key, v.isPresent(), v.orElse(null), invoke, micros()));
                                    } catch (ReplicatedClientException e) {
                                        // a read that got no answer constrains nothing
                                    }
                                } else {
                                    try {
                                        boolean found = client.delete(key);
                                        history.add(Operation.delete(id, key, found, invoke, micros()));
                                    } catch (ReplicatedClientException e) {
                                        if (e.outcomeUnknown()) {
                                            history.add(Operation.pendingDelete(id, key, invoke));
                                        }
                                    }
                                }
                            } catch (RuntimeException e) {
                                history.add(Operation.pendingSet(id, "error", e.toString(), invoke)); // never expected
                            }
                        }
                    }
                }, "client-" + c);
                t.start();
                clients.add(t);
            }

            Random chaos = new Random(12345);
            long end = System.nanoTime() + 7_000L * 1_000_000;
            while (System.nanoTime() < end) {
                Thread.sleep(300 + chaos.nextInt(500));
                if (chaos.nextInt(5) < 2) { // a partition: the leader keeps its client port but loses its peers
                    int isolated = cluster.leader();
                    if (isolated >= 0 && cluster.isUp(isolated)) {
                        ReplicatedServer server = cluster.servers[isolated];
                        server.blockPeers(Set.of((isolated + 1) % 3, (isolated + 2) % 3));
                        partitions.incrementAndGet();
                        Thread.sleep(400 + chaos.nextInt(500));
                        server.blockPeers(Set.of());
                    }
                    continue;
                }
                int victim = chaos.nextInt(3) == 0 ? chaos.nextInt(3) : Math.max(0, cluster.leader());
                if (!cluster.isUp(victim)) {
                    continue;
                }
                cluster.kill(victim);
                kills.incrementAndGet();
                int second = -1;
                if (chaos.nextInt(5) == 0) { // sometimes a second server too: no majority for a moment
                    second = (victim + 1) % 3;
                    if (cluster.isUp(second)) {
                        cluster.kill(second);
                        kills.incrementAndGet();
                    }
                }
                Thread.sleep(200 + chaos.nextInt(400));
                cluster.start(victim);
                if (second >= 0 && !cluster.isUp(second)) {
                    cluster.start(second);
                }
            }
            for (int i = 0; i < 3; i++) {
                if (!cluster.isUp(i)) {
                    cluster.start(i);
                }
            }
            stop.set(true);
            for (Thread t : clients) {
                t.join(30_000);
                assertFalse(t.isAlive(), "a client is stuck");
            }

            // after the dust settles every server must hold identical data
            cluster.awaitLeader(10_000);
            TestCluster.await(() -> {
                try {
                    Map<String, String> first = cluster.servers[0].localContents();
                    return first.equals(cluster.servers[1].localContents())
                            && first.equals(cluster.servers[2].localContents())
                            && cluster.servers[0].commitIndex() == cluster.servers[1].commitIndex()
                            && cluster.servers[1].commitIndex() == cluster.servers[2].commitIndex();
                } catch (InterruptedException e) {
                    return false;
                }
            }, 15_000, "all servers to hold the same data");
            for (int i = 0; i < 3; i++) {
                assertFalse(cluster.servers[i].failed(), "server " + i + " stopped itself");
            }
        }

        List<Operation> ops;
        synchronized (history) {
            ops = new ArrayList<>(history);
        }
        for (Operation op : ops) {
            assertFalse(op.key().equals("error"), "a client hit an unexpected error: " + op);
        }
        assertTrue(kills.get() >= 2, "the servers must actually have been killed, got " + kills.get());
        assertTrue(partitions.get() >= 1, "a leader must actually have been partitioned, got " + partitions.get());
        assertTrue(ops.size() >= 100, "too few operations recorded: " + ops.size());
        LinearizabilityChecker.Result result = new LinearizabilityChecker().check(ops);
        assertEquals(LinearizabilityChecker.Status.LINEARIZABLE, result.status(), result.explanation());
    }
}
