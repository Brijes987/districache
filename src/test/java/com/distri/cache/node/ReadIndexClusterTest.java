package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.raft.ReplicatedCache;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real servers and sockets with --read-mode read-index. */
class ReadIndexClusterTest {

    private static TestCluster cluster() throws Exception {
        TestCluster c = new TestCluster(3, null, null, true);
        c.readMode = ReplicatedCache.ReadMode.READ_INDEX;
        return c;
    }

    @Test
    void readsSeeTheClientsOwnWritesAndAddNothingToTheLog() throws Exception {
        try (TestCluster cluster = cluster()) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 50; i++) {
                    client.set("k" + i, "v" + i);
                    assertEquals(Optional.of("v" + i), client.get("k" + i), "read-your-write k" + i);
                }
                assertEquals(Optional.empty(), client.get("never-set"));
                NodeRuntime rt = cluster.servers[leader].runtimeForMetrics();
                long logBefore = rt.lastLogIndex();
                for (int i = 0; i < 200; i++) {
                    assertEquals(Optional.of("v" + (i % 50)), client.get("k" + (i % 50)));
                }
                Thread.sleep(100); // the runtime publishes its counters after each step
                assertEquals(logBefore, rt.lastLogIndex(), "reads must not append to the log");
                assertTrue(rt.readsServed() >= 250, "reads served by read-index: " + rt.readsServed());
                assertEquals(0, rt.pendingReads());
            }
        }
    }

    @Test
    void readsAndWritesSurviveKillingTheLeaderMidLoad() throws Exception {
        try (TestCluster cluster = cluster()) {
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            AtomicInteger failures = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int c = 0; c < 8; c++) {
                int id = c;
                Thread t = new Thread(() -> {
                    try (ReplicatedClient client = cluster.client(20_000)) {
                        for (int i = 0; i < 40; i++) {
                            String key = "c" + id + "-" + i;
                            client.set(key, "v" + i);
                            // an acknowledged write must be visible to the next read, whichever server is leader
                            Optional<String> seen = client.get(key);
                            if (!seen.equals(Optional.of("v" + i))) {
                                failures.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                t.start();
                threads.add(t);
            }
            Thread.sleep(300);
            cluster.kill(leader);
            for (Thread t : threads) {
                t.join(90_000);
            }
            assertEquals(0, failures.get(), "no read may miss an acknowledged write, no client may fail");
        }
    }
}
