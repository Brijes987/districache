package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.Operation;
import com.distri.cache.raft.RaftConfig;
import com.distri.cache.raft.ReplicatedCache.ReadMode;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Real servers, real TCP, real files, and network partitions made by cutting the peer links (the clients' ports keep
 * working, as they would when only the servers' network is split). Several clients run at once and every call and
 * answer is recorded with real timestamps; the checker then decides whether the recorded history could have come from
 * one correct key-value map.
 *
 * <p>One client can only talk to the server that was leader when the partition began, so it is the one that would
 * observe a stale read or a wrongly acknowledged write if the cut-off leader misbehaved. One test switches a known
 * mistake on to show that this test does fail when it should.
 */
class RealPartitionLinearizabilityTest {

    private static final int NODES = 5;

    private static long micros() {
        return System.nanoTime() / 1_000;
    }

    private enum Role { READER, WRITER, MIXED }

    /** What one run is configured with. */
    private record Setup(ReadMode readMode, boolean preVote, boolean checkQuorum, Set<RaftConfig.Fault> faults) {
    }

    /** What the run produced. */
    private record Outcome(LinearizabilityChecker.Result result, List<Operation> history, int writesOnMajoritySide,
            int readsAnsweredByTheCutOffLeader) {
    }

    /** How the network is disturbed, once the clients are running. */
    private interface Disturbance {
        void run(TestCluster cluster, int leader, Set<Integer> minority) throws Exception;
    }

    private static void cut(TestCluster cluster, Set<Integer> group) {
        Set<Integer> rest = new HashSet<>();
        for (int i = 0; i < NODES; i++) {
            if (!group.contains(i)) {
                rest.add(i);
            }
        }
        for (int i : group) {
            cluster.servers[i].blockPeers(rest);
        }
        for (int i : rest) {
            cluster.servers[i].blockPeers(group);
        }
    }

    private static void heal(TestCluster cluster) {
        for (int i = 0; i < NODES; i++) {
            cluster.servers[i].blockPeers(Set.of());
        }
    }

    private static Thread startClient(String name, int id, List<InetSocketAddress> servers, long timeoutMillis, Role role,
            AtomicBoolean stop, List<Operation> history, long seed) {
        Thread t = new Thread(() -> {
            Random random = new Random(seed);
            try (ReplicatedClient client = new ReplicatedClient(servers, null, timeoutMillis, null)) {
                int n = 0;
                while (!stop.get()) {
                    String key = random.nextBoolean() ? "x" : "y";
                    int pick = switch (role) {
                        case READER -> 4 + random.nextInt(4);
                        case WRITER -> random.nextInt(4);
                        case MIXED -> random.nextInt(10);
                    };
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
                        Thread.sleep(8);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (RuntimeException e) {
                        history.add(Operation.pendingSet(id, "error", e.toString(), invoke)); // never expected
                    }
                }
            }
        }, name);
        t.start();
        return t;
    }

    /**
     * Five servers. Clients: 1 only talks to the server that is leader when the partition starts (the cut-off one),
     * 2 writes and 3 reads through the servers on the majority side, 4 does a mix through all servers.
     */
    private Outcome run(Setup setup, java.util.function.IntFunction<Set<Integer>> minorityOf, Disturbance disturbance)
            throws Exception {
        List<Operation> history = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean stop = new AtomicBoolean();
        try (TestCluster cluster = new TestCluster(NODES, null, null, false, setup.checkQuorum())) {
            cluster.readMode = setup.readMode();
            cluster.preVote = setup.preVote();
            cluster.faults = setup.faults();
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            Set<Integer> minority = minorityOf.apply(leader);
            assertTrue(minority.contains(leader), "the leader must be on the minority side");
            assertTrue(minority.size() * 2 < NODES, "the cut-off side must be a minority");

            List<InetSocketAddress> all = cluster.clientAddresses();
            List<InetSocketAddress> majorityServers = new ArrayList<>();
            for (int i = 0; i < NODES; i++) {
                if (!minority.contains(i)) {
                    majorityServers.add(all.get(i));
                }
            }
            try (ReplicatedClient seed = cluster.client(5_000)) { // recorded like any other write, as client 0
                for (String key : new String[] {"x", "y"}) {
                    long invoke = micros();
                    seed.set(key, "seed");
                    history.add(Operation.set(0, key, "seed", invoke, micros()));
                }
            }

            List<Thread> clients = new ArrayList<>();
            clients.add(startClient("pinned-reader", 1, List.of(all.get(leader)), 500, Role.READER, stop, history, 11));
            clients.add(startClient("majority-writer", 2, majorityServers, 500, Role.WRITER, stop, history, 22));
            clients.add(startClient("majority-reader", 3, majorityServers, 500, Role.READER, stop, history, 33));
            clients.add(startClient("mixed", 4, all, 400, Role.MIXED, stop, history, 44));

            Thread.sleep(400);
            disturbance.run(cluster, leader, minority);
            heal(cluster);
            Thread.sleep(1_500);
            stop.set(true);
            for (Thread t : clients) {
                t.join(30_000);
                assertFalse(t.isAlive(), "a client is stuck");
            }

            cluster.awaitLeader(10_000);
            TestCluster.await(() -> {
                try {
                    Map<String, String> first = cluster.servers[0].localContents();
                    for (int i = 1; i < NODES; i++) {
                        if (!first.equals(cluster.servers[i].localContents())
                                || cluster.servers[0].commitIndex() != cluster.servers[i].commitIndex()) {
                            return false;
                        }
                    }
                    return true;
                } catch (InterruptedException e) {
                    return false;
                }
            }, 15_000, "all servers to hold the same data after the partition healed");
            for (int i = 0; i < NODES; i++) {
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
        int majorityWrites = 0;
        int pinnedReads = 0;
        for (Operation op : ops) {
            if (op.client() == 2 && op.kind() == Operation.Kind.SET && !op.isPending()) {
                majorityWrites++;
            }
            if (op.client() == 1 && op.kind() == Operation.Kind.GET) {
                pinnedReads++;
            }
        }
        return new Outcome(new LinearizabilityChecker().check(ops), ops, majorityWrites, pinnedReads);
    }

    /** The leader alone on one side for 2.5 seconds; the other four elect a new one meanwhile. */
    private static final java.util.function.IntFunction<Set<Integer>> LEADER_ALONE = leader -> Set.of(leader);

    private static final Disturbance ONE_CUT = (cluster, leader, minority) -> {
        long termBefore = cluster.servers[leader].term();
        cut(cluster, minority);
        TestCluster.await(() -> {
            for (int i = 0; i < NODES; i++) {
                if (!minority.contains(i) && cluster.servers[i].isLeader() && cluster.servers[i].term() > termBefore) {
                    return true;
                }
            }
            return false;
        }, 5_000, "the majority side to elect a new leader while the old one is cut off");
        Thread.sleep(2_500);
    };

    private static void assertLinearizable(Outcome o) {
        assertEquals(LinearizabilityChecker.Status.LINEARIZABLE, o.result().status(), o.result().explanation());
        assertTrue(o.writesOnMajoritySide() >= 20, "the majority side acknowledged too few writes: " + o.writesOnMajoritySide());
        assertTrue(o.history().size() >= 200, "too few operations recorded: " + o.history().size());
    }

    @Test
    void aCutOffLeaderThatStillThinksItLeadsServesNoStaleReadsWithReadIndex() throws Exception {
        // without check-quorum the old leader stays leader for the whole partition: only the read-index check protects reads
        assertLinearizable(run(new Setup(ReadMode.READ_INDEX, false, false, Set.of()), LEADER_ALONE, ONE_CUT));
    }

    @Test
    void theSameHoldsWithPreVoteOn() throws Exception {
        assertLinearizable(run(new Setup(ReadMode.READ_INDEX, true, false, Set.of()), LEADER_ALONE, ONE_CUT));
    }

    @Test
    void readsThroughTheLogWithCheckQuorumAndPreVoteAreLinearizableToo() throws Exception {
        assertLinearizable(run(new Setup(ReadMode.LOG, true, true, Set.of()), LEADER_ALONE, ONE_CUT));
    }

    @Test
    void aLeaderCutOffTogetherWithOneFollowerIsStillSafe() throws Exception {
        // two of five on the leader's side: still a minority, but the old leader can reach one follower
        assertLinearizable(run(new Setup(ReadMode.READ_INDEX, true, false, Set.of()),
                leader -> Set.of(leader, (leader + 1) % NODES), ONE_CUT));
    }

    @Test
    void repeatedRandomPartitionsLeaveEveryRecordedHistoryLinearizable() throws Exception {
        for (boolean preVote : new boolean[] {false, true}) {
            Disturbance flapping = (cluster, leader, minority) -> {
                Random random = new Random(preVote ? 31 : 17);
                for (int round = 0; round < 6; round++) {
                    int size = 1 + random.nextInt(2);
                    Set<Integer> group = new HashSet<>();
                    while (group.size() < size) {
                        group.add(random.nextInt(NODES));
                    }
                    cut(cluster, group);
                    Thread.sleep(300 + random.nextInt(500));
                    heal(cluster);
                    Thread.sleep(200 + random.nextInt(300));
                }
            };
            Outcome o = run(new Setup(ReadMode.READ_INDEX, preVote, false, Set.of()), LEADER_ALONE, flapping);
            assertEquals(LinearizabilityChecker.Status.LINEARIZABLE, o.result().status(),
                    "preVote=" + preVote + ": " + o.result().explanation());
            assertTrue(o.history().size() >= 200, "too few operations recorded: " + o.history().size());
        }
    }

    /**
     * Shows the test can fail. With the known mistake READ_SKIP_LEADERSHIP_CHECK the cut-off leader answers reads from
     * its own copy; the other side has meanwhile acknowledged newer writes, so the pinned reader sees an old value.
     */
    @Test
    void aLeaderThatSkipsTheReadIndexCheckIsCaughtServingStaleReads() throws Exception {
        Outcome o = run(new Setup(ReadMode.READ_INDEX, false, false, Set.of(RaftConfig.Fault.READ_SKIP_LEADERSHIP_CHECK)),
                LEADER_ALONE, ONE_CUT);
        assertTrue(o.readsAnsweredByTheCutOffLeader() > 0, "the cut-off leader answered no reads, so nothing was tested");
        System.out.println("RealPartitionLinearizabilityTest (fault switched on): " + o.result().explanation());
        assertTrue(o.result().status() != LinearizabilityChecker.Status.LINEARIZABLE, "the stale reads were not noticed");
    }

    // ------------------------------------------------------------------ one-way cut: what pre-vote is for

    /** The leader keeps sending; one follower stops receiving from it (its own messages still get out). */
    private static long[] oneWayCut(boolean preVote) throws Exception {
        try (TestCluster cluster = new TestCluster(3, null, null, false, true); ReplicatedClient client = cluster.client(5_000)) {
            cluster.preVote = preVote;
            cluster.startAll();
            int leader = cluster.awaitLeader(5_000);
            int deaf = (leader + 1) % 3;
            client.set("a", "1");
            long termBefore = cluster.servers[leader].term();
            cluster.servers[deaf].blockPeers(Set.of(), Set.of(leader)); // cannot hear the leader; can still be heard
            Thread.sleep(2_000);
            for (int i = 0; i < 20; i++) { // the group keeps working through the cut
                client.set("w" + i, "v" + i);
            }
            long maxTerm = 0;
            for (int i = 0; i < 3; i++) {
                maxTerm = Math.max(maxTerm, cluster.servers[i].term());
            }
            boolean sameLeader = cluster.servers[leader].isLeader();
            cluster.servers[deaf].blockPeers(Set.of(), Set.of());
            TestCluster.await(() -> cluster.leader() >= 0, 5_000, "a leader after healing");
            assertEquals(Optional.of("v19"), client.get("w19"));
            return new long[] {termBefore, maxTerm, sameLeader ? 1 : 0};
        }
    }

    @Test
    void withPreVoteAFollowerThatCannotHearTheLeaderDoesNotDisturbIt() throws Exception {
        long[] r = oneWayCut(true);
        assertEquals(r[0], r[1], "the term moved although the leader was healthy");
        assertEquals(1, r[2], "the leader lost its leadership");
    }

    @Test
    void withoutPreVoteTheSameFollowerRaisesTheTerm() throws Exception {
        long[] r = oneWayCut(false);
        assertTrue(r[1] > r[0], "expected a term increase without pre-vote, got " + r[0] + " -> " + r[1]);
    }
}
