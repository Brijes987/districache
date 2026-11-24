package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.LinearizabilityChecker.Status;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Leadership transfer: a leader hands over to a chosen follower by bringing its log up to date and then telling it to
 * start an election at once (TimeoutNow). Scripted tests drive one node with exact messages; cluster tests run the
 * deterministic simulator; each of three hand-made breakages must make a test fail; and random transfers are mixed
 * into the fault-injection and linearizability runs.
 */
class LeadershipTransferTest {

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage = new InMemoryRaftStorage();
        final List<Sent> sent = new ArrayList<>();
        final RaftNode node;

        Rig(int id, List<Integer> peers, RaftConfig config) {
            node = new RaftNode(id, peers, storage, (to, m) -> sent.add(new Sent(to, m)), (i, c) -> { }, config,
                    new Random(1));
        }

        /** Makes this node a follower of {@code leader} in {@code term}. */
        void followLeader(int leader, long term) {
            node.receive(leader, new RaftMessage.AppendEntries(term, leader, 0, 0, List.of(), 0));
            assertEquals(leader, node.leaderId());
            assertEquals(term, node.term());
            sent.clear();
        }

        long count(Class<? extends RaftMessage> type) {
            return sent.stream().filter(s -> type.isInstance(s.message())).count();
        }
    }

    private static RaftConfig config(boolean preVote, RaftConfig.Fault... faults) {
        return RaftConfig.defaults().withPreVote(preVote).withFaults(faults);
    }

    // ---------------------------------------------------------------- scripted: the follower side

    private boolean followerObeysOnlyItsLeader(RaftConfig config) {
        // a follower of server 1 in term 3
        Rig fromStranger = new Rig(0, List.of(1, 2), config);
        fromStranger.followLeader(1, 3);
        fromStranger.node.receive(2, new RaftMessage.TimeoutNow(3)); // server 2 is not its leader
        boolean ignoredStranger = fromStranger.node.role() == RaftNode.Role.FOLLOWER && fromStranger.node.term() == 3;

        Rig oldTerm = new Rig(0, List.of(1, 2), config);
        oldTerm.followLeader(1, 3);
        oldTerm.node.receive(1, new RaftMessage.TimeoutNow(2)); // right sender, but an old term
        boolean ignoredOldTerm = oldTerm.node.role() == RaftNode.Role.FOLLOWER && oldTerm.node.term() == 3;
        return ignoredStranger && ignoredOldTerm;
    }

    @Test
    void aFollowerIgnoresTimeoutNowFromAnyoneButItsLeaderInItsOwnTerm() {
        assertTrue(followerObeysOnlyItsLeader(config(false)));
        assertTrue(followerObeysOnlyItsLeader(config(true)));
    }

    @Test
    void aTimeoutNowFromTheLeaderStartsARealElectionAtOnceEvenWithPreVoteOn() {
        for (boolean preVote : new boolean[] {false, true}) {
            Rig rig = new Rig(0, List.of(1, 2), config(preVote));
            rig.followLeader(1, 3);
            rig.node.receive(1, new RaftMessage.TimeoutNow(3));
            assertEquals(RaftNode.Role.CANDIDATE, rig.node.role(), "preVote=" + preVote);
            assertEquals(4, rig.node.term());
            assertEquals(2, rig.count(RaftMessage.RequestVote.class));
            assertEquals(0, rig.count(RaftMessage.PreVote.class), "no pre-vote round: the leader asked for this");
            assertEquals(1, rig.node.timeoutNowReceived());
        }
    }

    // ---------------------------------------------------------------- scripted: the leader side

    @Test
    void transferLeadershipSaysWhyItCannotStart() {
        Rig follower = new Rig(0, List.of(1, 2), config(false));
        assertEquals(RaftNode.TransferResult.NOT_LEADER, follower.node.transferLeadership(1));

        SimCluster cluster = new SimCluster(3, 5, RaftConfig.defaults());
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 500));
        int leader = cluster.leader();
        assertEquals(RaftNode.TransferResult.ALREADY_LEADER, cluster.node(leader).transferLeadership(leader));
        assertEquals(RaftNode.TransferResult.NOT_A_PEER, cluster.node(leader).transferLeadership(99));
        assertEquals(-1, cluster.node(leader).transferTarget(), "a refused request must not leave a transfer behind");
    }

    // ---------------------------------------------------------------- cluster: the handover

    private static final class Handover {
        final boolean ok;
        final long steps;
        final long termBefore;
        final long termAfter;
        final long maxTermSeen;

        Handover(boolean ok, long steps, long termBefore, long termAfter, long maxTermSeen) {
            this.ok = ok;
            this.steps = steps;
            this.termBefore = termBefore;
            this.termAfter = termAfter;
            this.maxTermSeen = maxTermSeen;
        }
    }

    private static SimCluster quietCluster(int n, long seed, RaftConfig config) {
        SimCluster cluster = new SimCluster(n, seed, config);
        cluster.minDelay = 1;
        cluster.maxDelay = 1;
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000), "no leader elected");
        cluster.run(30); // let the cluster settle
        return cluster;
    }

    private static void commit(SimCluster cluster, int leader, String command) {
        assertTrue(cluster.propose(leader, command).accepted());
        long index = cluster.node(leader).lastLogIndex();
        assertTrue(cluster.runUntil(() -> cluster.node(leader).commitIndex() >= index, 200), "not committed: " + command);
    }

    /** A healthy 3-server group: hand over to a follower. */
    private Handover healthyHandover(long seed, RaftConfig config) {
        SimCluster cluster = quietCluster(3, seed, config);
        int leader = cluster.leader();
        commit(cluster, leader, "a");
        commit(cluster, leader, "b");
        int target = (leader + 1) % 3;
        long termBefore = cluster.node(leader).term();
        long start = cluster.now;
        assertEquals(RaftNode.TransferResult.STARTED, cluster.transfer(leader, target));
        boolean ok = cluster.runUntil(() -> cluster.node(target).role() == RaftNode.Role.LEADER, 100);
        return new Handover(ok, cluster.now - start, termBefore, cluster.node(target).term(), cluster.maxTermSeen());
    }

    @Test
    void aHealthyHandoverMakesTheTargetLeaderInOneElectionWithinFewTicks() {
        for (long seed = 1; seed <= 20; seed++) {
            for (boolean preVote : new boolean[] {false, true}) {
                Handover h = healthyHandover(seed, config(preVote));
                assertTrue(h.ok, "seed " + seed + ": the target did not become leader");
                assertEquals(h.termBefore + 1, h.termAfter, "seed " + seed + ": exactly one election");
                assertEquals(h.termAfter, h.maxTermSeen, "seed " + seed + ": no other election happened");
                // with 1-tick links: catch-up check, TimeoutNow, RequestVote and the votes take a handful of ticks, far
                // under the 10-tick minimum election timeout
                assertTrue(h.steps < 10, "seed " + seed + ": took " + h.steps + " ticks");
            }
        }
    }

    @Test
    void afterTheHandoverTheOldLeaderFollowsAndTheNewLeaderKeepsEveryCommittedEntry() {
        SimCluster cluster = quietCluster(5, 9, config(true));
        int leader = cluster.leader();
        for (int i = 0; i < 10; i++) {
            commit(cluster, leader, "k" + i);
        }
        int target = (leader + 2) % 5;
        assertEquals(RaftNode.TransferResult.STARTED, cluster.transfer(leader, target));
        assertTrue(cluster.runUntil(() -> cluster.leader() == target, 100));
        cluster.run(30);
        assertEquals(RaftNode.Role.FOLLOWER, cluster.node(leader).role());
        assertEquals(target, cluster.node(leader).leaderId());
        assertEquals(-1, cluster.node(target).transferTarget());
        commit(cluster, target, "after");
        cluster.run(20); // the next heartbeats tell the followers about the commit
        cluster.deepCheck();
        List<String> applied = new ArrayList<>();
        for (SimCluster.Applied a : cluster.applied(leader)) {
            applied.add(a.command());
        }
        assertTrue(applied.containsAll(List.of("k0", "k9", "after")), applied.toString());
    }

    // ---------------------------------------------------------------- cluster: a target that is behind

    /**
     * The target missed some entries (the leader's link to it was cut one way while writes went on). Heal, hand over at
     * once. Correct behaviour: the leader first brings the target level, so the target wins in the very next term. The
     * pre-vote makes the cut-off target harmless meanwhile (it asks, is refused, and does not inflate its term).
     */
    private boolean laggingTargetWinsInOneElection(long seed, RaftConfig config) {
        SimCluster cluster = quietCluster(3, seed, config);
        int leader = cluster.leader();
        int target = (leader + 1) % 3;
        cluster.cutOneWay(leader, target);
        for (int i = 0; i < 5; i++) {
            commit(cluster, leader, "x" + i);
        }
        long termBefore = cluster.node(leader).term();
        cluster.heal();
        assertTrue(cluster.node(target).lastLogIndex() < cluster.node(leader).lastLogIndex(), "the target should be behind");
        assertEquals(RaftNode.TransferResult.STARTED, cluster.transfer(leader, target));
        boolean won = cluster.runUntil(() -> cluster.node(target).role() == RaftNode.Role.LEADER, 40);
        return won && cluster.node(target).term() == termBefore + 1 && cluster.maxTermSeen() == termBefore + 1;
    }

    @Test
    void aTargetThatIsBehindIsBroughtLevelFirstAndThenWinsInOneElection() {
        for (long seed = 1; seed <= 20; seed++) {
            assertTrue(laggingTargetWinsInOneElection(seed, config(true)), "seed " + seed);
        }
    }

    @Test
    void sendingTimeoutNowBeforeTheCatchUpIsCaught() {
        int failed = 0;
        for (long seed = 1; seed <= 20; seed++) {
            if (!laggingTargetWinsInOneElection(seed, config(true, RaftConfig.Fault.TRANSFER_SKIP_CATCHUP))) {
                failed++;
            }
        }
        assertEquals(20, failed, "the breakage should make the handover fail every time");
    }

    // ---------------------------------------------------------------- cluster: writes during a handover

    /**
     * The target cannot be reached, so the handover cannot finish. While it is pending the leader must not take new
     * writes (they would only move the target further behind); after an election timeout it gives up and works again.
     */
    private boolean writesPausedThenResumed(long seed, RaftConfig config) {
        SimCluster cluster = quietCluster(3, seed, config);
        int leader = cluster.leader();
        int target = (leader + 1) % 3;
        cluster.isolate(target);
        commit(cluster, leader, "while-cut"); // the target is now behind
        long term = cluster.node(leader).term();
        assertEquals(RaftNode.TransferResult.STARTED, cluster.transfer(leader, target));
        RaftNode.ProposeResult during = cluster.propose(leader, "during");
        boolean refused = !during.accepted() && during.leaderHint() == -1;
        cluster.run(config.electionTimeoutMaxTicks() + 5);
        boolean gaveUp = cluster.node(leader).transferTarget() == -1 && cluster.node(leader).role() == RaftNode.Role.LEADER
                && cluster.node(leader).term() == term;
        boolean resumed = cluster.propose(leader, "after").accepted();
        return refused && gaveUp && resumed;
    }

    @Test
    void whileAHandoverIsPendingNewWritesAreRefusedAndAfterATimeoutTheLeaderCarriesOn() {
        for (long seed = 1; seed <= 10; seed++) {
            assertTrue(writesPausedThenResumed(seed, config(true)), "seed " + seed);
        }
    }

    @Test
    void acceptingWritesDuringAHandoverIsCaught() {
        for (long seed = 1; seed <= 10; seed++) {
            assertFalse(writesPausedThenResumed(seed, config(true, RaftConfig.Fault.TRANSFER_KEEP_ACCEPTING)), "seed " + seed);
        }
    }

    @Test
    void aTimeoutNowFromANonLeaderIsCaughtWhenTheBreakageIsOn() {
        assertFalse(followerObeysOnlyItsLeader(config(false, RaftConfig.Fault.TIMEOUT_NOW_FROM_ANYONE)));
        assertFalse(followerObeysOnlyItsLeader(config(true, RaftConfig.Fault.TIMEOUT_NOW_FROM_ANYONE)));
    }

    // ---------------------------------------------------------------- random faults with transfers mixed in

    private static void chaosRun(long seed, Chaos.Profile profile, RaftConfig config, double transferRate, int[] transfers) {
        int n = seed % 2 == 0 ? 3 : 5;
        SimCluster cluster = new SimCluster(n, seed, config);
        Chaos chaos = new Chaos(cluster, seed);
        chaos.transferRate = transferRate;
        chaos.run(3_000, profile);
        transfers[0] += cluster.transfersStarted;
        chaos.settleAndCheck(4_000);
    }

    @Test
    void safetyHoldsWhenLeadersAreAskedToHandOverDuringRandomFailures() {
        int[] transfers = {0};
        for (long seed = 1; seed <= 60; seed++) {
            chaosRun(seed, seed % 3 == 0 ? Chaos.Profile.brutal() : Chaos.Profile.normal(),
                    config(seed % 2 == 0), 0.01, transfers);
        }
        assertTrue(transfers[0] >= 200, "too few transfers actually started: " + transfers[0]);
    }

    @Test
    void safetyHoldsWithHandoversSnapshotsAndBrutalFailures() {
        int[] transfers = {0};
        for (long seed = 100; seed < 130; seed++) {
            chaosRun(seed, Chaos.Profile.brutal(), config(seed % 2 == 1).withSnapshotEvery(8), 0.02, transfers);
        }
        assertTrue(transfers[0] >= 100, "too few transfers actually started: " + transfers[0]);
    }

    @Test
    void clientHistoriesStayLinearizableWhileLeadersHandOver() {
        LinearizabilityChecker checker = new LinearizabilityChecker();
        int transfers = 0;
        int ops = 0;
        for (long seed = 1; seed <= 40; seed++) {
            for (Chaos.Profile profile : new Chaos.Profile[] {Chaos.Profile.normal(), Chaos.Profile.brutal()}) {
                ReplicatedCache.ReadMode mode = seed % 2 == 0 ? ReplicatedCache.ReadMode.READ_INDEX : ReplicatedCache.ReadMode.LOG;
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile,
                        config(seed % 3 == 0), null, null, true, seed % 4 < 2, mode, -1, 0.01);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
                transfers += r.transfers;
                ops += r.history.size();
            }
        }
        assertTrue(transfers >= 300, "too few transfers actually started: " + transfers);
        assertTrue(ops > 8_000, "too little checked: " + ops);
    }

    @Test
    void aCommandIsNeverLostWhenAHandoverHappensBetweenItsProposalAndItsCommit() {
        // propose, hand over before the commit is known, and check the entry is still in every log afterwards
        for (long seed = 1; seed <= 20; seed++) {
            SimCluster cluster = quietCluster(3, seed, config(seed % 2 == 0));
            int leader = cluster.leader();
            assertTrue(cluster.propose(leader, "inflight").accepted());
            int target = (leader + 1) % 3;
            cluster.transfer(leader, target);
            assertTrue(cluster.runUntil(() -> cluster.leader() == target, 100), "seed " + seed);
            cluster.run(40);
            for (int i = 0; i < 3; i++) {
                boolean found = cluster.applied(i).stream().anyMatch(a -> a.command().equals("inflight"));
                assertTrue(found, "seed " + seed + ": server " + i + " never applied the in-flight command");
            }
            cluster.deepCheck();
        }
    }

    @SuppressWarnings("unused")
    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unused")
    private static Set<Integer> none() {
        return Set.of();
    }
}
