package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.LinearizabilityChecker.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Pre-vote: before a real election (which raises the term) a node asks whether a majority would vote for it. The
 * scripted tests drive one node with exact messages. The cluster tests show the problem pre-vote solves (a node that
 * was cut off for a long time comes back with an inflated term and forces a healthy leader out), that it is solved,
 * and that each of two deliberately broken versions brings the problem back, so the test can fail.
 */
class PreVoteTest {

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage;
        final List<Sent> sent = new ArrayList<>();
        final RaftNode node;

        Rig(int id, List<Integer> peers, RaftConfig config) {
            this(id, peers, config, new InMemoryRaftStorage());
        }

        Rig(int id, List<Integer> peers, RaftConfig config, InMemoryRaftStorage storage) {
            this.storage = storage;
            node = new RaftNode(id, peers, storage, (to, m) -> sent.add(new Sent(to, m)), (i, c) -> {}, config,
                    new Random(1));
        }

        void tickUntilPreCandidate() {
            for (int i = 0; i < 1000 && !node.isPreCandidate(); i++) {
                node.tick();
            }
            assertTrue(node.isPreCandidate(), "the node never started a pre-vote");
        }

        List<RaftMessage> sentOfType(Class<? extends RaftMessage> type) {
            List<RaftMessage> out = new ArrayList<>();
            for (Sent s : sent) {
                if (type.isInstance(s.message())) {
                    out.add(s.message());
                }
            }
            return out;
        }

        RaftMessage lastTo(int peer) {
            for (int i = sent.size() - 1; i >= 0; i--) {
                if (sent.get(i).to() == peer) {
                    return sent.get(i).message();
                }
            }
            return null;
        }
    }

    private static RaftConfig on() {
        return RaftConfig.defaults().withPreVote(true);
    }

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, command.getBytes());
    }

    // ---------------------------------------------------------------- the candidate side

    @Test
    void withPreVoteATimeoutAsksFirstAndDoesNotRaiseTheTerm() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.saveTermAndVote(4, -1);
        Rig rig = new Rig(0, List.of(1, 2), on(), storage);
        rig.tickUntilPreCandidate();
        assertEquals(4, rig.node.term());
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertEquals(-1, storage.votedFor(), "no vote is cast for oneself yet");
        assertEquals(4, storage.currentTerm(), "nothing durable changed");
        assertTrue(rig.sentOfType(RaftMessage.RequestVote.class).isEmpty());
        assertEquals(new RaftMessage.PreVote(5, 0, 0, 0), rig.lastTo(1));
        assertEquals(new RaftMessage.PreVote(5, 0, 0, 0), rig.lastTo(2));
    }

    @Test
    void withoutPreVoteATimeoutRaisesTheTermAtOnce() {
        Rig rig = new Rig(0, List.of(1, 2), RaftConfig.defaults());
        for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
            rig.node.tick();
        }
        assertEquals(1, rig.node.term());
        assertTrue(rig.sentOfType(RaftMessage.PreVote.class).isEmpty());
    }

    @Test
    void aMajorityOfGrantedPreVotesStartsARealElection() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.saveTermAndVote(4, -1);
        Rig rig = new Rig(0, List.of(1, 2), on(), storage);
        rig.tickUntilPreCandidate();
        rig.node.receive(1, new RaftMessage.PreVoteReply(5, true));
        assertEquals(RaftNode.Role.CANDIDATE, rig.node.role());
        assertEquals(5, rig.node.term());
        assertEquals(0, storage.votedFor());
        assertFalse(rig.node.isPreCandidate());
        assertEquals(new RaftMessage.RequestVote(5, 0, 0, 0), rig.lastTo(1));
        assertEquals(new RaftMessage.RequestVote(5, 0, 0, 0), rig.lastTo(2));
    }

    @Test
    void withoutAMajorityTheTermNeverRisesHoweverLongItWaits() {
        Rig rig = new Rig(0, List.of(1, 2), on());
        for (int i = 0; i < 5_000; i++) {
            rig.node.tick();
        }
        assertEquals(0, rig.node.term());
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertTrue(rig.node.preVoteRoundsStarted() >= 100, "it should keep asking: " + rig.node.preVoteRoundsStarted());
        assertEquals(0, rig.storage.currentTerm());
    }

    @Test
    void refusalsDoNotCountAndASingleGrantInAFiveNodeClusterIsNotEnough() {
        Rig rig = new Rig(0, List.of(1, 2, 3, 4), on());
        rig.tickUntilPreCandidate();
        rig.node.receive(1, new RaftMessage.PreVoteReply(1, true));
        rig.node.receive(2, new RaftMessage.PreVoteReply(0, false));
        rig.node.receive(3, new RaftMessage.PreVoteReply(0, false));
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertEquals(0, rig.node.term());
        rig.node.receive(4, new RaftMessage.PreVoteReply(1, true));
        assertEquals(RaftNode.Role.CANDIDATE, rig.node.role(), "self + 2 grants is 3 of 5");
    }

    @Test
    void aGrantForAnEarlierRoundIsIgnored() {
        Rig rig = new Rig(0, List.of(1, 2), on());
        rig.tickUntilPreCandidate();
        rig.node.receive(1, new RaftMessage.PreVoteReply(7, true)); // not the term this node proposed
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertEquals(0, rig.node.term());
    }

    @Test
    void aRefusalThatCarriesAHigherRealTermIsAdoptedAndEndsThePreVote() {
        Rig rig = new Rig(0, List.of(1, 2), on());
        rig.tickUntilPreCandidate();
        rig.node.receive(1, new RaftMessage.PreVoteReply(9, false));
        assertEquals(9, rig.node.term());
        assertFalse(rig.node.isPreCandidate());
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
    }

    @Test
    void aLeadersMessageEndsAPreVote() {
        Rig rig = new Rig(0, List.of(1, 2), on());
        rig.tickUntilPreCandidate();
        rig.node.receive(1, new RaftMessage.AppendEntries(3, 1, 0, 0, List.of(), 0));
        assertFalse(rig.node.isPreCandidate());
        assertEquals(3, rig.node.term());
        assertEquals(1, rig.node.leaderId());
    }

    @Test
    void aSingleNodeClusterStillElectsItselfAtOnce() {
        Rig rig = new Rig(0, List.of(), on());
        for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.LEADER; i++) {
            rig.node.tick();
        }
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        assertEquals(1, rig.node.term());
    }

    // ---------------------------------------------------------------- the voter side

    /** Node 1 follows leader 0 in term 5. */
    private static Rig followerOfALeader(RaftConfig config) {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.append(List.of(entry(5, "a")));
        Rig rig = new Rig(1, List.of(0, 2), config, storage);
        rig.node.receive(0, new RaftMessage.AppendEntries(5, 0, 1, 5, List.of(), 0));
        assertEquals(5, rig.node.term());
        assertEquals(0, rig.node.leaderId());
        return rig;
    }

    @Test
    void aFollowerWithALiveLeaderRefusesAndChangesNothing() {
        Rig rig = followerOfALeader(on());
        rig.sent.clear();
        rig.node.receive(2, new RaftMessage.PreVote(6, 2, 1, 5));
        assertEquals(new RaftMessage.PreVoteReply(5, false), rig.lastTo(2));
        assertEquals(5, rig.node.term());
        assertEquals(-1, rig.storage.votedFor());
        assertEquals(0, rig.node.leaderId());
    }

    @Test
    void aFollowerThatHasHeardNothingForAWhileGrantsAndStillChangesNothing() {
        Rig rig = followerOfALeader(on());
        for (int i = 0; i < on().electionTimeoutMinTicks() - 1; i++) {
            rig.node.tick(); // stay below its own election timeout (random, at least the minimum)
        }
        rig.sent.clear();
        rig.node.receive(2, new RaftMessage.PreVote(6, 2, 1, 5));
        // exactly at the lease limit the leader may still be alive: wait out one more tick if it was refused
        if (rig.lastTo(2).equals(new RaftMessage.PreVoteReply(5, false))) {
            rig.node.tick();
            rig.node.receive(2, new RaftMessage.PreVote(6, 2, 1, 5));
        }
        assertEquals(new RaftMessage.PreVoteReply(6, true), rig.lastTo(2));
        assertEquals(5, rig.node.term(), "granting a pre-vote must not change the term");
        assertEquals(-1, rig.storage.votedFor(), "...or cast a vote");
        assertEquals(5, rig.storage.currentTerm());
    }

    @Test
    void aGrantedPreVoteDoesNotRestartTheVotersWait() {
        // if it did, one chatty cut-off node could keep every other node from ever timing out
        Rig rig = new Rig(1, List.of(0, 2), on());
        rig.node.receive(2, new RaftMessage.PreVote(1, 2, 0, 0));
        assertEquals(new RaftMessage.PreVoteReply(1, true), rig.lastTo(2));
        long before = rig.node.preVoteRoundsStarted();
        for (int i = 0; i < on().electionTimeoutMaxTicks() && rig.node.preVoteRoundsStarted() == before; i++) {
            rig.node.tick();
        }
        assertTrue(rig.node.preVoteRoundsStarted() > before, "the voter still times out on its own schedule");
    }

    @Test
    void aStaleLogIsRefused() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.append(List.of(entry(1, "a"), entry(3, "b")));
        storage.saveTermAndVote(3, -1);
        Rig rig = new Rig(1, List.of(0, 2), on(), storage);
        rig.node.receive(2, new RaftMessage.PreVote(4, 2, 5, 2)); // longer, but its last entry is from an older term
        assertEquals(new RaftMessage.PreVoteReply(3, false), rig.lastTo(2));
        rig.node.receive(2, new RaftMessage.PreVote(4, 2, 1, 3)); // same last term, shorter
        assertEquals(new RaftMessage.PreVoteReply(3, false), rig.lastTo(2));
        rig.node.receive(2, new RaftMessage.PreVote(4, 2, 2, 3)); // as up to date
        assertEquals(new RaftMessage.PreVoteReply(4, true), rig.lastTo(2));
    }

    @Test
    void aPreVoteThatDoesNotProposeAHigherTermIsRefused() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.saveTermAndVote(6, -1);
        Rig rig = new Rig(1, List.of(0, 2), on(), storage);
        rig.node.receive(2, new RaftMessage.PreVote(6, 2, 0, 0));
        assertEquals(new RaftMessage.PreVoteReply(6, false), rig.lastTo(2));
        rig.node.receive(2, new RaftMessage.PreVote(3, 2, 0, 0));
        assertEquals(new RaftMessage.PreVoteReply(6, false), rig.lastTo(2));
    }

    @Test
    void aProposedTermFarAheadIsNeverAdopted() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.saveTermAndVote(3, -1);
        Rig rig = new Rig(1, List.of(0, 2), on(), storage);
        rig.node.receive(2, new RaftMessage.PreVote(500, 2, 0, 0));
        assertEquals(3, rig.node.term());
        assertEquals(3, storage.currentTerm());
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
    }

    @Test
    void adoptingTheProposedTermIsVisibleWhenTheBugIsSwitchedOn() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.saveTermAndVote(3, -1);
        Rig rig = new Rig(1, List.of(0, 2), on().withFaults(RaftConfig.Fault.PREVOTE_ADOPT_TERM), storage);
        rig.node.receive(2, new RaftMessage.PreVote(500, 2, 0, 0));
        assertEquals(500, rig.node.term(), "with the bug the test above would fail, so it can detect it");
    }

    @Test
    void aLeaderRefusesAPreVote() {
        Rig rig = new Rig(0, List.of(1, 2), on());
        for (int i = 0; i < 1000 && !rig.node.isPreCandidate(); i++) {
            rig.node.tick();
        }
        rig.node.receive(1, new RaftMessage.PreVoteReply(1, true));
        assertEquals(RaftNode.Role.CANDIDATE, rig.node.role());
        rig.node.receive(1, new RaftMessage.RequestVoteReply(1, true));
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        rig.sent.clear();
        rig.node.receive(2, new RaftMessage.PreVote(2, 2, 5, 1));
        assertEquals(new RaftMessage.PreVoteReply(1, false), rig.lastTo(2));
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        assertEquals(1, rig.node.term());
    }

    @Test
    void aNodeWithPreVoteSwitchedOffRefuses() {
        Rig rig = new Rig(1, List.of(0, 2), RaftConfig.defaults());
        rig.node.receive(2, new RaftMessage.PreVote(1, 2, 0, 0));
        assertEquals(new RaftMessage.PreVoteReply(0, false), rig.lastTo(2));
        assertEquals(0, rig.node.term());
    }

    // ---------------------------------------------------------------- the problem, the cure, and the broken cures

    private record Outcome(long termBefore, long termAfter, int leaderBefore, int leaderAfter, long victimTerm) {}

    /**
     * Three servers; for a long time one follower cannot hear the leader but can still send to everyone (a one-way
     * link failure), with no writes meanwhile; then the link is repaired.
     */
    private static Outcome cannotHearTheLeader(RaftConfig config, long seed, int steps) {
        SimCluster cluster = new SimCluster(3, seed, config);
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000), "no leader elected");
        cluster.run(100); // let the leader's first entry reach everyone
        int leader = cluster.leader();
        long term = cluster.node(leader).term();
        int victim = (leader + 1) % 3;
        cluster.cutOneWay(leader, victim);
        cluster.run(steps);
        long victimTerm = cluster.node(victim).term();
        cluster.heal();
        cluster.run(400);
        cluster.deepCheck();
        return new Outcome(term, cluster.maxTermSeen(), leader, cluster.leader(), victimTerm);
    }

    /** One follower fully cut off for a long time with no writes meanwhile, then reconnected. */
    private static Outcome cutOffAndReturn(RaftConfig config, long seed, int steps) {
        SimCluster cluster = new SimCluster(3, seed, config);
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000), "no leader elected");
        cluster.run(100);
        int leader = cluster.leader();
        long term = cluster.node(leader).term();
        int victim = (leader + 1) % 3;
        cluster.isolate(victim);
        cluster.run(steps);
        long victimTerm = cluster.node(victim).term();
        cluster.heal();
        cluster.run(400);
        cluster.deepCheck();
        return new Outcome(term, cluster.maxTermSeen(), leader, cluster.leader(), victimTerm);
    }

    @Test
    void withoutPreVoteACutOffNodeInflatesItsTermAndForcesTheLeaderOutWhenItReturns() {
        for (long seed = 1; seed <= 10; seed++) {
            Outcome o = cutOffAndReturn(RaftConfig.defaults(), seed, 1_000);
            assertTrue(o.victimTerm() > o.termBefore() + 10, "seed " + seed + ": the cut-off node's term should balloon: " + o);
            assertTrue(o.termAfter() > o.termBefore(), "seed " + seed + ": the healthy leader should have been disrupted: " + o);
        }
    }

    @Test
    void withPreVoteACutOffNodeNeverRaisesItsTermAndTheLeaderIsUndisturbed() {
        for (long seed = 1; seed <= 10; seed++) {
            Outcome o = cutOffAndReturn(on(), seed, 1_000);
            assertEquals(o.termBefore(), o.victimTerm(), "seed " + seed + ": the cut-off node kept its term: " + o);
            assertEquals(o.termBefore(), o.termAfter(), "seed " + seed + ": no election happened: " + o);
            assertEquals(o.leaderBefore(), o.leaderAfter(), "seed " + seed + ": the same leader carries on: " + o);
        }
    }

    @Test
    void withoutPreVoteAFollowerThatCannotHearTheLeaderDeposesAHealthyLeader() {
        for (long seed = 1; seed <= 10; seed++) {
            Outcome o = cannotHearTheLeader(RaftConfig.defaults(), seed, 600);
            assertTrue(o.termAfter() > o.termBefore(), "seed " + seed + ": a healthy leader should have been deposed: " + o);
            assertTrue(o.victimTerm() > o.termBefore(), "seed " + seed + ": the follower should have raised its term: " + o);
        }
    }

    @Test
    void withPreVoteAFollowerThatCannotHearTheLeaderChangesNothing() {
        for (long seed = 1; seed <= 10; seed++) {
            Outcome o = cannotHearTheLeader(on(), seed, 600);
            assertEquals(o.termBefore(), o.termAfter(), "seed " + seed + ": " + o);
            assertEquals(o.termBefore(), o.victimTerm(), "seed " + seed + ": " + o);
            assertEquals(o.leaderBefore(), o.leaderAfter(), "seed " + seed + ": " + o);
        }
    }

    @Test
    void grantingAPreVoteToLiveLeadersFollowersBringsTheDisruptionBack() {
        int disrupted = 0;
        for (long seed = 1; seed <= 10; seed++) {
            Outcome o = cannotHearTheLeader(on().withFaults(RaftConfig.Fault.PREVOTE_IGNORE_LEADER_LEASE), seed, 600);
            if (o.termAfter() > o.termBefore()) {
                disrupted++;
            }
        }
        assertEquals(10, disrupted, "ignoring the leader lease must be caught in every run");
    }

    @Test
    void adoptingTheTermOfAPreVoteMakesElectionsImpossibleAndIsCaught() {
        // every pre-vote would raise the receivers' terms to the proposed one, after which "is the proposed term
        // higher than mine" is false everywhere, so nobody can ever win a pre-vote
        for (long seed = 1; seed <= 10; seed++) {
            SimCluster cluster = new SimCluster(3, seed, on().withFaults(RaftConfig.Fault.PREVOTE_ADOPT_TERM));
            assertFalse(cluster.runUntil(() -> cluster.leader() != -1, 2_000), "seed " + seed + ": a leader was elected");
        }
    }

    @Test
    void aCutOffNodeThatMissedWritesCatchesUpWithoutDisruptingAnyone() {
        SimCluster cluster = new SimCluster(3, 21, on());
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000));
        int leader = cluster.leader();
        long term = cluster.node(leader).term();
        int victim = (leader + 2) % 3;
        cluster.run(50);
        cluster.isolate(victim);
        for (int i = 0; i < 40; i++) {
            cluster.propose(leader, "w" + i);
            cluster.run(10);
        }
        cluster.run(500);
        cluster.heal();
        assertTrue(cluster.runUntil(() -> cluster.applied(victim).size() >= 40, 1_000),
                "the returning node should catch up from the leader's log");
        assertEquals(term, cluster.maxTermSeen());
        assertEquals(leader, cluster.leader());
        cluster.deepCheck();
    }

    // ---------------------------------------------------------------- it must not make the cluster unable to elect

    @Test
    void aNewLeaderIsElectedAfterTheLeaderCrashes() {
        for (long seed = 1; seed <= 20; seed++) {
            SimCluster cluster = new SimCluster(5, seed, on());
            assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000));
            cluster.run(50);
            int old = cluster.leader();
            cluster.crash(old);
            assertTrue(cluster.runUntil(() -> cluster.leader() != -1 && cluster.leader() != old, 500),
                    "seed " + seed + ": no new leader within 500 steps of the crash");
            cluster.deepCheck();
        }
    }

    @Test
    void aNewLeaderIsElectedWhenTheLeaderIsCutOff() {
        SimCluster cluster = new SimCluster(5, 5, on());
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000));
        cluster.run(50);
        int old = cluster.leader();
        cluster.isolate(old);
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1 && cluster.leader() != old, 500));
        cluster.deepCheck();
    }

    @Test
    void aLeaderIsElectedAfterEveryServerRestarts() {
        for (long seed = 1; seed <= 10; seed++) {
            SimCluster cluster = new SimCluster(3, seed, on());
            assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000));
            cluster.run(50);
            for (int i = 0; i < 3; i++) {
                cluster.crash(i);
            }
            for (int i = 0; i < 3; i++) {
                cluster.restart(i);
            }
            assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000), "seed " + seed);
            cluster.deepCheck();
        }
    }

    @Test
    void anAlreadyElectedClusterNeverStartsAPreVoteWhileTheLeaderIsHealthy() {
        SimCluster cluster = new SimCluster(3, 8, on());
        assertTrue(cluster.runUntil(() -> cluster.leader() != -1, 1_000));
        cluster.run(100);
        long rounds = 0;
        for (int i = 0; i < 3; i++) {
            rounds += cluster.node(i).preVoteRoundsStarted();
        }
        long termBefore = cluster.maxTermSeen();
        cluster.run(3_000);
        long roundsAfter = 0;
        for (int i = 0; i < 3; i++) {
            roundsAfter += cluster.node(i).preVoteRoundsStarted();
        }
        assertEquals(rounds, roundsAfter, "a healthy leader's heartbeats keep followers from timing out");
        assertEquals(termBefore, cluster.maxTermSeen());
    }

    // ---------------------------------------------------------------- safety under random faults

    @Test
    void safetyHoldsUnderRandomFailuresWithPreVoteAcross60Seeds() {
        for (long seed = 3_000; seed < 3_060; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.normal(), on());
        }
    }

    @Test
    void safetyHoldsUnderBrutalFailuresWithPreVoteAcross40Seeds() {
        for (long seed = 4_000; seed < 4_040; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.brutal(), on());
        }
    }

    @Test
    void safetyHoldsWithPreVoteAndWithoutCheckQuorum() {
        for (long seed = 5_000; seed < 5_020; seed++) {
            Chaos.fullRun(seed, 3_000, Chaos.Profile.brutal(), on().withCheckQuorum(false));
        }
    }

    @Test
    void preVoteRunsReplayExactlyFromTheirSeed() {
        assertEquals(fingerprintOf(91), fingerprintOf(91));
    }

    private static long fingerprintOf(long seed) {
        SimCluster cluster = new SimCluster(5, seed, on());
        new Chaos(cluster, seed).run(1_500, Chaos.Profile.brutal());
        return cluster.fingerprint();
    }

    @Test
    void theCorrectSystemWithPreVoteIsLinearizableUnderFaultsAcross60Runs() {
        LinearizabilityChecker checker = new LinearizabilityChecker();
        for (long seed = 1; seed <= 30; seed++) {
            for (Chaos.Profile profile : new Chaos.Profile[] {Chaos.Profile.normal(), Chaos.Profile.brutal()}) {
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile, on(), null, null);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
            }
        }
    }
}
