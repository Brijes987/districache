package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Hand-driven tests of one {@link RaftNode}: the test plays the other servers by feeding in exact messages.
 * Random chaos rarely produces the two situations in the first tests (they need a precise interleaving), so they
 * are scripted. Each one also runs against a node with the matching deliberate bug and asserts the bug IS visible,
 * which proves the test is able to fail.
 */
class RaftNodeTest {

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage;
        final List<Sent> sent = new ArrayList<>();
        final List<String> applied = new ArrayList<>();
        final RaftNode node;

        Rig(int id, List<Integer> peers, RaftConfig config) {
            this(id, peers, config, new InMemoryRaftStorage());
        }

        Rig(int id, List<Integer> peers, RaftConfig config, InMemoryRaftStorage storage) {
            this.storage = storage;
            node = new RaftNode(id, peers, storage, (to, m) -> sent.add(new Sent(to, m)),
                    (index, command) -> applied.add(index + ":" + new String(command)), config, new Random(1));
        }

        void tickUntilCandidate() {
            for (int i = 0; i < 1000 && node.role() != RaftNode.Role.CANDIDATE; i++) {
                node.tick();
            }
            assertEquals(RaftNode.Role.CANDIDATE, node.role());
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

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, command.getBytes());
    }

    private static RaftConfig noCheckQuorum() {
        return RaftConfig.defaults().withCheckQuorum(false);
    }

    // ---------------------------------------------------------------- Figure 8 (Raft paper, section 5.4.2)

    /** Server 0 holds [t1, t2] and has seen term 3, so its next election is term 4. */
    private static Rig leaderWithOldTermEntry(RaftConfig config) {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.append(List.of(entry(1, "a"), entry(2, "b")));
        storage.saveTermAndVote(3, -1);
        return new Rig(0, List.of(1, 2), config, storage);
    }

    @Test
    void aLeaderDoesNotCommitAnOldTermEntryJustBecauseAMajorityHoldsIt() {
        Rig rig = leaderWithOldTermEntry(noCheckQuorum());
        rig.tickUntilCandidate();
        assertEquals(4, rig.node.term());
        rig.node.receive(1, new RaftMessage.RequestVoteReply(4, true));
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        assertEquals(3, rig.node.lastLogIndex(), "the new leader appends a no-op of its own term");

        rig.node.receive(1, new RaftMessage.AppendEntriesReply(4, true, 2, 0, 0));
        assertEquals(0, rig.node.commitIndex(),
                "index 2 is on a majority but from term 2; committing it would let a later leader overwrite it");

        rig.node.receive(1, new RaftMessage.AppendEntriesReply(4, true, 3, 0, 0));
        assertEquals(3, rig.node.commitIndex(), "once a current-term entry is on a majority, everything before it commits");
        assertEquals(List.of("1:a", "2:b"), rig.applied);
    }

    @Test
    void theFigure8MistakeIsVisibleWhenTheBugIsSwitchedOn() {
        Rig rig = leaderWithOldTermEntry(
                noCheckQuorum().withFaults(RaftConfig.Fault.COMMIT_OLD_TERMS_BY_COUNTING));
        rig.tickUntilCandidate();
        rig.node.receive(1, new RaftMessage.RequestVoteReply(4, true));
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(4, true, 2, 0, 0));
        assertEquals(2, rig.node.commitIndex(), "with the bug the test above would fail, so it can detect it");
    }

    // ---------------------------------------------------------------- follower commit limit

    private static Rig followerWithUnverifiedTail(RaftConfig config) {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        // index 2 came from an old leader and may differ from the current leader's index 2
        storage.append(List.of(entry(1, "a"), entry(1, "stale")));
        storage.saveTermAndVote(1, -1);
        return new Rig(1, List.of(0, 2), config, storage);
    }

    @Test
    void aFollowerOnlyCommitsEntriesTheLeaderHasVerifiedAgainstItsLog() {
        Rig rig = followerWithUnverifiedTail(noCheckQuorum());
        // heartbeat: matches index 1, carries no entries, leader has committed up to index 2
        rig.node.receive(0, new RaftMessage.AppendEntries(2, 0, 1, 1, List.of(), 2));
        assertEquals(1, rig.node.commitIndex(), "index 2 was never verified against this leader");
        assertEquals(List.of("1:a"), rig.applied);
    }

    @Test
    void committingPastTheVerifiedPrefixIsVisibleWhenTheBugIsSwitchedOn() {
        Rig rig = followerWithUnverifiedTail(
                noCheckQuorum().withFaults(RaftConfig.Fault.COMMIT_PAST_VERIFIED_ENTRIES));
        rig.node.receive(0, new RaftMessage.AppendEntries(2, 0, 1, 1, List.of(), 2));
        assertEquals(2, rig.node.commitIndex());
        assertEquals(List.of("1:a", "2:stale"), rig.applied, "the stale entry was applied: exactly the bug");
    }

    // ---------------------------------------------------------------- log matching

    @Test
    void aDelayedShorterAppendDoesNotDeleteEntriesThatAlreadyMatch() {
        Rig rig = new Rig(1, List.of(0, 2), noCheckQuorum());
        rig.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0, List.of(entry(1, "a"), entry(1, "b"), entry(1, "c")), 0));
        assertEquals(3, rig.node.lastLogIndex());
        // an older, shorter copy of the same leader's message arrives late
        rig.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0, List.of(entry(1, "a")), 0));
        assertEquals(3, rig.node.lastLogIndex(), "a matching prefix must not truncate the tail");
    }

    @Test
    void aRealConflictIsResolvedInFavourOfTheLeader() {
        Rig rig = new Rig(1, List.of(0, 2), noCheckQuorum());
        rig.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0, List.of(entry(1, "a"), entry(1, "x"), entry(1, "y")), 0));
        rig.node.receive(2, new RaftMessage.AppendEntries(2, 2, 1, 1, List.of(entry(2, "b")), 0));
        assertEquals(2, rig.node.lastLogIndex());
        assertEquals(2, rig.storage.termAt(2));
    }

    @Test
    void aFollowerRefusesEntriesWhenItsLogDoesNotContainThePreviousEntry() {
        Rig rig = new Rig(1, List.of(0, 2), noCheckQuorum());
        rig.node.receive(0, new RaftMessage.AppendEntries(1, 0, 5, 1, List.of(entry(1, "z")), 0));
        RaftMessage.AppendEntriesReply reply = (RaftMessage.AppendEntriesReply) rig.lastTo(0);
        assertFalse(reply.success());
        assertEquals(0, rig.node.lastLogIndex());
        assertEquals(1, reply.conflictIndex(), "tells the leader where to resume instead of backing up one at a time");
    }

    @Test
    void messagesFromAnOldTermAreRejectedAndChangeNothing() {
        Rig rig = new Rig(1, List.of(0, 2), noCheckQuorum());
        rig.node.receive(0, new RaftMessage.AppendEntries(5, 0, 0, 0, List.of(entry(5, "a")), 0));
        rig.node.receive(2, new RaftMessage.AppendEntries(3, 2, 0, 0, List.of(entry(3, "evil")), 1));
        assertEquals(5, rig.node.term());
        assertEquals(5, rig.storage.termAt(1));
        assertEquals(0, rig.node.commitIndex());
        RaftMessage.AppendEntriesReply reply = (RaftMessage.AppendEntriesReply) rig.lastTo(2);
        assertFalse(reply.success());
        assertEquals(5, reply.term(), "the stale sender learns the current term");
    }

    // ---------------------------------------------------------------- voting

    @Test
    void aServerVotesOnceAPerTermAndPersistsTheVoteBeforeAnswering() {
        Rig rig = new Rig(2, List.of(0, 1), noCheckQuorum());
        rig.node.receive(0, new RaftMessage.RequestVote(1, 0, 0, 0));
        assertEquals(0, rig.storage.votedFor());
        assertEquals(1, rig.storage.currentTerm());
        assertTrue(((RaftMessage.RequestVoteReply) rig.lastTo(0)).voteGranted());
        rig.node.receive(1, new RaftMessage.RequestVote(1, 1, 0, 0));
        assertFalse(((RaftMessage.RequestVoteReply) rig.lastTo(1)).voteGranted(), "already voted in term 1");
        rig.node.receive(0, new RaftMessage.RequestVote(1, 0, 0, 0));
        assertTrue(((RaftMessage.RequestVoteReply) rig.lastTo(0)).voteGranted(), "a repeated request from the same candidate is fine");
    }

    @Test
    void aServerRefusesACandidateWhoseLogIsBehindItsOwn() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        storage.append(List.of(entry(1, "a"), entry(2, "b")));
        Rig rig = new Rig(2, List.of(0, 1), noCheckQuorum(), storage);
        RaftNode node = rig.node;
        node.receive(0, new RaftMessage.RequestVote(3, 0, 5, 1)); // longer log but older last term
        assertFalse(((RaftMessage.RequestVoteReply) rig.lastTo(0)).voteGranted());
        node.receive(1, new RaftMessage.RequestVote(3, 1, 2, 2));
        assertTrue(((RaftMessage.RequestVoteReply) rig.lastTo(1)).voteGranted());
    }

    // ---------------------------------------------------------------- leadership

    @Test
    void aLeaderStepsDownWhenItSeesAHigherTerm() {
        Rig rig = new Rig(0, List.of(1, 2), noCheckQuorum());
        rig.tickUntilCandidate();
        rig.node.receive(1, new RaftMessage.RequestVoteReply(1, true));
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        rig.node.receive(2, new RaftMessage.AppendEntriesReply(9, false, 0, 1, 0));
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertEquals(9, rig.node.term());
        assertFalse(rig.node.propose("x".getBytes()).accepted());
    }

    @Test
    void aSingleServerGroupElectsItselfAndCommitsImmediately() {
        Rig rig = new Rig(0, List.of(), noCheckQuorum());
        for (int i = 0; i < 100 && rig.node.role() != RaftNode.Role.LEADER; i++) {
            rig.node.tick();
        }
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        RaftNode.ProposeResult r = rig.node.propose("x".getBytes());
        assertTrue(r.accepted());
        assertEquals(r.index(), rig.node.commitIndex());
        assertEquals(List.of(r.index() + ":x"), rig.applied);
    }
}
