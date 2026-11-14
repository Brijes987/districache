package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Read-index reads at the Raft node: when a read may be answered, and when it must not be. */
class ReadIndexNodeTest {

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage = new InMemoryRaftStorage();
        final List<Sent> sent = new ArrayList<>();
        final List<Object> ready = new ArrayList<>();
        final List<Object> failed = new ArrayList<>();
        final RaftNode node;

        Rig(List<Integer> peers, RaftConfig config) {
            node = new RaftNode(0, peers, storage, (to, m) -> sent.add(new Sent(to, m)), (i, c) -> { },
                    config.withCheckQuorum(false), new Random(1));
            node.setReadListener(new RaftNode.ReadListener() {
                @Override public void ready(Object token) { ready.add(token); }
                @Override public void failed(Object token) { failed.add(token); }
            });
        }

        /** Makes node 0 leader of a three-server group; its no-op (index 1) is NOT yet committed. */
        static Rig electedLeader(RaftConfig config) {
            Rig rig = new Rig(List.of(1, 2), config);
            for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
                rig.node.tick();
            }
            rig.node.receive(1, new RaftMessage.RequestVoteReply(rig.node.term(), true));
            assertEquals(RaftNode.Role.LEADER, rig.node.role());
            return rig;
        }

        /** A leader whose no-op is committed (server 1 holds index 1). */
        static Rig committedLeader(RaftConfig config) {
            Rig rig = electedLeader(config);
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0));
            assertEquals(1, rig.node.commitIndex());
            rig.sent.clear();
            return rig;
        }

        long lastRoundSentTo(int peer) {
            long round = -1;
            for (Sent s : sent) {
                if (s.to() == peer && s.message() instanceof RaftMessage.AppendEntries ae) {
                    round = ae.readRound();
                }
            }
            return round;
        }

        void ack(int peer, long round) {
            node.receive(peer, new RaftMessage.AppendEntriesReply(node.term(), true, 1, 0, 0, round));
        }
    }

    @Test
    void aFollowerRefusesAndPointsToTheLeader() {
        Rig rig = new Rig(List.of(1, 2), RaftConfig.defaults());
        RaftNode.ReadStart start = rig.node.beginRead("r");
        assertFalse(start.accepted());
        assertTrue(rig.ready.isEmpty() && rig.failed.isEmpty());
    }

    @Test
    void aReadIsNotAnsweredUntilAMajorityAcknowledgesAHeartbeatSentAfterIt() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults());
        assertTrue(rig.node.beginRead("r").accepted());
        assertTrue(rig.ready.isEmpty(), "nobody has confirmed the leader yet");
        long round = rig.lastRoundSentTo(1);
        assertTrue(round >= 1, "the heartbeat sent for the read must carry its round");
        rig.ack(1, round);
        assertEquals(List.of("r"), rig.ready, "self + one follower is a majority of three");
        assertEquals(1, rig.node.readsServed());
    }

    @Test
    void anAcknowledgementOfAnEarlierHeartbeatDoesNotCountForALaterRead() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults());
        rig.node.beginRead("first");
        long first = rig.lastRoundSentTo(1);
        rig.node.beginRead("second");
        long second = rig.lastRoundSentTo(1);
        assertTrue(second > first);
        rig.ack(1, first);
        assertEquals(List.of("first"), rig.ready, "an ack of round 1 proves nothing about the read started in round 2");
        rig.ack(2, second);
        assertEquals(List.of("first", "second"), rig.ready);
    }

    @Test
    void aDelayedOldReplyCannotLowerWhatIsAlreadyProven() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults());
        rig.node.beginRead("r");
        long round = rig.lastRoundSentTo(1);
        rig.ack(1, round);
        assertEquals(1, rig.ready.size());
        rig.ack(1, 0); // a late reply from before any read
        rig.node.beginRead("later");
        assertEquals(1, rig.ready.size(), "the later read still needs its own confirmation");
    }

    @Test
    void aReadWaitsForTheLeadersFirstEntryOfItsTermToCommit() {
        Rig rig = Rig.electedLeader(RaftConfig.defaults()); // no-op at index 1 not committed
        rig.node.beginRead("r");
        long round = rig.lastRoundSentTo(1);
        // server 1 confirms the leader but has NOT stored the no-op (matchIndex 0): a majority follows, yet the
        // commit index is not known to be current
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), false, 0, 1, 0, round));
        assertTrue(rig.ready.isEmpty(), "must wait until the no-op of this term is committed");
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0, round));
        assertEquals(List.of("r"), rig.ready);
    }

    @Test
    void aReadWritesNothingToTheLog() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults());
        long before = rig.storage.lastIndex();
        rig.node.beginRead("r");
        rig.ack(1, rig.lastRoundSentTo(1));
        assertEquals(before, rig.storage.lastIndex());
    }

    @Test
    void aLeaderThatLosesLeadershipFailsItsPendingReadsAndNeverAnswersThem() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults());
        rig.node.beginRead("r");
        rig.node.receive(2, new RaftMessage.RequestVote(rig.node.term() + 1, 2, 5, 5));
        assertEquals(RaftNode.Role.FOLLOWER, rig.node.role());
        assertEquals(List.of("r"), rig.failed);
        assertTrue(rig.ready.isEmpty());
        rig.ack(1, 99); // a stale reply arriving afterwards changes nothing
        assertTrue(rig.ready.isEmpty());
    }

    @Test
    void aOneServerGroupAnswersAtOnce() {
        Rig rig = new Rig(List.of(), RaftConfig.defaults());
        for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.LEADER; i++) {
            rig.node.tick();
        }
        assertEquals(RaftNode.Role.LEADER, rig.node.role());
        assertTrue(rig.node.beginRead("r").accepted());
        assertEquals(List.of("r"), rig.ready);
    }

    @Test
    void aFollowerEchoesTheRoundInEveryReplyToAnAppendEntries() {
        Rig rig = new Rig(List.of(1, 2), RaftConfig.defaults());
        long term = 3;
        // success
        rig.node.receive(1, new RaftMessage.AppendEntries(term, 1, 0, 0,
                List.of(new LogEntry(term, "x".getBytes())), 0, 7));
        // failure: prev entry missing
        rig.node.receive(1, new RaftMessage.AppendEntries(term, 1, 50, 2, List.of(), 0, 8));
        // failure: prev entry has another term
        rig.node.receive(1, new RaftMessage.AppendEntries(term, 1, 1, 2, List.of(), 0, 9));
        List<Long> echoed = new ArrayList<>();
        for (Sent s : rig.sent) {
            if (s.message() instanceof RaftMessage.AppendEntriesReply r) {
                echoed.add(r.readRound());
            }
        }
        assertEquals(List.of(7L, 8L, 9L), echoed);
    }

    // ---- each deliberate bug must produce the wrong behaviour these tests forbid

    @Test
    void withoutTheLeadershipCheckAReadIsAnsweredAtOnce() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults().withFaults(RaftConfig.Fault.READ_SKIP_LEADERSHIP_CHECK));
        rig.node.beginRead("r");
        assertEquals(List.of("r"), rig.ready, "the injected bug answers without any acknowledgement");
    }

    @Test
    void withoutTheTermStartWaitAReadIsAnsweredBeforeTheNoopCommits() {
        Rig rig = Rig.electedLeader(RaftConfig.defaults().withFaults(RaftConfig.Fault.READ_IGNORE_TERM_START));
        rig.node.beginRead("r");
        long round = rig.lastRoundSentTo(1);
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), false, 0, 1, 0, round));
        assertEquals(List.of("r"), rig.ready, "the injected bug answers although the no-op is not committed");
    }

    @Test
    void countingOldAcknowledgementsAnswersAReadNobodyConfirmed() {
        Rig rig = Rig.committedLeader(RaftConfig.defaults().withFaults(RaftConfig.Fault.READ_COUNT_OLD_ACKS));
        rig.ack(1, 0); // an old heartbeat reply, before any read
        rig.node.beginRead("r");
        assertEquals(List.of("r"), rig.ready, "the injected bug accepts the old reply");
    }
}
