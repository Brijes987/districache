package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Group commit at the Raft node: {@link RaftNode#proposeBatch} must write many entries with ONE storage append (one
 * fsync), without weakening any rule a single {@link RaftNode#propose} follows.
 */
class GroupCommitNodeTest {

    /** Counts storage appends and can check, at the moment a message is sent, that what it carries is already stored. */
    private static final class CountingStorage implements RaftStorage {
        final InMemoryRaftStorage inner = new InMemoryRaftStorage();
        int appendCalls;

        @Override public long currentTerm() { return inner.currentTerm(); }
        @Override public int votedFor() { return inner.votedFor(); }
        @Override public void saveTermAndVote(long term, int votedFor) { inner.saveTermAndVote(term, votedFor); }
        @Override public long lastIndex() { return inner.lastIndex(); }
        @Override public LogEntry entry(long index) { return inner.entry(index); }
        @Override public long termAt(long index) { return inner.termAt(index); }
        @Override public List<LogEntry> slice(long from, long to) { return inner.slice(from, to); }
        @Override public void truncateFrom(long index) { inner.truncateFrom(index); }
        @Override public void append(List<LogEntry> entries) {
            appendCalls++;
            inner.append(entries);
        }
    }

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final CountingStorage storage = new CountingStorage();
        final List<Sent> sent = new ArrayList<>();
        final List<String> applied = new ArrayList<>();
        final List<String> problems = new ArrayList<>();
        final RaftNode node;

        Rig(List<Integer> peers) {
            node = new RaftNode(0, peers, storage, (to, m) -> {
                if (m instanceof RaftMessage.AppendEntries ae && !ae.entries().isEmpty()) {
                    long last = ae.prevLogIndex() + ae.entries().size();
                    if (storage.lastIndex() < last) {
                        problems.add("sent entries up to " + last + " but only " + storage.lastIndex() + " are stored");
                    }
                }
                sent.add(new Sent(to, m));
            }, (index, command) -> applied.add(index + ":" + new String(command)),
                    RaftConfig.defaults().withCheckQuorum(false), new Random(1));
        }

        /** Elects node 0 in a three-server group and has server 1 acknowledge its no-op (log index 1). */
        static Rig leader() {
            Rig rig = new Rig(List.of(1, 2));
            for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
                rig.node.tick();
            }
            rig.node.receive(1, new RaftMessage.RequestVoteReply(rig.node.term(), true));
            assertEquals(RaftNode.Role.LEADER, rig.node.role());
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0));
            assertEquals(1, rig.node.commitIndex());
            rig.sent.clear();
            return rig;
        }

        List<RaftMessage.AppendEntries> appendsTo(int peer) {
            List<RaftMessage.AppendEntries> out = new ArrayList<>();
            for (Sent s : sent) {
                if (s.to() == peer && s.message() instanceof RaftMessage.AppendEntries ae && !ae.entries().isEmpty()) {
                    out.add(ae);
                }
            }
            return out;
        }
    }

    private static List<byte[]> commands(String... texts) {
        List<byte[]> out = new ArrayList<>();
        for (String t : texts) {
            out.add(t.getBytes());
        }
        return out;
    }

    @Test
    void aBatchIsOneStorageWriteOneBroadcastAndConsecutiveIndexes() {
        Rig rig = Rig.leader();
        int before = rig.storage.appendCalls;
        RaftNode.ProposeResult r = rig.node.proposeBatch(commands("a", "b", "c", "d", "e"));

        assertTrue(r.accepted());
        assertEquals(2, r.index(), "the first of the batch follows the no-op at index 1");
        assertEquals(before + 1, rig.storage.appendCalls, "five entries, ONE append (one fsync)");
        assertEquals(6, rig.storage.lastIndex());
        for (int i = 0; i < 5; i++) {
            assertEquals(r.term(), rig.storage.entry(2 + i).term());
            assertEquals("abcde".substring(i, i + 1), new String(rig.storage.entry(2 + i).command()));
        }
        List<RaftMessage.AppendEntries> toPeer = rig.appendsTo(1);
        assertEquals(1, toPeer.size(), "one AppendEntries per follower for the whole batch");
        assertEquals(5, toPeer.get(0).entries().size());
        assertEquals(1, rig.appendsTo(2).size(), "the other follower got one message as well");
    }

    @Test
    void nothingInTheBatchCommitsOrIsAppliedUntilAMajorityHoldsIt() {
        Rig rig = Rig.leader();
        RaftNode.ProposeResult r = rig.node.proposeBatch(commands("a", "b", "c"));
        assertEquals(1, rig.node.commitIndex(), "a leader alone is not a majority of three");
        assertFalse(rig.applied.stream().anyMatch(s -> s.endsWith(":a") || s.endsWith(":b") || s.endsWith(":c")));

        rig.node.receive(1, new RaftMessage.AppendEntriesReply(r.term(), true, 4, 0, 0));
        assertEquals(4, rig.node.commitIndex());
        assertEquals(List.of("2:a", "3:b", "4:c"),
                rig.applied.subList(rig.applied.size() - 3, rig.applied.size()), "applied in log order");
    }

    @Test
    void aFollowerThatAcknowledgesOnlyPartOfABatchCommitsOnlyThatPart() {
        Rig rig = Rig.leader();
        RaftNode.ProposeResult r = rig.node.proposeBatch(commands("a", "b", "c", "d"));
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(r.term(), true, 3, 0, 0)); // holds a and b only
        assertEquals(3, rig.node.commitIndex());
        assertTrue(rig.applied.contains("3:b"));
        assertFalse(rig.applied.contains("4:c"));
    }

    @Test
    void entriesAreInTheLeadersStorageBeforeAnyMessageCarryingThemLeavesTheNode() {
        Rig rig = Rig.leader();
        rig.node.proposeBatch(commands("a", "b", "c"));
        assertFalse(rig.sent.isEmpty());
        assertEquals(List.of(), rig.problems);
    }

    @Test
    void aBatchEqualsTheSameCommandsProposedOneByOne() {
        Rig batch = Rig.leader();
        Rig single = Rig.leader();
        batch.node.proposeBatch(commands("a", "b", "c"));
        for (String c : new String[] {"a", "b", "c"}) {
            single.node.propose(c.getBytes());
        }
        assertEquals(single.storage.lastIndex(), batch.storage.lastIndex());
        for (long i = 1; i <= batch.storage.lastIndex(); i++) {
            assertEquals(single.storage.entry(i).term(), batch.storage.entry(i).term());
            assertEquals(new String(single.storage.entry(i).command()), new String(batch.storage.entry(i).command()));
        }
        assertEquals(batch.storage.appendCalls + 2, single.storage.appendCalls,
                "one-by-one costs two more appends than the batch");
    }

    @Test
    void aNodeThatIsNotLeaderRefusesTheWholeBatchAndWritesNothing() {
        Rig rig = new Rig(List.of(1, 2));
        RaftNode.ProposeResult r = rig.node.proposeBatch(commands("a", "b"));
        assertFalse(r.accepted());
        assertEquals(0, rig.storage.appendCalls);
        assertEquals(0, rig.storage.lastIndex());
    }

    @Test
    void anEmptyBatchOrAnEmptyCommandIsRejectedBeforeAnythingIsWritten() {
        Rig rig = Rig.leader();
        int before = rig.storage.appendCalls;
        long last = rig.storage.lastIndex();
        assertThrows(IllegalArgumentException.class, () -> rig.node.proposeBatch(List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> rig.node.proposeBatch(List.of("ok".getBytes(), new byte[0], "ok2".getBytes())));
        assertEquals(before, rig.storage.appendCalls);
        assertEquals(last, rig.storage.lastIndex(), "not even the valid first command was written");
    }

    @Test
    void aFollowerWritesAReceivedBatchWithOneStorageAppend() {
        CountingStorage storage = new CountingStorage();
        List<Sent> sent = new ArrayList<>();
        RaftNode follower = new RaftNode(1, List.of(0, 2), storage, (to, m) -> sent.add(new Sent(to, m)),
                (i, c) -> {}, RaftConfig.defaults(), new Random(2));
        List<LogEntry> entries = new ArrayList<>();
        for (String c : new String[] {"a", "b", "c", "d", "e", "f"}) {
            entries.add(new LogEntry(1, c.getBytes()));
        }
        follower.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0, entries, 0));
        assertEquals(1, storage.appendCalls, "six entries, one fsync on the follower too");
        assertEquals(6, storage.lastIndex());
    }

    @Test
    void aSingleServerGroupCommitsAndAppliesTheWholeBatchInsideTheCall() {
        Rig rig = new Rig(List.of());
        for (int i = 0; i < 100 && rig.node.role() != RaftNode.Role.LEADER; i++) {
            rig.node.tick();
        }
        RaftNode.ProposeResult r = rig.node.proposeBatch(commands("a", "b", "c"));
        assertEquals(r.index() + 2, rig.node.commitIndex());
        assertEquals(List.of(r.index() + ":a", (r.index() + 1) + ":b", (r.index() + 2) + ":c"),
                rig.applied.subList(rig.applied.size() - 3, rig.applied.size()));
    }
}
