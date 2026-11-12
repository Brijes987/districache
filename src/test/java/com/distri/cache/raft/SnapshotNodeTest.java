package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Hand-driven checks of how {@link RaftNode} takes, sends, installs and restarts from snapshots. */
class SnapshotNodeTest {

    /** A state machine whose state is the list of commands applied so far. */
    private static final class ListMachine implements StateMachine {
        final List<String> state = new ArrayList<>();
        int restores;
        boolean rejectRestore;

        @Override
        public void apply(long index, byte[] command) {
            state.add(index + ":" + new String(command));
        }

        @Override
        public byte[] snapshot() {
            return String.join(",", state).getBytes();
        }

        @Override
        public void restore(byte[] bytes) {
            if (rejectRestore) {
                throw new IllegalArgumentException("unusable snapshot");
            }
            restores++;
            state.clear();
            String text = new String(bytes);
            if (!text.isEmpty()) {
                state.addAll(List.of(text.split(",")));
            }
        }
    }

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage;
        final ListMachine machine = new ListMachine();
        final List<Sent> sent = new ArrayList<>();
        final RaftNode node;

        Rig(int id, List<Integer> peers, int snapshotEvery, InMemoryRaftStorage storage, RaftConfig.Fault... faults) {
            this.storage = storage;
            node = new RaftNode(id, peers, storage, (to, m) -> sent.add(new Sent(to, m)), machine,
                    RaftConfig.defaults().withCheckQuorum(false).withSnapshotEvery(snapshotEvery).withFaults(faults),
                    new Random(1));
        }

        /** Node 0 of three, elected, with its no-op (index 1) acknowledged by server 1: commit index 1. */
        static Rig leader(int snapshotEvery) {
            Rig rig = new Rig(0, List.of(1, 2), snapshotEvery, new InMemoryRaftStorage());
            for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
                rig.node.tick();
            }
            rig.node.receive(1, new RaftMessage.RequestVoteReply(rig.node.term(), true));
            assertEquals(RaftNode.Role.LEADER, rig.node.role());
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0));
            rig.sent.clear();
            return rig;
        }

        Rig follower(int snapshotEvery) {
            return new Rig(1, List.of(0, 2), snapshotEvery, new InMemoryRaftStorage());
        }

        List<RaftMessage.InstallSnapshot> snapshotsTo(int peer) {
            List<RaftMessage.InstallSnapshot> out = new ArrayList<>();
            for (Sent s : sent) {
                if (s.to() == peer && s.message() instanceof RaftMessage.InstallSnapshot m) {
                    out.add(m);
                }
            }
            return out;
        }

        RaftMessage.AppendEntriesReply lastReply() {
            for (int i = sent.size() - 1; i >= 0; i--) {
                if (sent.get(i).message() instanceof RaftMessage.AppendEntriesReply r) {
                    return r;
                }
            }
            return null;
        }
    }

    private static byte[] cmd(String s) {
        return s.getBytes();
    }

    /** Commits five more commands on a leader (indexes 2..6) and returns it with its log compacted at 6. */
    private static Rig leaderWith5Committed(int snapshotEvery) {
        Rig rig = Rig.leader(snapshotEvery);
        long term = rig.node.term();
        rig.node.proposeBatch(List.of(cmd("a"), cmd("b"), cmd("c"), cmd("d"), cmd("e")));
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(term, true, 6, 0, 0));
        assertEquals(6, rig.node.commitIndex());
        return rig;
    }

    @Test
    void aSnapshotIsTakenOnceEnoughEntriesWereAppliedAndTheLogItCoversIsDropped() {
        Rig rig = leaderWith5Committed(3);
        assertEquals(6, rig.node.snapshotIndex(), "applied 1..6 with a snapshot every 3: at 3, then at 6");
        assertEquals(2, rig.node.snapshotsTaken());
        assertEquals(6, rig.storage.lastIndex());
        assertEquals(6, rig.storage.snapshotIndex());
        assertEquals(rig.node.term(), rig.storage.snapshotTerm());
        assertEquals("2:a,3:b,4:c,5:d,6:e", new String(rig.storage.snapshot().state()),
                "the state as of index 6, not before it");
    }

    @Test
    void theSnapshotIsTakenAfterTheEntryItNamesIsApplied() {
        Rig rig = Rig.leader(1); // a snapshot after every single applied entry
        long term = rig.node.term();
        rig.node.proposeBatch(List.of(cmd("a")));
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(term, true, 2, 0, 0));
        assertEquals(2, rig.node.snapshotIndex());
        assertEquals("2:a", new String(rig.storage.snapshot().state()));
    }

    @Test
    void withSnapshotsOffTheLogOnlyGrows() {
        Rig rig = leaderWith5Committed(0);
        assertEquals(0, rig.node.snapshotIndex());
        assertEquals(0, rig.node.snapshotsTaken());
        assertEquals(6, rig.storage.lastIndex());
        assertEquals(rig.storage.entry(1).term(), rig.storage.termAt(1));
    }

    @Test
    void aRestartRestoresTheSnapshotThenOnlyReplaysWhatCameAfterIt() {
        Rig rig = leaderWith5Committed(3);
        rig.node.proposeBatch(List.of(cmd("f"), cmd("g"))); // 7, 8: in the log, not yet committed here
        Rig restarted = new Rig(0, List.of(1, 2), 3, rig.storage);
        assertEquals(1, restarted.machine.restores);
        assertEquals(List.of("2:a", "3:b", "4:c", "5:d", "6:e"), restarted.machine.state);
        assertEquals(6, restarted.node.commitIndex());
        assertEquals(6, restarted.node.lastApplied());
        assertEquals(8, restarted.storage.lastIndex());
        assertEquals(0, restarted.node.snapshotsTaken(), "restoring is not taking");
    }

    @Test
    void aStateMachineThatCannotRestoreTheSnapshotStopsTheNodeFromStarting() {
        Rig rig = leaderWith5Committed(3);
        ListMachine broken = new ListMachine();
        broken.rejectRestore = true;
        assertThrows(IllegalArgumentException.class, () -> new RaftNode(0, List.of(1, 2), rig.storage,
                (to, m) -> {}, broken, RaftConfig.defaults(), new Random(1)));
    }

    @Test
    void aLeaderSendsItsSnapshotToAFollowerThatNeedsEntriesItNoLongerHas() {
        Rig rig = leaderWith5Committed(3);
        for (int i = 0; i < 20; i++) {
            rig.node.tick(); // heartbeats
        }
        List<RaftMessage.InstallSnapshot> toTwo = rig.snapshotsTo(2);
        assertFalse(toTwo.isEmpty(), "server 2 never acknowledged anything, so only the snapshot can help it");
        RaftMessage.InstallSnapshot m = toTwo.get(0);
        assertEquals(6, m.lastIncludedIndex());
        assertEquals(rig.storage.snapshotTerm(), m.lastIncludedTerm());
        assertEquals("2:a,3:b,4:c,5:d,6:e", new String(m.state()));
        assertTrue(rig.snapshotsTo(1).isEmpty(), "server 1 holds everything up to 6 already");
    }

    @Test
    void theSnapshotIsNotResentEveryHeartbeatWhileOneIsOnItsWay() {
        Rig rig = leaderWith5Committed(3);
        for (int i = 0; i < 4; i++) {
            rig.node.tick();
        }
        int early = rig.snapshotsTo(2).size();
        assertEquals(1, early);
        rig.sent.clear();
        for (int i = 0; i < 200; i++) {
            rig.node.tick();
        }
        int later = rig.snapshotsTo(2).size();
        assertTrue(later > 1 && later < 100, "retried now and then, not at every heartbeat: " + later);
    }

    @Test
    void whenTheFollowerAcknowledgesTheSnapshotTheLeaderContinuesWithEntriesAfterIt() {
        Rig rig = leaderWith5Committed(3);
        for (int i = 0; i < 10; i++) {
            rig.node.tick();
        }
        long term = rig.node.term();
        rig.node.proposeBatch(List.of(cmd("f")));          // index 7
        rig.node.receive(2, new RaftMessage.AppendEntriesReply(term, true, 6, 0, 0)); // ack of the snapshot
        RaftMessage.AppendEntries after = null;
        for (Sent s : rig.sent) {
            if (s.to() == 2 && s.message() instanceof RaftMessage.AppendEntries ae && !ae.entries().isEmpty()) {
                after = ae;
            }
        }
        assertEquals(6, after.prevLogIndex(), "continues right after the snapshot");
        assertEquals(1, after.entries().size());
        assertEquals("f", new String(after.entries().get(0).command()));
    }

    // ------------------------------------------------------------------ followers

    private static RaftMessage.InstallSnapshot snapshotMessage(long term, long index, long snapTerm, String state) {
        return new RaftMessage.InstallSnapshot(term, 0, index, snapTerm, state.getBytes());
    }

    @Test
    void aFollowerInstallsASnapshotAndContinuesFromIt() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, snapshotMessage(1, 10, 1, "1:x,2:y"));
        assertEquals(List.of("1:x", "2:y"), f.machine.state);
        assertEquals(10, f.node.commitIndex());
        assertEquals(10, f.node.lastApplied());
        assertEquals(10, f.storage.snapshotIndex());
        assertEquals(10, f.storage.lastIndex());
        assertEquals(1, f.node.snapshotsInstalled());
        RaftMessage.AppendEntriesReply r = f.lastReply();
        assertTrue(r.success());
        assertEquals(10, r.matchIndex());

        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 10, 1, List.of(new LogEntry(1, cmd("z"))), 11));
        assertEquals(11, f.node.commitIndex());
        assertEquals("11:z", f.machine.state.get(f.machine.state.size() - 1));
    }

    @Test
    void aFollowerKeepsTheEntriesAfterASnapshotThatItsOwnLogAlreadyContains() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0,
                List.of(new LogEntry(1, cmd("a")), new LogEntry(1, cmd("b")), new LogEntry(1, cmd("c")),
                        new LogEntry(1, cmd("d"))), 0));
        f.node.receive(0, snapshotMessage(1, 2, 1, "1:a,2:b"));
        assertEquals(4, f.storage.lastIndex(), "entries 3 and 4 match the leader's history and stay");
        assertEquals(2, f.storage.snapshotIndex());
        assertEquals(2, f.node.commitIndex());
    }

    @Test
    void aSnapshotOlderThanWhatTheFollowerCommittedIsAcknowledgedButNeverInstalled() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0,
                List.of(new LogEntry(1, cmd("a")), new LogEntry(1, cmd("b")), new LogEntry(1, cmd("c"))), 3));
        assertEquals(3, f.node.commitIndex());
        int restores = f.machine.restores;
        f.node.receive(0, snapshotMessage(1, 2, 1, "old"));
        assertEquals(restores, f.machine.restores, "installing it would roll the state machine back");
        assertEquals(3, f.node.commitIndex());
        assertEquals(3, f.machine.state.size());
        assertEquals(0, f.storage.snapshotIndex());
        assertTrue(f.lastReply().success());
        assertEquals(2, f.lastReply().matchIndex());
    }

    @Test
    void aSnapshotFromAnOldTermIsRefused() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, new RaftMessage.AppendEntries(5, 0, 0, 0, List.of(), 0)); // now in term 5
        f.node.receive(0, snapshotMessage(3, 50, 3, "stale-leader"));
        assertEquals(0, f.storage.snapshotIndex());
        assertEquals(0, f.node.commitIndex());
        RaftMessage.AppendEntriesReply r = f.lastReply();
        assertFalse(r.success());
        assertEquals(5, r.term(), "tells the old leader about the newer term");
    }

    @Test
    void aSnapshotThatTheStateMachineRejectsChangesNothingOnDisk() {
        Rig f = Rig.leader(0).follower(0);
        f.machine.rejectRestore = true;
        assertThrows(IllegalArgumentException.class, () -> f.node.receive(0, snapshotMessage(1, 10, 1, "junk")));
        assertEquals(0, f.storage.snapshotIndex());
        assertEquals(0, f.node.commitIndex());
    }

    @Test
    void entriesThatStartBeforeTheFollowersSnapshotAreTrimmedToWhatItDoesNotHave() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, snapshotMessage(1, 5, 1, "1:a"));
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 3; i <= 8; i++) { // entries 3..8, prev = 2: the first three are inside the snapshot
            entries.add(new LogEntry(1, cmd("e" + i)));
        }
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 2, 1, entries, 8));
        assertEquals(8, f.storage.lastIndex());
        assertEquals("e6", new String(f.storage.entry(6).command()), "index 6 is the 4th of the message");
        assertEquals(8, f.node.commitIndex());
        RaftMessage.AppendEntriesReply r = f.lastReply();
        assertTrue(r.success());
        assertEquals(8, r.matchIndex());
    }

    @Test
    void aHeartbeatFromBeforeTheSnapshotIsAnsweredWithoutTouchingTheLog() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, snapshotMessage(1, 5, 1, "1:a"));
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 1, 1, List.of(new LogEntry(1, cmd("old"))), 2));
        assertEquals(5, f.storage.lastIndex());
        assertEquals(5, f.node.commitIndex());
        assertTrue(f.lastReply().success());
    }

    @Test
    void aFollowerThatSnapshotsItselfStillAnswersAConflictSearchCorrectly() {
        Rig f = Rig.leader(0).follower(2);
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0,
                List.of(new LogEntry(1, cmd("a")), new LogEntry(1, cmd("b")), new LogEntry(1, cmd("c"))), 3));
        assertEquals(2, f.node.snapshotIndex(), "snapshot taken after 2 applied entries");
        // a leader whose entry 3 has another term: the follower must report a conflict at 3, not search below 2
        f.node.receive(0, new RaftMessage.AppendEntries(2, 0, 3, 2, List.of(new LogEntry(2, cmd("x"))), 3));
        RaftMessage.AppendEntriesReply r = f.lastReply();
        assertFalse(r.success());
        assertTrue(r.conflictIndex() > f.node.snapshotIndex(), "conflict index " + r.conflictIndex());
    }

    // ------------------------------------------------------------------ elections after compaction

    @Test
    void aNodeWithAnEmptyLogAfterItsSnapshotStillVotesBySnapshotIndexAndTerm() {
        Rig f = Rig.leader(0).follower(0);
        f.node.receive(0, snapshotMessage(2, 10, 2, "1:a"));
        f.sent.clear();
        // candidate with an older log (last term 1) must be refused; one with last term 2, index 10 is acceptable
        f.node.receive(2, new RaftMessage.RequestVote(3, 2, 50, 1));
        assertFalse(((RaftMessage.RequestVoteReply) f.sent.get(0).message()).voteGranted());
        f.node.receive(2, new RaftMessage.RequestVote(4, 2, 10, 2));
        assertTrue(((RaftMessage.RequestVoteReply) f.sent.get(1).message()).voteGranted());
    }

    @Test
    void theBrokenFaultsActuallyBreakThings() {
        // a snapshot taken before applying the entry it names holds the wrong state (guards the "after apply" order)
        Rig bad = new Rig(0, List.of(1, 2), 1, new InMemoryRaftStorage(), RaftConfig.Fault.SNAPSHOT_BEFORE_APPLY);
        for (int i = 0; i < 1000 && bad.node.role() != RaftNode.Role.CANDIDATE; i++) {
            bad.node.tick();
        }
        bad.node.receive(1, new RaftMessage.RequestVoteReply(bad.node.term(), true));
        bad.node.receive(1, new RaftMessage.AppendEntriesReply(bad.node.term(), true, 1, 0, 0));
        bad.node.proposeBatch(List.of(cmd("a")));
        bad.node.receive(1, new RaftMessage.AppendEntriesReply(bad.node.term(), true, 2, 0, 0));
        assertEquals("", new String(bad.storage.snapshot().state()), "the fault drops the last applied command");
    }

    @Test
    void noSnapshotMeansNothingIsSentOrRestored() {
        Rig rig = Rig.leader(0);
        assertNull(rig.storage.snapshot());
        assertEquals(0, rig.node.snapshotsInstalled());
    }
}
