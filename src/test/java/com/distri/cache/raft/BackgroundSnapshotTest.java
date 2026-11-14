package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * A snapshot taken through a {@link RaftNode.SnapshotWorker}: the state is frozen when the snapshot is due, the encoding and
 * the write happen elsewhere, and the result is committed back on the node's thread. The worker here is manual, so each test
 * decides exactly when the background work runs and when its result arrives.
 */
class BackgroundSnapshotTest {

    /** State = the list of commands applied; capturing copies the list, serializing joins it. */
    private static final class ListMachine implements StateMachine {
        final List<String> state = new ArrayList<>();
        boolean failSerialize;
        int serializeCalls;

        @Override
        public void apply(long index, byte[] command) {
            state.add(index + ":" + new String(command));
        }

        @Override
        public SnapshotCapture captureSnapshot() {
            List<String> frozen = new ArrayList<>(state);
            return () -> {
                serializeCalls++;
                if (failSerialize) {
                    throw new IllegalStateException("disk full");
                }
                return String.join(",", frozen).getBytes();
            };
        }

        @Override
        public byte[] snapshot() {
            return String.join(",", state).getBytes();
        }

        @Override
        public void restore(byte[] bytes) {
            state.clear();
            String text = new String(bytes);
            if (!text.isEmpty()) {
                state.addAll(List.of(text.split(",")));
            }
        }
    }

    /** Collects submitted jobs; the test runs the work and delivers the result when it chooses. */
    private static final class ManualWorker implements RaftNode.SnapshotWorker {
        record Job(Supplier<RaftNode.SnapshotOutcome> work, Consumer<RaftNode.SnapshotOutcome> done) {}

        final ArrayDeque<Job> queue = new ArrayDeque<>();
        int submitted;

        @Override
        public void submit(Supplier<RaftNode.SnapshotOutcome> work, Consumer<RaftNode.SnapshotOutcome> done) {
            submitted++;
            queue.add(new Job(work, done));
        }

        /** Runs the oldest job's work, but does not deliver the result yet. */
        RaftNode.SnapshotOutcome runWork() {
            return queue.peek().work().get();
        }

        /** Runs the oldest job's work and delivers its result, as the node's thread would. */
        void finishOldest() {
            Job job = queue.poll();
            job.done().accept(job.work().get());
        }
    }

    private static final class Rig {
        final InMemoryRaftStorage storage = new InMemoryRaftStorage();
        final ListMachine machine = new ListMachine();
        final ManualWorker worker = new ManualWorker();
        final RaftNode node;

        Rig(int id, List<Integer> peers, int snapshotEvery) {
            node = new RaftNode(id, peers, storage, (to, m) -> { }, machine,
                    RaftConfig.defaults().withCheckQuorum(false).withSnapshotEvery(snapshotEvery), new Random(1));
            node.setSnapshotWorker(worker);
        }

        /** Node 0 of three, elected; its no-op (index 1) is committed and applied. */
        static Rig leader(int snapshotEvery) {
            Rig rig = new Rig(0, List.of(1, 2), snapshotEvery);
            for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
                rig.node.tick();
            }
            rig.node.receive(1, new RaftMessage.RequestVoteReply(rig.node.term(), true));
            assertEquals(RaftNode.Role.LEADER, rig.node.role());
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0));
            return rig;
        }

        void commit(String... commands) {
            List<byte[]> batch = new ArrayList<>();
            for (String c : commands) {
                batch.add(c.getBytes());
            }
            node.proposeBatch(batch);
            node.receive(1, new RaftMessage.AppendEntriesReply(node.term(), true, node.commitIndex() + commands.length, 0, 0));
        }
    }

    @Test
    void theSnapshotIsOnlyInstalledWhenTheResultIsDelivered() {
        Rig rig = Rig.leader(3);
        rig.commit("a", "b"); // applied 1..3: a snapshot is due at 3
        assertTrue(rig.node.snapshotInFlight());
        assertEquals(1, rig.worker.submitted);
        assertEquals(0, rig.node.snapshotIndex(), "nothing installed yet");
        assertEquals(0, rig.storage.snapshotIndex());
        assertEquals(3, rig.storage.lastIndex());
        assertEquals(0, rig.machine.serializeCalls, "encoding has not run: it happens in the background");
        rig.worker.finishOldest();
        assertFalse(rig.node.snapshotInFlight());
        assertEquals(3, rig.node.snapshotIndex());
        assertEquals(1, rig.node.snapshotsTaken());
    }

    @Test
    void theSnapshotHoldsTheStateAtTheMomentItWasDueNotTheStateWhenItIsFinished() {
        Rig rig = Rig.leader(3);
        rig.commit("a", "b");            // due at 3: captures 2:a,3:b
        rig.commit("c", "d", "e");       // applied meanwhile: 4,5,6
        assertEquals(6, rig.node.lastApplied());
        rig.worker.finishOldest();
        assertEquals(3, rig.storage.snapshotIndex());
        assertEquals("2:a,3:b", new String(rig.storage.snapshot().state()), "c, d and e are not in it");
        assertEquals(6, rig.storage.lastIndex(), "the entries after the snapshot stay in the log");
        assertEquals("c", new String(rig.storage.entry(4).command()));
    }

    @Test
    void onlyOneSnapshotIsPreparedAtATimeAndTheNextOneStartsAfterwards() {
        Rig rig = Rig.leader(3);
        rig.commit("a", "b");
        rig.commit("c", "d", "e"); // several applies were "due" while one was running
        assertEquals(1, rig.worker.submitted);
        rig.worker.finishOldest();
        rig.commit("f");           // apply 7: 7 - 3 >= 3
        assertEquals(2, rig.worker.submitted);
        assertTrue(rig.node.snapshotInFlight());
        rig.worker.finishOldest();
        assertEquals(7, rig.node.snapshotIndex());
        assertEquals("2:a,3:b,4:c,5:d,6:e,7:f", new String(rig.storage.snapshot().state()));
    }

    @Test
    void aSnapshotInstalledFromTheLeaderMeanwhileMakesTheBackgroundOneObsolete() {
        Rig f = new Rig(1, List.of(0, 2), 3);
        f.node.receive(0, new RaftMessage.AppendEntries(1, 0, 0, 0,
                List.of(new LogEntry(1, "a".getBytes()), new LogEntry(1, "b".getBytes()), new LogEntry(1, "c".getBytes())), 3));
        assertTrue(f.node.snapshotInFlight());
        f.node.receive(0, new RaftMessage.InstallSnapshot(1, 0, 10, 1, "1:x,2:y".getBytes())); // a far newer snapshot
        assertEquals(10, f.node.snapshotIndex());
        long taken = f.node.snapshotsTaken();
        f.worker.finishOldest();
        assertEquals(10, f.node.snapshotIndex(), "the older background snapshot did not replace it");
        assertEquals("1:x,2:y", new String(f.storage.snapshot().state()));
        assertEquals(taken, f.node.snapshotsTaken(), "and it is not counted");
        assertEquals(List.of("1:x", "2:y"), f.machine.state);
    }

    @Test
    void aFailureInTheBackgroundIsRaisedOnTheNodesThreadAndNothingIsInstalled() {
        Rig rig = Rig.leader(3);
        rig.machine.failSerialize = true;
        rig.commit("a", "b");
        RuntimeException e = assertThrows(IllegalStateException.class, () -> rig.worker.finishOldest());
        assertEquals("background snapshot failed", e.getMessage());
        assertEquals(0, rig.storage.snapshotIndex());
        assertEquals(3, rig.storage.lastIndex(), "the log was not touched");
    }

    @Test
    void aStateTooLargeToSendIsSkippedAndTheLogIsKept() {
        Rig rig = Rig.leader(3);
        rig.node.maxSnapshotBytes = 5;
        rig.commit("a", "b");
        rig.worker.finishOldest();
        assertEquals(1, rig.node.snapshotsSkipped());
        assertEquals(0, rig.node.snapshotsTaken());
        assertEquals(0, rig.storage.snapshotIndex());
        assertFalse(rig.node.snapshotInFlight(), "the next snapshot may be tried later");
    }

    @Test
    void aNodeRestartedWhileASnapshotWasBeingPreparedLosesNothing() {
        Rig rig = Rig.leader(3);
        rig.commit("a", "b");
        assertTrue(rig.node.snapshotInFlight()); // ...and the server dies here; its result is never delivered
        RaftNode restarted = new RaftNode(0, List.of(1, 2), rig.storage, (to, m) -> { }, new ListMachine(),
                RaftConfig.defaults().withCheckQuorum(false).withSnapshotEvery(3), new Random(2));
        assertEquals(0, restarted.snapshotIndex());
        assertEquals(3, rig.storage.lastIndex(), "the whole log is still there to replay");
        assertFalse(restarted.snapshotInFlight());
    }

    @Test
    void theSnapshotTermIsTheTermOfTheEntryItCovers() {
        Rig rig = Rig.leader(3);
        rig.commit("a", "b");
        rig.worker.finishOldest();
        assertEquals(rig.node.term(), rig.storage.snapshotTerm());
    }
}
