package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** A snapshot is sent in chunks, one in flight at a time; how the leader paces them and how a follower assembles them. */
class ChunkedSnapshotTest {

    /** The state is a byte array set by the test; applying commands appends them. */
    private static final class BytesMachine implements StateMachine {
        byte[] state = new byte[0];
        int restores;
        boolean rejectRestore;

        @Override
        public void apply(long index, byte[] command) {
            byte[] next = Arrays.copyOf(state, state.length + command.length);
            System.arraycopy(command, 0, next, state.length, command.length);
            state = next;
        }

        @Override
        public byte[] snapshot() {
            return state.clone();
        }

        @Override
        public void restore(byte[] bytes) {
            if (rejectRestore) {
                throw new IllegalArgumentException("unusable snapshot");
            }
            restores++;
            state = bytes.clone();
        }
    }

    private record Sent(int to, RaftMessage message) {}

    private static final class Rig {
        final InMemoryRaftStorage storage = new InMemoryRaftStorage();
        final BytesMachine machine = new BytesMachine();
        final List<Sent> sent = new ArrayList<>();
        final RaftNode node;

        Rig(int id, List<Integer> peers, int chunk, RaftConfig.Fault... faults) {
            node = new RaftNode(id, peers, storage, (to, m) -> sent.add(new Sent(to, m)), machine,
                    RaftConfig.defaults().withCheckQuorum(false).withSnapshotEvery(1).withSnapshotChunkBytes(chunk)
                            .withFaults(faults),
                    new Random(1));
        }

        /** A leader with a snapshot at index 2 whose state is {@code payload}; server 2 has never acknowledged anything. */
        static Rig leaderWithSnapshot(byte[] payload, int chunk) {
            Rig rig = new Rig(0, List.of(1, 2), chunk);
            for (int i = 0; i < 1000 && rig.node.role() != RaftNode.Role.CANDIDATE; i++) {
                rig.node.tick();
            }
            rig.node.receive(1, new RaftMessage.RequestVoteReply(rig.node.term(), true));
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 1, 0, 0)); // the no-op
            rig.node.proposeBatch(List.of(payload));
            rig.node.receive(1, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 2, 0, 0));
            assertEquals(2, rig.node.snapshotIndex());
            rig.sent.clear();
            return rig;
        }

        List<RaftMessage.InstallSnapshot> chunksTo(int peer) {
            List<RaftMessage.InstallSnapshot> out = new ArrayList<>();
            for (Sent s : sent) {
                if (s.to() == peer && s.message() instanceof RaftMessage.InstallSnapshot m) {
                    out.add(m);
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

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) ('a' + i % 26);
        }
        return b;
    }

    private static void tick(Rig rig, int times) {
        for (int i = 0; i < times; i++) {
            rig.node.tick();
        }
    }

    private static RaftMessage.SnapshotChunkReply ack(Rig rig, long next) {
        return new RaftMessage.SnapshotChunkReply(rig.node.term(), 2, next);
    }

    // ------------------------------------------------------------------ leader

    @Test
    void theLeaderSendsOneChunkAndWaitsForItsAcknowledgement() {
        Rig rig = Rig.leaderWithSnapshot(bytes(20), 8);
        tick(rig, 2); // a heartbeat
        List<RaftMessage.InstallSnapshot> chunks = rig.chunksTo(2);
        assertEquals(1, chunks.size());
        RaftMessage.InstallSnapshot first = chunks.get(0);
        assertEquals(0, first.offset());
        assertEquals(8, first.data().length);
        assertFalse(first.done());
        rig.sent.clear();
        tick(rig, 3); // more heartbeats, but the first chunk is still unacknowledged and recent
        assertTrue(rig.chunksTo(2).isEmpty(), "no second chunk before the first is acknowledged");
    }

    @Test
    void eachAcknowledgementReleasesTheNextChunkAndTheChunksAddUpToTheState() {
        byte[] payload = bytes(20);
        Rig rig = Rig.leaderWithSnapshot(payload, 8);
        tick(rig, 2);
        byte[] state = rig.storage.snapshot().state();
        java.io.ByteArrayOutputStream got = new java.io.ByteArrayOutputStream();
        RaftMessage.InstallSnapshot chunk = rig.chunksTo(2).get(0);
        int count = 0;
        while (true) {
            count++;
            assertEquals(got.size(), chunk.offset(), "chunks arrive in order, without gaps");
            got.writeBytes(chunk.data());
            if (chunk.done()) {
                break;
            }
            rig.sent.clear();
            rig.node.receive(2, ack(rig, got.size()));
            List<RaftMessage.InstallSnapshot> next = rig.chunksTo(2);
            assertEquals(1, next.size(), "an acknowledgement releases exactly one chunk");
            chunk = next.get(0);
        }
        assertArrayEquals(state, got.toByteArray());
        assertEquals((state.length + 7) / 8, count);
        assertTrue(chunk.done());
    }

    @Test
    void whenTheStateIsAMultipleOfTheChunkSizeThereIsNoEmptyExtraChunk() {
        Rig rig = Rig.leaderWithSnapshot(bytes(20), 5);
        int total = rig.storage.snapshot().state().length;
        assertEquals(0, total % 5, "the test needs a state that is a multiple of the chunk size, got " + total);
        tick(rig, 2);
        RaftMessage.InstallSnapshot chunk = rig.chunksTo(2).get(0);
        int count = 1;
        long offset = 0;
        while (!chunk.done()) {
            offset += chunk.data().length;
            rig.sent.clear();
            rig.node.receive(2, ack(rig, offset));
            chunk = rig.chunksTo(2).get(0);
            count++;
            assertFalse(chunk.data().length == 0, "no empty chunk");
        }
        assertEquals(total / 5, count);
    }

    @Test
    void aDuplicateAcknowledgementDoesNotSendAnotherCopyOfTheNextChunk() {
        Rig rig = Rig.leaderWithSnapshot(bytes(40), 8);
        tick(rig, 2);
        rig.sent.clear();
        rig.node.receive(2, ack(rig, 8));
        assertEquals(1, rig.chunksTo(2).size());
        rig.sent.clear();
        rig.node.receive(2, ack(rig, 8)); // the network delivered the same acknowledgement twice
        assertTrue(rig.chunksTo(2).isEmpty(), "a duplicate must not multiply the chunks in flight");
    }

    @Test
    void aLostChunkIsSentAgainAfterAWhileButNotAtOnce() {
        Rig rig = Rig.leaderWithSnapshot(bytes(40), 8);
        tick(rig, 2);
        assertEquals(1, rig.chunksTo(2).size());
        rig.sent.clear();
        tick(rig, 2);
        assertTrue(rig.chunksTo(2).isEmpty(), "not resent immediately");
        tick(rig, 60);
        List<RaftMessage.InstallSnapshot> again = rig.chunksTo(2);
        assertFalse(again.isEmpty(), "resent after the retry interval");
        assertEquals(0, again.get(0).offset(), "the same chunk");
        assertTrue(again.size() < 30, "retried now and then, not at every heartbeat: " + again.size());
    }

    @Test
    void aFollowerThatLostBytesIsServedFromWhereItSaysItIsAfterTheRetryInterval() {
        Rig rig = Rig.leaderWithSnapshot(bytes(40), 8);
        tick(rig, 2);
        rig.node.receive(2, ack(rig, 8));
        rig.node.receive(2, ack(rig, 16)); // leader is now at offset 16
        rig.sent.clear();
        rig.node.receive(2, ack(rig, 8));  // but the follower reports it only holds 8 (a gap)
        assertTrue(rig.chunksTo(2).isEmpty(), "a reply below the acknowledged point is not acted on at once");
        tick(rig, 60);
        List<RaftMessage.InstallSnapshot> again = rig.chunksTo(2);
        assertFalse(again.isEmpty());
        assertEquals(8, again.get(0).offset(), "continues from what the follower holds");
    }

    @Test
    void aReplyForAnotherSnapshotOrAnImpossibleOffsetIsIgnored() {
        Rig rig = Rig.leaderWithSnapshot(bytes(40), 8);
        tick(rig, 2);
        rig.sent.clear();
        rig.node.receive(2, new RaftMessage.SnapshotChunkReply(rig.node.term(), 999, 8)); // other snapshot
        rig.node.receive(2, ack(rig, 10_000));                                             // beyond the state
        rig.node.receive(1, new RaftMessage.SnapshotChunkReply(rig.node.term(), 2, 8));    // no transfer to server 1
        assertTrue(rig.chunksTo(2).isEmpty() && rig.chunksTo(1).isEmpty());
    }

    @Test
    void whenTheFinalChunkIsAcknowledgedTheLeaderGoesBackToEntries() {
        Rig rig = Rig.leaderWithSnapshot(bytes(10), 8);
        tick(rig, 2);
        rig.node.receive(2, ack(rig, 8));
        long term = rig.node.term();
        rig.node.receive(2, new RaftMessage.AppendEntriesReply(term, true, 2, 0, 0)); // the final chunk's reply
        rig.node.proposeBatch(List.of("z".getBytes()));
        boolean sawEntries = false;
        for (Sent s : rig.sent) {
            if (s.to() == 2 && s.message() instanceof RaftMessage.AppendEntries ae && !ae.entries().isEmpty()) {
                sawEntries = true;
                assertEquals(2, ae.prevLogIndex());
            }
        }
        assertTrue(sawEntries);
    }

    @Test
    void aNewerSnapshotTakenWhileSendingRestartsTheTransferFromTheStart() {
        Rig rig = Rig.leaderWithSnapshot(bytes(40), 8);
        tick(rig, 2);
        rig.node.receive(2, ack(rig, 8));
        rig.node.receive(2, ack(rig, 16));
        long term = rig.node.term();
        rig.node.proposeBatch(List.of("more".getBytes())); // index 3, snapshot taken at 3 (snapshotEvery = 1)
        rig.node.receive(1, new RaftMessage.AppendEntriesReply(term, true, 3, 0, 0));
        assertEquals(3, rig.node.snapshotIndex());
        rig.sent.clear();
        tick(rig, 60);
        List<RaftMessage.InstallSnapshot> chunks = rig.chunksTo(2);
        assertFalse(chunks.isEmpty());
        assertEquals(3, chunks.get(0).lastIncludedIndex(), "the new snapshot, not the old one");
        assertEquals(0, chunks.get(0).offset(), "from the start: its bytes are not the old snapshot's bytes");
    }

    // ------------------------------------------------------------------ follower

    private static RaftMessage.InstallSnapshot chunk(long term, int leader, long index, long offset, byte[] all,
            int from, int to) {
        return new RaftMessage.InstallSnapshot(term, leader, index, 1, offset, Arrays.copyOfRange(all, from, to),
                to == all.length);
    }

    private static Rig follower(RaftConfig.Fault... faults) {
        return new Rig(1, List.of(0, 2), 1_000, faults);
    }

    @Test
    void aFollowerInstallsOnlyWhenTheLastChunkArrivesAndRepliesToEachChunk() {
        byte[] all = bytes(20);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 8), f.lastTo(0));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 16), f.lastTo(0));
        assertEquals(0, f.machine.restores, "nothing installed before the last chunk");
        assertEquals(0, f.storage.snapshotIndex());
        assertEquals(0, f.node.commitIndex());
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 20));
        assertEquals(1, f.machine.restores);
        assertArrayEquals(all, f.machine.state);
        assertEquals(10, f.storage.snapshotIndex());
        assertEquals(10, f.node.commitIndex());
        assertEquals(10, f.node.lastApplied());
        RaftMessage.AppendEntriesReply done = (RaftMessage.AppendEntriesReply) f.lastTo(0);
        assertTrue(done.success());
        assertEquals(10, done.matchIndex());
        assertEquals(3, f.node.snapshotChunksReceived());
    }

    @Test
    void aDuplicateChunkChangesNothing() {
        byte[] all = bytes(20);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));   // the first chunk again, late
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 16), f.lastTo(0), "still holds 16: tells the leader so");
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));  // and the second again
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 20));
        assertTrue(Arrays.equals(all, f.machine.state), "duplicates must not be appended twice");
    }

    @Test
    void withoutOffsetCheckingADuplicateCorruptsTheSnapshot() {
        // the injected bug: this is what the previous test protects against
        byte[] all = bytes(20);
        Rig f = follower(RaftConfig.Fault.SNAPSHOT_CHUNK_IGNORE_OFFSET);
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 20));
        assertTrue(all.length != f.machine.state.length, "the injected bug appends the duplicate");
    }

    @Test
    void aGapIsNotAppendedAndTheLeaderIsToldWhereTheNextByteBelongs() {
        byte[] all = bytes(30);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 24)); // chunk 2 lost
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 8), f.lastTo(0));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 24));
        f.node.receive(0, chunk(1, 0, 10, 24, all, 24, 30));
        assertArrayEquals(all, f.machine.state);
    }

    @Test
    void aChunkInTheMiddleOfNothingAsksForTheStart() {
        byte[] all = bytes(30);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 0), f.lastTo(0));
        assertEquals(0, f.machine.restores);
    }

    @Test
    void piecesOfTwoLeadersSnapshotsAreNeverMixed() {
        // Two servers' snapshots of the same log index can differ byte for byte (expiry times, for example)
        byte[] fromA = bytes(20);
        byte[] fromB = new byte[20];
        Arrays.fill(fromB, (byte) 'Z');
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, fromA, 0, 8));
        // a new leader (server 2, term 2) continues "the same" snapshot index at offset 8: it must be refused
        f.node.receive(2, chunk(2, 2, 10, 8, fromB, 8, 16));
        assertEquals(new RaftMessage.SnapshotChunkReply(2, 10, 0), f.lastTo(2));
        f.node.receive(2, chunk(2, 2, 10, 0, fromB, 0, 8));
        f.node.receive(2, chunk(2, 2, 10, 8, fromB, 8, 16));
        f.node.receive(2, chunk(2, 2, 10, 16, fromB, 16, 20));
        assertTrue(Arrays.equals(fromB, f.machine.state), "only the second leader's bytes");
    }

    @Test
    void aSnapshotThatIsAlreadyCoveredIsAcknowledgedAndDropped() {
        byte[] all = bytes(20);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 20));        // installed: commit index 10
        f.node.receive(0, chunk(1, 0, 5, 0, all, 0, 8));          // an old, partial one for index 5
        RaftMessage.AppendEntriesReply r = (RaftMessage.AppendEntriesReply) f.lastTo(0);
        assertTrue(r.success());
        assertEquals(5, r.matchIndex());
        assertEquals(1, f.machine.restores, "not installed again");
        assertEquals(10, f.node.commitIndex());
    }

    @Test
    void anUnusableSnapshotIsRefusedWithoutChangingAnythingAndALaterTransferStillWorks() {
        byte[] all = bytes(20);
        Rig f = follower();
        f.machine.rejectRestore = true;
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        assertThrows(IllegalArgumentException.class, () -> f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 20)));
        assertEquals(0, f.storage.snapshotIndex());
        assertEquals(0, f.node.commitIndex());
        f.machine.rejectRestore = false;
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));         // the leader starts over
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 20));
        assertArrayEquals(all, f.machine.state);
    }

    @Test
    void aStaleLeadersChunkIsRefused() {
        Rig f = follower();
        f.node.receive(2, new RaftMessage.RequestVote(5, 2, 0, 0)); // the follower is now in term 5
        f.node.receive(0, chunk(3, 0, 10, 0, bytes(10), 0, 10));
        RaftMessage.AppendEntriesReply r = (RaftMessage.AppendEntriesReply) f.lastTo(0);
        assertFalse(r.success());
        assertEquals(0, f.machine.restores);
    }

    @Test
    void aStrayChunkOfAnotherSnapshotDoesNotWipeTheTransferInProgress() {
        byte[] all = bytes(30);
        byte[] other = bytes(30);
        Rig f = follower();
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        // a delayed chunk of an older snapshot (index 8) of the same leader: refused, and nothing already held is lost
        f.node.receive(0, chunk(1, 0, 8, 8, other, 8, 16));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 8, 0), f.lastTo(0));
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 24));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 24), f.lastTo(0), "the transfer in progress carried on");
    }

    @Test
    void aSnapshotLargerThanTheNodeAcceptsIsRefusedAndNothingIsInstalled() {
        byte[] all = bytes(30);
        Rig f = follower();
        f.node.maxSnapshotBytes = 20;
        f.node.receive(0, chunk(1, 0, 10, 0, all, 0, 8));
        f.node.receive(0, chunk(1, 0, 10, 8, all, 8, 16));
        f.sent.clear();
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 30)); // would make 30 bytes, over the limit of 20
        assertEquals(0, f.machine.restores);
        assertEquals(0, f.storage.snapshotIndex());
        assertEquals(16, f.node.snapshotChunksReceived() * 8, "the oversized chunk was not counted");
        // and the transfer was dropped, so the next chunk is told to start again
        f.node.receive(0, chunk(1, 0, 10, 16, all, 16, 24));
        assertEquals(new RaftMessage.SnapshotChunkReply(1, 10, 0), f.lastTo(0));
    }

    @Test
    void aFinishedTransferIsNotHeldInMemoryAnyLonger() {
        Rig rig = Rig.leaderWithSnapshot(bytes(10), 8);
        tick(rig, 2);
        assertEquals(1, rig.node.snapshotTransfersInProgress());
        rig.node.receive(2, ack(rig, 8));
        rig.node.receive(2, new RaftMessage.AppendEntriesReply(rig.node.term(), true, 2, 0, 0));
        assertEquals(0, rig.node.snapshotTransfersInProgress(), "the snapshot bytes can be released");
    }

    // ------------------------------------------------------------------ larger than the old single-message limit

    @Test
    void aSnapshotLargerThanTheOldSingleMessageLimitIsTransferredInChunks() {
        int size = RaftStorage.MAX_SNAPSHOT_CHUNK_BYTES * 8 + 12_345; // about 33.5 MiB: over the old 32 MiB limit
        assertTrue(size > 32 * 1024 * 1024);
        byte[] payload = new byte[size];
        new Random(5).nextBytes(payload);
        int chunkBytes = RaftConfig.DEFAULT_SNAPSHOT_CHUNK_BYTES;
        Rig leader = Rig.leaderWithSnapshot(payload, chunkBytes);
        Rig f = new Rig(2, List.of(0, 1), chunkBytes);
        tick(leader, 2);
        int chunks = 0;
        RaftMessage.InstallSnapshot c = leader.chunksTo(2).get(0);
        while (true) {
            chunks++;
            assertTrue(c.data().length <= chunkBytes);
            leader.sent.clear();
            f.sent.clear();
            f.node.receive(0, c);
            RaftMessage reply = f.lastTo(0);
            if (c.done()) {
                assertTrue(reply instanceof RaftMessage.AppendEntriesReply r && r.success());
                break;
            }
            leader.node.receive(2, reply);
            c = leader.chunksTo(2).get(0);
        }
        assertEquals((size + chunkBytes - 1) / chunkBytes, chunks);
        assertArrayEquals(payload, f.machine.state);
        assertEquals(2, f.storage.snapshotIndex());
    }
}
