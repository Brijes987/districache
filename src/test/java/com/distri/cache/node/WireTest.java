package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.raft.LogEntry;
import com.distri.cache.raft.RaftMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** The byte formats: Raft messages, frames, the client protocol, the handshake. */
class WireTest {

    private static void roundTrip(RaftMessage message) throws IOException {
        assertEquals(message, RaftMessageCodec.decode(RaftMessageCodec.encode(message)));
    }

    @Test
    void everyRaftMessageSurvivesTheWire() throws IOException {
        roundTrip(new RaftMessage.RequestVote(7, 2, 123_456_789_012L, 6));
        roundTrip(new RaftMessage.RequestVoteReply(7, true));
        roundTrip(new RaftMessage.RequestVoteReply(1, false));
        roundTrip(new RaftMessage.AppendEntries(9, 1, 0, 0, List.of(), 0));
        roundTrip(new RaftMessage.AppendEntries(9, 1, 41, 8,
                List.of(new LogEntry(8, "a".getBytes()), LogEntry.noop(9), new LogEntry(9, new byte[1000])), 40));
        roundTrip(new RaftMessage.AppendEntriesReply(9, true, 44, 0, 0));
        roundTrip(new RaftMessage.AppendEntriesReply(9, false, 0, 17, 5));
    }

    @Test
    void randomMessagesSurviveTheWire() throws IOException {
        Random random = new Random(3);
        for (int i = 0; i < 500; i++) {
            List<LogEntry> entries = new java.util.ArrayList<>();
            for (int e = random.nextInt(6); e > 0; e--) {
                byte[] command = new byte[random.nextInt(50)];
                random.nextBytes(command);
                entries.add(new LogEntry(1 + random.nextInt(100), command));
            }
            roundTrip(new RaftMessage.AppendEntries(1 + random.nextInt(1000), random.nextInt(5),
                    random.nextInt(10_000), random.nextInt(100), entries, random.nextInt(10_000)));
        }
    }

    @Test
    void malformedMessagesAreRejectedNotCrashedOn() {
        byte[] valid = RaftMessageCodec.encode(new RaftMessage.AppendEntries(3, 1, 0, 0,
                List.of(new LogEntry(3, "x".getBytes())), 0));
        for (int cut = 0; cut < valid.length; cut++) {
            byte[] truncated = java.util.Arrays.copyOf(valid, cut);
            assertThrows(IOException.class, () -> RaftMessageCodec.decode(truncated), "cut at " + cut);
        }
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(java.util.Arrays.copyOf(valid, valid.length + 1)));
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(new byte[] {99}));
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(new byte[0]));
        // an entry count far larger than the data could hold
        byte[] huge = valid.clone();
        huge[1 + 8 + 4 + 8 + 8 + 8 + 8] = 0x7f; // type, term, leader, prevIndex, prevTerm, commit, readRound, then the count
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(huge));
        // term 0 is not a valid term
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.RequestVoteReply(0, true))));
    }

    @Test
    void randomBytesNeverCrashTheDecoderWithAnythingButIOException() {
        Random random = new Random(11);
        for (int i = 0; i < 20_000; i++) {
            byte[] junk = new byte[random.nextInt(60)];
            random.nextBytes(junk);
            if (junk.length > 0 && random.nextBoolean()) {
                junk[0] = (byte) (1 + random.nextInt(4));
            }
            try {
                RaftMessageCodec.decode(junk);
            } catch (IOException expected) {
                // fine
            }
        }
    }

    @Test
    void framesRoundTripAndRefuseAbsurdLengths() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Frames.write(new DataOutputStream(bytes), "hello".getBytes());
        Frames.write(new DataOutputStream(bytes), new byte[0]);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        assertEquals("hello", new String(Frames.read(in)));
        assertEquals(0, Frames.read(in).length);

        byte[] negative = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        assertThrows(IOException.class, () -> Frames.read(new DataInputStream(new ByteArrayInputStream(negative))));
        byte[] twoGigabytes = {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        assertThrows(IOException.class, () -> Frames.read(new DataInputStream(new ByteArrayInputStream(twoGigabytes))));
        byte[] fiveHundred = {0, 0, 1, (byte) 0xf4};
        assertThrows(IOException.class,
                () -> Frames.read(new DataInputStream(new ByteArrayInputStream(fiveHundred)), 256),
                "a stranger's first frame is limited to a few hundred bytes");
    }

    @Test
    void clientMessagesRoundTripAndBadOnesAreRejected() throws IOException {
        ClientProtocol.Request request = new ClientProtocol.Request(ClientProtocol.SET, 99, 4, "kéy", "välue ☃", 500);
        assertEquals(request, ClientProtocol.decodeRequest(ClientProtocol.encode(request)));
        ClientProtocol.Response response = new ClientProtocol.Response(ClientProtocol.NOT_LEADER, false, "", "h:1", "");
        assertEquals(response, ClientProtocol.decodeResponse(ClientProtocol.encode(response)));

        byte[] valid = ClientProtocol.encode(request);
        for (int cut = 0; cut < valid.length; cut++) {
            byte[] truncated = java.util.Arrays.copyOf(valid, cut);
            assertThrows(IOException.class, () -> ClientProtocol.decodeRequest(truncated));
        }
        assertThrows(IOException.class, () -> ClientProtocol.decodeRequest(ClientProtocol.encode(
                new ClientProtocol.Request((byte) 42, 1, 1, "k", "", -1))));
        assertThrows(IOException.class, () -> ClientProtocol.decodeRequest(ClientProtocol.encode(
                new ClientProtocol.Request(ClientProtocol.GET, 1, 0, "k", "", -1))), "sequence numbers start at 1");
    }

    // ---------------------------------------------------------------- handshake

    /** Runs both sides of a handshake over in-memory pipes. Returns null on success or the acceptor's failure. */
    private static IOException handshake(byte[] serverSecret, byte[] clientSecret) throws Exception {
        PipedOutputStream clientToServer = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientToServer, 4096);
        PipedOutputStream serverToClient = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverToClient, 4096);
        IOException[] serverResult = new IOException[1];
        IOException[] clientResult = new IOException[1];
        Thread server = new Thread(() -> {
            try {
                Handshake.accept(new DataInputStream(serverIn), new DataOutputStream(serverToClient), serverSecret, "r", 3);
            } catch (IOException e) {
                serverResult[0] = e;
            }
        });
        Thread client = new Thread(() -> {
            try {
                Handshake.connect(new DataInputStream(clientIn), new DataOutputStream(clientToServer), clientSecret, "r", 3);
            } catch (IOException e) {
                clientResult[0] = e;
            }
        });
        server.start();
        client.start();
        server.join(5_000);
        client.join(5_000);
        return serverResult[0] != null ? serverResult[0] : clientResult[0];
    }

    @Test
    void theHandshakeAcceptsTheRightSecretAndRefusesWrongOrMissingOnes() throws Exception {
        byte[] secret = "correct horse".getBytes();
        assertEquals(null, handshake(secret, secret));
        assertEquals(null, handshake(null, null));
        assertEquals(null, handshake(null, secret), "a client with a secret is fine if none is required");
        assertTrue(handshake(secret, "wrong".getBytes()).getMessage().contains("handshake failed"));
        assertTrue(handshake(secret, null).getMessage().contains("secret"));
    }

    // ---------------------------------------------------------------- command line

    @Test
    void theMembersListIsParsedStrictly() {
        var members = ReplicatedServerMain.parseMembers("0=10.0.0.1:7000:7100, 1=host:7001:7101,2=h:7002:7102");
        assertEquals(3, members.size());
        assertEquals(new ReplicatedServer.Member("10.0.0.1", 7000, 7100), members.get(0));
        assertEquals(new ReplicatedServer.Member("host", 7001, 7101), members.get(1));
        for (String bad : new String[] {"", "0", "0=host", "0=host:1", "0=host:1:2:3", "x=h:1:2", "0=h:a:2", "0=h:0:2",
                "0=h:70000:2", "0=h:5:5", "0=:1:2", "0=h:1:2,0=h:3:4"}) {
            assertThrows(IllegalArgumentException.class, () -> ReplicatedServerMain.parseMembers(bad), "'" + bad + "'");
        }
    }

    @Test
    void installSnapshotSurvivesTheWireAndMalformedOnesAreRejected() throws IOException {
        byte[] state = new byte[5_000];
        new Random(5).nextBytes(state);
        RaftMessage.InstallSnapshot sent = new RaftMessage.InstallSnapshot(9, 2, 12_345, 8, state);
        byte[] bytes = RaftMessageCodec.encode(sent);
        RaftMessage.InstallSnapshot got = (RaftMessage.InstallSnapshot) RaftMessageCodec.decode(bytes);
        assertEquals(9, got.term());
        assertEquals(2, got.leaderId());
        assertEquals(12_345, got.lastIncludedIndex());
        assertEquals(8, got.lastIncludedTerm());
        org.junit.jupiter.api.Assertions.assertArrayEquals(state, got.state());

        roundTripEmptyState();
        for (int cut = 0; cut < bytes.length; cut += 7) {
            byte[] truncated = java.util.Arrays.copyOf(bytes, cut);
            assertThrows(IOException.class, () -> RaftMessageCodec.decode(truncated), "cut at " + cut);
        }
        byte[] badLength = bytes.clone();
        badLength[1 + 8 + 4 + 8 + 8 + 8 + 1] = 0x7f; // type, term, leader, index, term, offset, done, then the length: claims far more than is there
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(badLength));
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(9, 2, 0, 8, state))), "index must be positive");
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(9, 2, 5, 0, state))), "term must be positive");
    }

    private static void roundTripEmptyState() throws IOException {
        RaftMessage.InstallSnapshot empty = (RaftMessage.InstallSnapshot) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(1, 0, 1, 1, new byte[0])));
        assertEquals(0, empty.state().length);
    }

    @Test
    void theReadRoundTravelsInBothDirectionsAndANegativeOneIsRejected() throws IOException {
        RaftMessage.AppendEntries ae = (RaftMessage.AppendEntries) RaftMessageCodec.decode(RaftMessageCodec.encode(
                new RaftMessage.AppendEntries(4, 1, 7, 3, java.util.List.of(), 6, 123_456_789_012L)));
        assertEquals(123_456_789_012L, ae.readRound());
        assertEquals(6, ae.leaderCommit());
        RaftMessage.AppendEntriesReply ok = (RaftMessage.AppendEntriesReply) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.AppendEntriesReply(4, true, 9, 0, 0, 55)));
        assertEquals(55, ok.readRound());
        assertEquals(9, ok.matchIndex());
        RaftMessage.AppendEntriesReply no = (RaftMessage.AppendEntriesReply) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.AppendEntriesReply(4, false, 0, 3, 2, 66)));
        assertEquals(66, no.readRound());
        assertEquals(3, no.conflictIndex());
        byte[] negative = RaftMessageCodec.encode(new RaftMessage.AppendEntriesReply(4, true, 9, 0, 0, 1));
        java.util.Arrays.fill(negative, negative.length - 8, negative.length, (byte) 0xff);
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(negative));
    }

    @Test
    void aSnapshotChunkAndItsReplySurviveTheWireAndBadOnesAreRejected() throws IOException {
        byte[] data = new byte[300];
        new Random(8).nextBytes(data);
        RaftMessage.InstallSnapshot chunk = (RaftMessage.InstallSnapshot) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(7, 1, 500, 6, 4_096, data, true)));
        assertEquals(4_096, chunk.offset());
        assertTrue(chunk.done());
        org.junit.jupiter.api.Assertions.assertArrayEquals(data, chunk.data());
        RaftMessage.InstallSnapshot middle = (RaftMessage.InstallSnapshot) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(7, 1, 500, 6, 0, data, false)));
        assertTrue(!middle.done());

        RaftMessage.SnapshotChunkReply reply = (RaftMessage.SnapshotChunkReply) RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.SnapshotChunkReply(7, 500, 8_192)));
        assertEquals(7, reply.term());
        assertEquals(500, reply.lastIncludedIndex());
        assertEquals(8_192, reply.nextOffset());

        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.InstallSnapshot(7, 1, 500, 6, -1, data, false))), "negative offset");
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(RaftMessageCodec.encode(
                new RaftMessage.InstallSnapshot(7, 1, 500, 6, com.distri.cache.raft.RaftStorage.MAX_SNAPSHOT_BYTES - 10,
                        data, false))), "offset + length beyond the largest snapshot");
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(RaftMessageCodec.encode(
                new RaftMessage.InstallSnapshot(7, 1, 500, 6, 0,
                        new byte[com.distri.cache.raft.RaftStorage.MAX_SNAPSHOT_CHUNK_BYTES + 1], false))),
                "a chunk larger than the chunk limit");
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.SnapshotChunkReply(7, 0, 5))), "index must be positive");
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(
                RaftMessageCodec.encode(new RaftMessage.SnapshotChunkReply(7, 5, -1))), "offset must not be negative");
        byte[] bytes = RaftMessageCodec.encode(new RaftMessage.SnapshotChunkReply(7, 500, 8_192));
        for (int cut = 0; cut < bytes.length; cut++) {
            byte[] truncated = java.util.Arrays.copyOf(bytes, cut);
            assertThrows(IOException.class, () -> RaftMessageCodec.decode(truncated), "cut at " + cut);
        }
    }
}
