package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.raft.RaftMessage;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class PreVoteWireTest {

    @Test
    void preVoteMessagesRoundTrip() throws IOException {
        RaftMessage[] messages = {
            new RaftMessage.PreVote(7, 2, 100, 6),
            new RaftMessage.PreVoteReply(7, true),
            new RaftMessage.PreVoteReply(6, false),
            new RaftMessage.PreVoteReply(0, false), // a node that has never seen a term refuses with term 0
        };
        for (RaftMessage m : messages) {
            assertEquals(m, RaftMessageCodec.decode(RaftMessageCodec.encode(m)));
        }
    }

    @Test
    void malformedPreVotesAreRejected() {
        byte[] good = RaftMessageCodec.encode(new RaftMessage.PreVote(7, 2, 100, 6));
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(java.util.Arrays.copyOf(good, good.length - 1)));
        byte[] extra = java.util.Arrays.copyOf(good, good.length + 1);
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(extra));
        byte[] zeroTerm = RaftMessageCodec.encode(new RaftMessage.PreVote(7, 2, 100, 6));
        for (int i = 1; i <= 8; i++) {
            zeroTerm[i] = 0; // the term field
        }
        assertThrows(IOException.class, () -> RaftMessageCodec.decode(zeroTerm));
    }

    @Test
    void existingMessageTypesKeepTheirCodes() {
        assertEquals(1, RaftMessageCodec.encode(new RaftMessage.RequestVote(1, 0, 0, 0))[0]);
        assertEquals(2, RaftMessageCodec.encode(new RaftMessage.RequestVoteReply(1, true))[0]);
        assertEquals(7, RaftMessageCodec.encode(new RaftMessage.PreVote(1, 0, 0, 0))[0]);
        assertEquals(8, RaftMessageCodec.encode(new RaftMessage.PreVoteReply(1, true))[0]);
    }
}
