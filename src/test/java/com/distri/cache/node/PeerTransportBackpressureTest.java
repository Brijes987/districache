package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.raft.LogEntry;
import com.distri.cache.raft.RaftMessage;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A peer that is down must not make the sender hold on to an unbounded amount of memory. The outgoing queue used to be
 * bounded by the NUMBER of messages (4,096), so a leader with a dead follower queued one multi-megabyte AppendEntries
 * per heartbeat and per client write until the heap was full. Raft tolerates dropped messages (the heartbeat resends
 * what matters), so the queue is now bounded by bytes too.
 */
class PeerTransportBackpressureTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static RaftMessage.AppendEntries bigAppend(int entries, int bytesEach) {
        List<LogEntry> list = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            list.add(new LogEntry(1, new byte[bytesEach]));
        }
        return new RaftMessage.AppendEntries(1, 0, 0, 0, list, 0);
    }

    @Test
    void aPeerThatIsDownDoesNotMakeTheSenderQueueWithoutLimit() throws Exception {
        Map<Integer, InetSocketAddress> peers = Map.of(1, new InetSocketAddress("127.0.0.1", freePort())); // nothing listens
        try (PeerTransport transport = new PeerTransport(0, peers, null, (from, message) -> {})) {
            transport.start("127.0.0.1", 0);
            long offered = 0;
            for (int i = 0; i < 60; i++) {
                RaftMessage.AppendEntries message = bigAppend(20, 100_000); // about 2 MB, like a batch of large values
                transport.send(1, message);
                offered += 2_000_000;
            }
            long queued = transport.queuedBytes(1);
            assertTrue(offered >= 100_000_000, "the test offers well over the limit");
            assertTrue(queued <= PeerTransport.MAX_QUEUED_BYTES + 3_000_000,
                    "bytes waiting for a peer that is down: " + queued);
            assertTrue(transport.dropped() > 0, "messages beyond the limit are dropped, not queued");
        }
    }

    @Test
    void aSingleMessageLargerThanTheLimitIsStillSentWhenNothingElseIsWaiting() throws Exception {
        Map<Integer, InetSocketAddress> peers = Map.of(1, new InetSocketAddress("127.0.0.1", freePort()));
        try (PeerTransport transport = new PeerTransport(0, peers, null, (from, message) -> {})) {
            transport.start("127.0.0.1", 0);
            transport.send(1, bigAppend(1, 1_000_000));
            assertTrue(transport.queuedBytes(1) >= 1_000_000, "a lone message is queued, however large");
        }
    }
}
