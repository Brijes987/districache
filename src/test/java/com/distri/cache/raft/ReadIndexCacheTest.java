package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Read-index mode of {@link ReplicatedCache}. */
class ReadIndexCacheTest {

    private static final class Single {
        final ReplicatedCache cache;
        final RaftNode node;

        Single(int capacity, ReplicatedCache.ReadMode mode) {
            cache = new ReplicatedCache(capacity, 100, System::currentTimeMillis);
            node = new RaftNode(0, List.of(), new InMemoryRaftStorage(), (to, m) -> { }, cache,
                    RaftConfig.defaults(), new Random(1));
            cache.attach(node);
            cache.setReadMode(mode);
            for (int i = 0; i < 1000 && node.role() != RaftNode.Role.LEADER; i++) {
                node.tick();
            }
            assertEquals(RaftNode.Role.LEADER, node.role());
        }
    }

    @Test
    void aReadSeesTheLatestAcknowledgedWrite() {
        Single s = new Single(10, ReplicatedCache.ReadMode.READ_INDEX);
        assertEquals(ReplicatedCache.Kind.OK, s.cache.set(1, 1, "k", "v1", -1).join().kind());
        assertEquals("v1", s.cache.get(1, 2, "k").join().value());
        s.cache.set(1, 3, "k", "v2", -1).join();
        assertEquals("v2", s.cache.get(1, 4, "k").join().value());
        assertFalse(s.cache.get(1, 5, "missing").join().found());
    }

    @Test
    void aReadAddsNoLogEntry() {
        Single s = new Single(10, ReplicatedCache.ReadMode.READ_INDEX);
        s.cache.set(1, 1, "k", "v", -1).join();
        long before = s.node.lastLogIndex();
        for (int i = 0; i < 20; i++) {
            s.cache.get(1, 2 + i, "k").join();
        }
        assertEquals(before, s.node.lastLogIndex());
        assertEquals(20, s.node.readsServed());
    }

    @Test
    void inLogModeAReadDoesAddAnEntry() {
        Single s = new Single(10, ReplicatedCache.ReadMode.LOG);
        s.cache.set(1, 1, "k", "v", -1).join();
        long before = s.node.lastLogIndex();
        s.cache.get(1, 2, "k").join();
        assertEquals(before + 1, s.node.lastLogIndex());
    }

    @Test
    void aReadDoesNotChangeWhichKeyIsEvicted() {
        // Every server must evict the same key. Only the leader serves a read-index read, so if it reordered the
        // recency list the leader and its followers would later disagree about what is in the cache.
        Single s = new Single(2, ReplicatedCache.ReadMode.READ_INDEX);
        s.cache.set(1, 1, "a", "1", -1).join();
        s.cache.set(1, 2, "b", "2", -1).join();
        assertEquals("1", s.cache.get(1, 3, "a").join().value()); // would make "a" most recently used with get()
        s.cache.set(1, 4, "c", "3", -1).join();                  // evicts the least recently used WRITE: "a"
        assertEquals(ReplicatedCache.Kind.OK, s.cache.get(1, 5, "a").join().kind());
        assertFalse(s.cache.get(1, 6, "a").join().found(), "a was the least recently used by the log, so it is evicted");
        assertTrue(s.cache.get(1, 7, "b").join().found());
    }

    @Test
    void aFollowerRefusesAndAReadOnADemotedLeaderFails() {
        ReplicatedCache cache = new ReplicatedCache(10, 100, System::currentTimeMillis);
        RaftNode node = new RaftNode(0, List.of(1, 2), new InMemoryRaftStorage(), (to, m) -> { }, cache,
                RaftConfig.defaults(), new Random(1));
        cache.attach(node);
        cache.setReadMode(ReplicatedCache.ReadMode.READ_INDEX);
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, cache.get(1, 1, "k").join().kind());

        for (int i = 0; i < 1000 && node.role() != RaftNode.Role.CANDIDATE; i++) {
            node.tick();
        }
        node.receive(1, new RaftMessage.RequestVoteReply(node.term(), true));
        assertEquals(RaftNode.Role.LEADER, node.role());
        CompletableFuture<ReplicatedCache.Reply> pending = cache.get(1, 2, "k");
        assertFalse(pending.isDone(), "nobody confirmed the leader yet");
        node.receive(2, new RaftMessage.RequestVote(node.term() + 1, 2, 9, 9));
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, pending.join().kind());
    }

    @Test
    void abandoningFailsReadsInFlight() {
        ReplicatedCache cache = new ReplicatedCache(10, 100, System::currentTimeMillis);
        RaftNode node = new RaftNode(0, List.of(1, 2), new InMemoryRaftStorage(), (to, m) -> { }, cache,
                RaftConfig.defaults(), new Random(1));
        cache.attach(node);
        cache.setReadMode(ReplicatedCache.ReadMode.READ_INDEX);
        for (int i = 0; i < 1000 && node.role() != RaftNode.Role.CANDIDATE; i++) {
            node.tick();
        }
        node.receive(1, new RaftMessage.RequestVoteReply(node.term(), true));
        CompletableFuture<ReplicatedCache.Reply> pending = cache.get(1, 1, "k");
        cache.abandonPending();
        assertEquals(ReplicatedCache.Kind.UNKNOWN, pending.join().kind());
    }
}
