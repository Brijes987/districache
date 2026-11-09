package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ReplicatedCacheTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();

    private static ReplicatedCache.Reply done(CompletableFuture<ReplicatedCache.Reply> f) {
        assertTrue(f.isDone(), "the request should have been answered by now");
        return f.join();
    }

    /** Submits through the leader and runs the simulation until the answer arrives. */
    private static ReplicatedCache.Reply await(ReplicatedHarness h, CompletableFuture<ReplicatedCache.Reply> f) {
        assertTrue(h.until(f::isDone, 3_000), "no answer");
        return f.join();
    }

    // ---------------------------------------------------------------- one server

    @Test
    void aSingleServerAnswersSetGetAndDelete() {
        ReplicatedHarness h = new ReplicatedHarness(1, 1, CONFIG);
        int leader = h.electLeader();
        ReplicatedCache c = h.caches[leader];

        assertEquals(ReplicatedCache.Kind.OK, done(c.set(1, 1, "k", "v", -1)).kind());
        ReplicatedCache.Reply get = done(c.get(1, 2, "k"));
        assertTrue(get.found());
        assertEquals("v", get.value());
        assertTrue(done(c.delete(1, 3, "k")).found());
        assertFalse(done(c.get(1, 4, "k")).found());
        assertFalse(done(c.delete(1, 5, "k")).found(), "deleting a missing key reports not found");
    }

    @Test
    void aRetriedRequestIsAnsweredFromTheSessionAndNotExecutedAgain() {
        ReplicatedHarness h = new ReplicatedHarness(1, 2, CONFIG);
        ReplicatedCache c = h.caches[h.electLeader()];

        done(c.set(7, 1, "k", "v", -1));
        ReplicatedCache.Reply again = done(c.set(7, 1, "k", "v", -1));
        assertEquals(ReplicatedCache.Kind.OK, again.kind());
        assertEquals(1, c.executions(7, 1));

        assertTrue(done(c.delete(7, 2, "k")).found());
        ReplicatedCache.Reply retriedDelete = done(c.delete(7, 2, "k"));
        assertTrue(retriedDelete.found(), "the retry gets the ORIGINAL answer, not 'not found'");
        assertEquals(1, c.executions(7, 2));

        // another client writes the key in between; the retry must not delete it
        done(c.set(8, 1, "k", "other", -1));
        done(c.delete(7, 2, "k"));
        assertEquals(Map.of("k", "other"), c.contents());
    }

    @Test
    void aRequestOlderThanOneAlreadyExecutedIsRefused() {
        ReplicatedHarness h = new ReplicatedHarness(1, 3, CONFIG);
        ReplicatedCache c = h.caches[h.electLeader()];
        done(c.set(5, 1, "k", "first", -1));
        done(c.set(5, 2, "k", "second", -1));
        ReplicatedCache.Reply stale = done(c.set(5, 1, "k", "evil", -1));
        assertEquals(ReplicatedCache.Kind.STALE, stale.kind());
        assertEquals(Map.of("k", "second"), c.contents());
    }

    @Test
    void aTimeToLiveEndsTheEntry() throws InterruptedException {
        ReplicatedHarness h = new ReplicatedHarness(1, 4, CONFIG);
        ReplicatedCache c = h.caches[h.electLeader()];
        done(c.set(1, 1, "k", "v", 60));
        assertTrue(done(c.get(1, 2, "k")).found());
        Thread.sleep(150);
        assertFalse(done(c.get(1, 3, "k")).found());
    }

    @Test
    void aSetWhoseExpiryHasAlreadyPassedWhenItIsAppliedLeavesTheKeyAbsent() {
        long[] now = {1_000};
        ReplicatedCache c = new ReplicatedCache(10, 10, () -> now[0]);
        c.apply(1, CacheCommand.set(1, 1, "k", "old", -1).encode());
        c.apply(2, CacheCommand.set(1, 2, "k", "late", 500).encode()); // expired at 500, applied at 1000
        assertEquals(Map.of(), c.contents(), "the expired write replaces the old value with nothing");
        c.apply(3, CacheCommand.set(1, 3, "j", "fresh", 5_000).encode());
        assertEquals(Map.of("j", "fresh"), c.contents());
        c.close();
    }

    @Test
    void theSessionTableIsBounded() {
        ReplicatedCache c = new ReplicatedCache(10, 2, () -> 0L);
        for (long client = 1; client <= 5; client++) {
            c.apply(client, CacheCommand.set(client, 1, "k" + client, "v", -1).encode());
        }
        assertEquals(2, c.sessionCount());
        c.close();
    }

    @Test
    void badArgumentsAreRejectedBeforeAnythingIsProposed() {
        ReplicatedHarness h = new ReplicatedHarness(1, 5, CONFIG);
        ReplicatedCache c = h.caches[h.electLeader()];
        assertThrows(IllegalArgumentException.class, () -> c.set(1, 1, null, "v", -1));
        assertThrows(IllegalArgumentException.class, () -> c.set(1, 1, "k", null, -1));
        assertThrows(IllegalArgumentException.class, () -> c.set(1, 1, "k", "v", 0));
        assertThrows(IllegalArgumentException.class, () -> c.get(1, 1, null));
        assertEquals(1, h.cluster.node(0).lastLogIndex(), "only the leader's no-op is in the log");
    }

    @Test
    void theCommandFormatRoundTripsAndRejectsGarbage() {
        CacheCommand original = CacheCommand.set(Long.MAX_VALUE, 42, "kéy ☃", "", 123_456_789L);
        assertEquals(original, CacheCommand.decode(original.encode()));
        assertEquals(CacheCommand.get(1, 2, "x"), CacheCommand.decode(CacheCommand.get(1, 2, "x").encode()));
        assertThrows(IllegalArgumentException.class, () -> CacheCommand.decode(new byte[] {9, 0, 0}));
        byte[] valid = CacheCommand.delete(1, 1, "k").encode();
        byte[] withTail = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class, () -> CacheCommand.decode(withTail));
    }

    // ---------------------------------------------------------------- a group

    @Test
    void aFollowerSaysNotLeaderAndNamesTheLeader() {
        ReplicatedHarness h = new ReplicatedHarness(3, 6, CONFIG);
        int leader = h.electLeader();
        h.until(() -> {
            for (int i = 0; i < 3; i++) {
                if (h.cluster.node(i).leaderId() != leader) {
                    return false;
                }
            }
            return true;
        }, 500);
        int follower = (leader + 1) % 3;
        ReplicatedCache.Reply reply = done(h.caches[follower].set(1, 1, "k", "v", -1));
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, reply.kind());
        assertEquals(leader, reply.leaderHint());
        assertEquals(Map.of(), h.caches[leader].contents(), "a refused request must change nothing");
    }

    @Test
    void everyServerEndsWithTheSameDataAndARestartedServerRebuildsItFromItsLog() {
        ReplicatedHarness h = new ReplicatedHarness(3, 7, CONFIG);
        int leader = h.electLeader();
        long seq = 0;
        for (int i = 0; i < 20; i++) {
            assertEquals(ReplicatedCache.Kind.OK, await(h, h.caches[leader].set(1, ++seq, "k" + (i % 7), "v" + i, -1)).kind());
        }
        assertTrue(h.until(h::fullyApplied, 2_000));
        Map<String, String> expected = h.caches[leader].contents();
        for (int i = 0; i < 3; i++) {
            assertEquals(expected, h.caches[i].contents(), "node " + i);
        }

        int follower = (leader + 1) % 3;
        h.cluster.crash(follower);
        for (int i = 20; i < 30; i++) {
            await(h, h.caches[leader].set(1, ++seq, "k" + (i % 7), "v" + i, -1));
        }
        h.cluster.restart(follower);
        assertTrue(h.until(h::fullyApplied, 3_000));
        expected = h.caches[leader].contents();
        assertEquals(expected, h.caches[follower].contents(), "the restarted server replayed its log and caught up");
    }

    @Test
    void anAcknowledgedWriteSurvivesTheLeaderCrashing() {
        ReplicatedHarness h = new ReplicatedHarness(3, 8, CONFIG);
        int leader = h.electLeader();
        assertEquals(ReplicatedCache.Kind.OK, await(h, h.caches[leader].set(1, 1, "k", "precious", -1)).kind());
        h.cluster.crash(leader);
        int next = h.electLeader();
        assertTrue(next != leader);
        ReplicatedCache.Reply reply = await(h, h.caches[next].get(2, 1, "k"));
        assertEquals(ReplicatedCache.Kind.OK, reply.kind());
        assertEquals("precious", reply.value());
    }

    @Test
    void aDeposedLeaderAnswersUnknownAndTheRetryTakesEffectExactlyOnce() {
        ReplicatedHarness h = new ReplicatedHarness(3, 9, CONFIG.withCheckQuorum(false));
        int old = h.electLeader();
        assertTrue(h.until(h::fullyApplied, 500));
        h.cluster.isolate(old);
        CompletableFuture<ReplicatedCache.Reply> inFlight = h.caches[old].set(1, 1, "k", "v", -1);
        assertFalse(inFlight.isDone(), "cannot commit without a majority");

        assertTrue(h.until(() -> {
            int l = h.cluster.leader();
            return l >= 0 && l != old;
        }, 3_000), "the other two elect a new leader");
        int fresh = h.cluster.leader();
        h.cluster.heal();
        assertTrue(h.until(inFlight::isDone, 3_000));
        assertEquals(ReplicatedCache.Kind.UNKNOWN, inFlight.join().kind());

        // the client retries the SAME request number at the current leader
        ReplicatedCache.Reply retry = await(h, h.caches[h.cluster.leader()].set(1, 1, "k", "v", -1));
        assertEquals(ReplicatedCache.Kind.OK, retry.kind());
        assertTrue(h.until(h::fullyApplied, 3_000));
        for (int i = 0; i < 3; i++) {
            assertEquals(1, h.caches[i].executions(1, 1), "node " + i + " executed the request exactly once");
            assertEquals(Map.of("k", "v"), h.caches[i].contents());
        }
        assertTrue(fresh >= 0);
    }

    @Test
    void aRequestThatDidCommitBeforeTheLeaderWasLostIsNotAppliedTwiceByTheRetry() {
        ReplicatedHarness h = new ReplicatedHarness(3, 10, CONFIG);
        int leader = h.electLeader();
        // commit the write, then lose the reply (the client never sees it) and retry elsewhere
        CompletableFuture<ReplicatedCache.Reply> lost = h.caches[leader].set(3, 1, "k", "v", -1);
        assertTrue(h.until(lost::isDone, 2_000));
        h.cluster.crash(leader);
        int next = h.electLeader();
        ReplicatedCache.Reply retry = await(h, h.caches[next].set(3, 1, "k", "v", -1));
        assertEquals(ReplicatedCache.Kind.OK, retry.kind());
        h.cluster.restart(leader);
        assertTrue(h.until(h::fullyApplied, 3_000));
        for (int i = 0; i < 3; i++) {
            assertEquals(1, h.caches[i].executions(3, 1), "node " + i);
        }
        assertEquals(Set.of("k"), h.caches[next].contents().keySet());
    }
}
