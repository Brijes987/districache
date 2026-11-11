package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** The client front end with group commit on: requests queue, {@code flush()} writes them together. */
class GroupCommitCacheTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();

    private static ReplicatedCache.Reply ans(CompletableFuture<ReplicatedCache.Reply> f) {
        assertTrue(f.isDone(), "the request was never answered");
        return f.join();
    }

    private static ReplicatedHarness harness(int n, long seed) {
        return new ReplicatedHarness(n, seed, CONFIG, null, true);
    }

    @Test
    void queuedRequestsWaitForTheFlushAndThenShareOneBatch() {
        ReplicatedHarness h = harness(1, 1);
        ReplicatedCache c = h.caches[h.electLeader()];
        CompletableFuture<ReplicatedCache.Reply> set = c.set(1, 1, "k", "v", -1);
        CompletableFuture<ReplicatedCache.Reply> get = c.get(1, 2, "k");
        CompletableFuture<ReplicatedCache.Reply> del = c.delete(1, 3, "k");
        CompletableFuture<ReplicatedCache.Reply> gone = c.get(1, 4, "k");
        assertEquals(4, c.queuedCount());
        assertFalse(set.isDone() || get.isDone() || del.isDone() || gone.isDone(), "nothing is answered before flush");

        long batchesBefore = c.batchesProposed();
        c.flush();
        assertEquals(0, c.queuedCount());
        assertEquals(batchesBefore + 1, c.batchesProposed(), "four requests, one batch");
        assertEquals(4, c.largestBatch());
        assertEquals(ReplicatedCache.Kind.OK, ans(set).kind());
        assertTrue(ans(get).found(), "a read later in the same batch sees the write before it");
        assertEquals("v", ans(get).value());
        assertTrue(ans(del).found());
        assertFalse(ans(gone).found(), "and a read after the delete sees it gone");
    }

    @Test
    void inAGroupNothingIsAnsweredUntilTheBatchIsCommittedEverywhereNeeded() {
        ReplicatedHarness h = harness(3, 2);
        int leader = h.electLeader();
        h.until(h::fullyApplied, 500);
        ReplicatedCache c = h.caches[leader];
        List<CompletableFuture<ReplicatedCache.Reply>> futures = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            futures.add(c.set(1, i, "k" + i, "v" + i, -1));
        }
        c.flush();
        for (var f : futures) {
            assertFalse(f.isDone(), "acknowledged before a majority could have stored it");
        }
        assertTrue(h.until(() -> futures.stream().allMatch(CompletableFuture::isDone), 3_000));
        for (var f : futures) {
            assertEquals(ReplicatedCache.Kind.OK, ans(f).kind());
        }
        assertTrue(h.until(h::fullyApplied, 3_000));
        Map<String, String> expected = c.contents();
        assertEquals(6, expected.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(expected, h.caches[i].contents());
        }
    }

    @Test
    void aFollowerAnswersNotLeaderAtOnceAndQueuesNothing() {
        ReplicatedHarness h = harness(3, 3);
        int leader = h.electLeader();
        h.until(() -> h.cluster.node((leader + 1) % 3).leaderId() == leader, 500);
        ReplicatedCache follower = h.caches[(leader + 1) % 3];
        ReplicatedCache.Reply r = ans(follower.set(1, 1, "k", "v", -1));
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, r.kind());
        assertEquals(leader, r.leaderHint());
        assertEquals(0, follower.queuedCount());
    }

    @Test
    void aLeaderDemotedBetweenSubmitAndFlushAnswersNotLeaderAndWritesNothing() {
        ReplicatedHarness h = harness(3, 4);
        int leader = h.electLeader();
        h.until(h::fullyApplied, 500);
        ReplicatedCache c = h.caches[leader];
        long lastBefore = h.cluster.node(leader).lastLogIndex();
        CompletableFuture<ReplicatedCache.Reply> f = c.set(1, 1, "k", "v", -1);
        // a message from a newer term arrives before the loop gets to flush
        h.cluster.node(leader).receive((leader + 1) % 3, new RaftMessage.RequestVote(
                h.cluster.node(leader).term() + 5, (leader + 1) % 3, 0, 0));
        assertEquals(RaftNode.Role.FOLLOWER, h.cluster.node(leader).role());
        c.flush();
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, ans(f).kind());
        assertEquals(lastBefore, h.cluster.node(leader).lastLogIndex(), "nothing was written");
    }

    @Test
    void aLeaderCutOffAfterFlushNeverAcknowledgesAndTheRequestsEndAsUnknown() {
        ReplicatedHarness h = harness(3, 5);
        int leader = h.electLeader();
        h.until(h::fullyApplied, 500);
        ReplicatedCache c = h.caches[leader];
        h.cluster.isolate(leader);
        List<CompletableFuture<ReplicatedCache.Reply>> futures = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            futures.add(c.set(9, i, "k", "v" + i, -1));
        }
        c.flush();
        assertTrue(h.until(() -> futures.stream().allMatch(CompletableFuture::isDone), 3_000));
        for (var f : futures) {
            assertEquals(ReplicatedCache.Kind.UNKNOWN, ans(f).kind(), "never OK without a majority");
        }
    }

    @Test
    void theSameRequestTwiceInOneBatchRunsOnceAndBothGetTheSameAnswer() {
        ReplicatedHarness h = harness(1, 6);
        ReplicatedCache c = h.caches[h.electLeader()];
        CompletableFuture<ReplicatedCache.Reply> a = c.set(7, 1, "k", "v", -1);
        CompletableFuture<ReplicatedCache.Reply> b = c.set(7, 1, "k", "v", -1);
        c.flush();
        assertEquals(ReplicatedCache.Kind.OK, ans(a).kind());
        assertEquals(ReplicatedCache.Kind.OK, ans(b).kind());
        assertEquals(1, c.executions(7, 1));
    }

    @Test
    void anEmptyFlushDoesNothingAndBatchingCannotBeSwitchedOffWithRequestsWaiting() {
        ReplicatedHarness h = harness(1, 7);
        ReplicatedCache c = h.caches[h.electLeader()];
        long batches = c.batchesProposed();
        c.flush();
        assertEquals(batches, c.batchesProposed());
        c.set(1, 1, "k", "v", -1);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> c.setBatching(false));
        c.flush();
        c.setBatching(false);
    }

    @Test
    void abandoningFailsQueuedRequestsToo() {
        ReplicatedHarness h = harness(1, 8);
        ReplicatedCache c = h.caches[h.electLeader()];
        CompletableFuture<ReplicatedCache.Reply> f = c.set(1, 1, "k", "v", -1);
        c.abandonPending();
        assertEquals(ReplicatedCache.Kind.UNKNOWN, ans(f).kind());
        assertEquals(0, c.queuedCount());
    }
}
