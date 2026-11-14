package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * The case read-index exists for: a leader that has been cut off does not know it was replaced. It must NOT answer
 * reads (it cannot reach a majority to confirm), because a newer leader may already have committed newer data.
 */
class ReadIndexScenarioTest {

    private record Setup(ReplicatedHarness h, int oldLeader, int newLeader) {}

    /** Old leader holds k=old; it is then isolated; the other two elect a new leader that commits k=new. */
    private static Setup replacedLeader(RaftConfig config) {
        ReplicatedHarness h = new ReplicatedHarness(3, 7, config.withCheckQuorum(false), null, false,
                ReplicatedCache.ReadMode.READ_INDEX);
        int oldLeader = h.electLeader();
        CompletableFuture<ReplicatedCache.Reply> w1 = h.caches[oldLeader].set(1, 1, "k", "old", -1);
        assertTrue(h.until(w1::isDone, 2_000));
        assertEquals(ReplicatedCache.Kind.OK, w1.join().kind());
        assertTrue(h.until(h::fullyApplied, 2_000));
        // a read before the cut works: it proves the read path is alive and sees the write
        CompletableFuture<ReplicatedCache.Reply> before = h.caches[oldLeader].get(1, 2, "k");
        assertTrue(h.until(before::isDone, 2_000));
        assertEquals("old", before.join().value());

        h.cluster.isolate(oldLeader);
        assertTrue(h.until(() -> {
            for (int l : h.cluster.leaders()) {
                if (l != oldLeader) {
                    return true;
                }
            }
            return false;
        }, 3_000), "the other two servers must elect a new leader");
        int newLeader = -1;
        for (int l : h.cluster.leaders()) {
            if (l != oldLeader) {
                newLeader = l;
            }
        }
        CompletableFuture<ReplicatedCache.Reply> w2 = h.caches[newLeader].set(2, 1, "k", "new", -1);
        assertTrue(h.until(w2::isDone, 2_000));
        assertEquals(ReplicatedCache.Kind.OK, w2.join().kind());
        return new Setup(h, oldLeader, newLeader);
    }

    @Test
    void aCutOffLeaderDoesNotAnswerAReadAndOnceItLearnsOfTheNewLeaderItRefuses() {
        Setup s = replacedLeader(RaftConfig.defaults());
        assertEquals(RaftNode.Role.LEADER, s.h().cluster.node(s.oldLeader()).role(),
                "without check-quorum the cut-off leader still believes it leads");
        CompletableFuture<ReplicatedCache.Reply> read = s.h().caches[s.oldLeader()].get(9, 1, "k");
        s.h().until(read::isDone, 500);
        assertFalse(read.isDone(), "answered without a majority: a stale read. Got "
                + (read.isDone() ? read.join() : null));

        s.h().cluster.heal();
        assertTrue(s.h().until(read::isDone, 3_000));
        assertEquals(ReplicatedCache.Kind.NOT_LEADER, read.join().kind(),
                "once it hears of the new term the old leader must refuse, not answer");
    }

    @Test
    void theNewLeaderAnswersWithTheNewValue() {
        Setup s = replacedLeader(RaftConfig.defaults());
        CompletableFuture<ReplicatedCache.Reply> read = s.h().caches[s.newLeader()].get(9, 1, "k");
        assertTrue(s.h().until(read::isDone, 2_000));
        assertEquals("new", read.join().value());
    }

    @Test
    void withoutTheLeadershipCheckTheCutOffLeaderAnswersWithStaleData() {
        Setup s = replacedLeader(RaftConfig.defaults().withFaults(RaftConfig.Fault.READ_SKIP_LEADERSHIP_CHECK));
        CompletableFuture<ReplicatedCache.Reply> read = s.h().caches[s.oldLeader()].get(9, 1, "k");
        assertTrue(s.h().until(read::isDone, 500), "the injected bug answers at once");
        assertEquals("old", read.join().value(), "and the answer is stale: the new leader already holds \"new\"");
    }

    @Test
    void countingOldAcknowledgementsLetsTheCutOffLeaderAnswerWithStaleData() {
        Setup s = replacedLeader(RaftConfig.defaults().withFaults(RaftConfig.Fault.READ_COUNT_OLD_ACKS));
        CompletableFuture<ReplicatedCache.Reply> read = s.h().caches[s.oldLeader()].get(9, 1, "k");
        assertTrue(s.h().until(read::isDone, 500), "the injected bug answers at once");
        assertEquals("old", read.join().value());
    }
}
