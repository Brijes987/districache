package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The cache as a snapshottable state machine: what is saved, and that a bad snapshot changes nothing. */
class SnapshotCacheTest {

    private final AtomicLong clock = new AtomicLong(1_000);

    private ReplicatedCache cache(int capacity) {
        return new ReplicatedCache(capacity, 100, clock::get, true);
    }

    private static void apply(ReplicatedCache c, long index, CacheCommand command) {
        c.apply(index, command.encode());
    }

    @Test
    void aRestoredCopyHoldsTheSameEntriesAndTheSameClientSessions() {
        ReplicatedCache a = cache(100);
        apply(a, 1, CacheCommand.set(1, 1, "k1", "v1", -1));
        apply(a, 2, CacheCommand.set(2, 1, "k2", "v2", 5_000));
        apply(a, 3, CacheCommand.delete(1, 2, "k1"));
        apply(a, 4, CacheCommand.set(3, 7, "é-ключ-键", "väl-值", -1));
        byte[] state = a.snapshot();

        ReplicatedCache b = cache(100);
        apply(b, 1, CacheCommand.set(9, 1, "junk", "junk", -1)); // replaced, not merged
        b.restore(state);
        assertEquals(a.contents(), b.contents());
        assertEquals(Map.of("k2", "v2", "é-ключ-键", "väl-值"), b.contents());
        assertEquals(a.sessionCount(), b.sessionCount());

        // the sessions work: a retry of client 1's last request (seq 2) is not executed again, an old one is stale
        apply(b, 5, CacheCommand.delete(1, 2, "k2"));
        assertTrue(b.contents().containsKey("k2"), "a retry of an already executed request must not run again");
        apply(b, 6, CacheCommand.set(1, 1, "again", "x", -1));
        assertFalse(b.contents().containsKey("again"), "an older request number than the session's is refused");
        apply(b, 7, CacheCommand.set(1, 3, "new", "y", -1));
        assertEquals("y", b.contents().get("new"));
    }

    @Test
    void theLeastRecentlyUsedOrderSurvivesSoTheSameEntryIsEvictedNext() {
        ReplicatedCache a = cache(3);
        apply(a, 1, CacheCommand.set(1, 1, "a", "1", -1));
        apply(a, 2, CacheCommand.set(1, 2, "b", "2", -1));
        apply(a, 3, CacheCommand.set(1, 3, "c", "3", -1));
        apply(a, 4, CacheCommand.get(1, 4, "a")); // a is now the most recent; b is the oldest
        ReplicatedCache b = cache(3);
        b.restore(a.snapshot());
        apply(a, 5, CacheCommand.set(1, 5, "d", "4", -1));
        apply(b, 5, CacheCommand.set(1, 5, "d", "4", -1));
        assertEquals(a.contents(), b.contents());
        assertFalse(b.contents().containsKey("b"));
        assertTrue(b.contents().containsKey("a"));
    }

    @Test
    void anEntryWhoseTimeHasPassedWhenRestoredIsNotRestoredAndOthersKeepTheirRemainingTime() throws Exception {
        ReplicatedCache a = new ReplicatedCache(10, 100, System::currentTimeMillis, true);
        long now = System.currentTimeMillis();
        apply(a, 1, CacheCommand.set(1, 1, "short", "v", now + 300));
        apply(a, 2, CacheCommand.set(1, 2, "long", "v", now + 600_000));
        byte[] state = a.snapshot();
        Thread.sleep(450);
        ReplicatedCache b = new ReplicatedCache(10, 100, System::currentTimeMillis, true);
        b.restore(state);
        assertEquals(Map.of("long", "v"), b.contents());
    }

    @Test
    void theExecutionCountsTravelWithTheSnapshotSoExactlyOnceChecksStayMeaningful() {
        ReplicatedCache a = cache(10);
        apply(a, 1, CacheCommand.set(4, 1, "k", "v", -1));
        ReplicatedCache b = cache(10);
        b.restore(a.snapshot());
        assertEquals(1, b.executions(4, 1));
    }

    @Test
    void anyMalformedSnapshotIsRejectedAndLeavesTheCacheUntouched() {
        ReplicatedCache a = cache(10);
        apply(a, 1, CacheCommand.set(1, 1, "k", "v", -1));
        apply(a, 2, CacheCommand.set(2, 1, "k2", "v2", -1));
        byte[] good = a.snapshot();

        ReplicatedCache b = cache(10);
        apply(b, 1, CacheCommand.set(7, 1, "mine", "kept", -1));
        Map<String, String> before = b.contents();
        int sessionsBefore = b.sessionCount();

        for (int cut = 0; cut < good.length; cut++) {
            byte[] truncated = java.util.Arrays.copyOf(good, cut);
            assertThrows(IllegalArgumentException.class, () -> b.restore(truncated), "cut at " + cut);
            assertEquals(before, b.contents());
            assertEquals(sessionsBefore, b.sessionCount());
        }
        byte[] trailing = java.util.Arrays.copyOf(good, good.length + 1);
        assertThrows(IllegalArgumentException.class, () -> b.restore(trailing));
        byte[] wrongVersion = good.clone();
        wrongVersion[3] = 9;
        assertThrows(IllegalArgumentException.class, () -> b.restore(wrongVersion));
        assertEquals(before, b.contents());
        b.restore(good); // and the good one still works afterwards
        assertEquals(a.contents(), b.contents());
    }

    @Test
    void randomGarbageNeverCrashesAnythingButTheDocumentedException() {
        java.util.Random random = new java.util.Random(11);
        ReplicatedCache b = cache(10);
        for (int i = 0; i < 2_000; i++) {
            byte[] junk = new byte[random.nextInt(64)];
            random.nextBytes(junk);
            try {
                b.restore(junk);
            } catch (IllegalArgumentException expected) {
                // fine
            }
        }
    }

    @Test
    void aCapturedStateIsNotAffectedByCommandsAppliedAfterItWasCaptured() {
        ReplicatedCache a = cache(100);
        apply(a, 1, CacheCommand.set(1, 1, "k1", "v1", -1));
        apply(a, 2, CacheCommand.set(2, 1, "k2", "v2", -1));
        StateMachine.SnapshotCapture capture = a.captureSnapshot();
        // the node goes on while the bytes are produced elsewhere
        apply(a, 3, CacheCommand.delete(1, 2, "k1"));
        apply(a, 4, CacheCommand.set(1, 3, "k3", "v3", -1));
        apply(a, 5, CacheCommand.set(3, 1, "k4", "v4", -1));
        byte[] state = capture.serialize();

        ReplicatedCache b = cache(100);
        b.restore(state);
        assertEquals(Map.of("k1", "v1", "k2", "v2"), b.contents(), "the state as of index 2, nothing later");
        assertEquals(2, b.sessionCount(), "the session of client 3 did not exist at index 2");
        // client 1's session is as of seq 1: its seq 2 and 3 are new requests, not retries
        apply(b, 3, CacheCommand.delete(1, 2, "k1"));
        assertFalse(b.contents().containsKey("k1"));
        apply(b, 4, CacheCommand.set(1, 3, "k3", "v3", -1));
        assertEquals("v3", b.contents().get("k3"));
    }
}
