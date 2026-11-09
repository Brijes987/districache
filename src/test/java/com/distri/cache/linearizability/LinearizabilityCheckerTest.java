package com.distri.cache.linearizability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.linearizability.LinearizabilityChecker.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LinearizabilityCheckerTest {

    private final LinearizabilityChecker checker = new LinearizabilityChecker();

    private LinearizabilityChecker.Result check(Operation... ops) {
        return checker.check(List.of(ops));
    }

    private static Operation set(int c, String k, String v, long a, long b) {
        return Operation.set(c, k, v, a, b);
    }

    private static Operation get(int c, String k, String v, long a, long b) {
        return Operation.get(c, k, v != null, v, a, b);
    }

    private static Operation del(int c, String k, boolean found, long a, long b) {
        return Operation.delete(c, k, found, a, b);
    }

    // ---------------------------------------------------------------- hand-made histories

    @Test
    void anEmptyHistoryAndASequentialCorrectOneAreLinearizable() {
        assertTrue(check().ok());
        assertTrue(check(
                set(1, "a", "1", 1, 2),
                get(1, "a", "1", 3, 4),
                del(1, "a", true, 5, 6),
                get(1, "a", null, 7, 8),
                del(1, "a", false, 9, 10)).ok());
    }

    @Test
    void readingAnOldValueAfterANewerWriteHasCompletedIsAViolation() {
        LinearizabilityChecker.Result r = check(
                set(1, "a", "1", 1, 2),
                set(2, "a", "2", 3, 4),
                get(3, "a", "1", 5, 6)); // the write of 2 finished before this read began
        assertEquals(Status.VIOLATION, r.status());
        assertEquals("a", r.key());
        assertTrue(r.explanation().contains("NOT LINEARIZABLE"), r.explanation());
    }

    @Test
    void readingNothingAfterACompletedWriteIsAViolation() {
        assertEquals(Status.VIOLATION, check(set(1, "a", "1", 1, 2), get(2, "a", null, 3, 4)).status());
    }

    @Test
    void readingAValueThatWasNeverWrittenIsAViolation() {
        assertEquals(Status.VIOLATION, check(set(1, "a", "1", 1, 2), get(2, "a", "ghost", 3, 4)).status());
    }

    @Test
    void anOperationOverlappingAWriteMayBeOrderedEitherWay() {
        assertTrue(check(set(1, "a", "1", 1, 10), get(2, "a", null, 2, 3)).ok(), "read ordered before the write");
        assertTrue(check(set(1, "a", "1", 1, 10), get(2, "a", "1", 2, 3)).ok(), "read ordered after the write");
    }

    @Test
    void valuesCannotFlipBackWithoutAnotherWrite() {
        // two concurrent writes, then reads that see 1, then 2, then 1 again: would need three writes
        LinearizabilityChecker.Result r = check(
                set(1, "a", "1", 1, 100),
                set(2, "a", "2", 1, 100),
                get(3, "a", "1", 10, 11),
                get(3, "a", "2", 12, 13),
                get(3, "a", "1", 14, 15));
        assertEquals(Status.VIOLATION, r.status());
        assertTrue(check(
                set(1, "a", "1", 1, 100),
                set(2, "a", "2", 1, 100),
                get(3, "a", "1", 10, 11),
                get(3, "a", "2", 12, 13)).ok(), "1 then 2 is fine: write 1, read, write 2, read");
    }

    @Test
    void realTimeOrderBetweenDifferentClientsIsEnforced() {
        // client 2's write began after client 1's write finished, so the later read cannot see client 1's value
        assertEquals(Status.VIOLATION, check(
                set(1, "a", "1", 1, 2),
                set(2, "a", "2", 3, 4),
                get(3, "a", "1", 5, 6)).status());
    }

    @Test
    void deleteMustReportWhetherTheKeyExisted() {
        assertEquals(Status.VIOLATION, check(set(1, "a", "1", 1, 2), del(1, "a", false, 3, 4)).status());
        assertEquals(Status.VIOLATION, check(del(1, "a", true, 1, 2)).status());
        assertEquals(Status.VIOLATION, check(
                set(1, "a", "1", 1, 2), del(1, "a", true, 3, 4), del(2, "a", true, 5, 6)).status());
        assertTrue(check(set(1, "a", "1", 1, 2), del(1, "a", true, 3, 4), del(2, "a", false, 5, 6)).ok());
    }

    @Test
    void aPendingWriteMayHaveTakenEffectOrNot() {
        assertTrue(check(Operation.pendingSet(1, "a", "9", 1), get(2, "a", "9", 5, 6)).ok(), "it took effect");
        assertTrue(check(Operation.pendingSet(1, "a", "9", 1), get(2, "a", null, 5, 6)).ok(), "it did not");
        assertEquals(Status.VIOLATION, check(get(2, "a", "9", 5, 6)).status(), "with no write at all it is impossible");
    }

    @Test
    void aPendingWriteCannotExplainAReadThatFinishedBeforeItStarted() {
        assertEquals(Status.VIOLATION, check(get(2, "a", "9", 1, 2), Operation.pendingSet(1, "a", "9", 5)).status());
    }

    @Test
    void aPendingWriteTakesEffectAtMostOnceAtOnePoint() {
        // seen as 9, then absent, then 9 again, with no other writer: impossible even for a pending write
        assertEquals(Status.VIOLATION, check(
                Operation.pendingSet(1, "a", "9", 1),
                get(2, "a", "9", 5, 6),
                get(2, "a", null, 7, 8),
                get(2, "a", "9", 9, 10)).status());
    }

    @Test
    void aPendingDeleteMayExplainAnAbsentRead() {
        assertTrue(check(set(1, "a", "1", 1, 2), Operation.pendingDelete(2, "a", 3), get(3, "a", null, 5, 6)).ok());
        assertTrue(check(set(1, "a", "1", 1, 2), Operation.pendingDelete(2, "a", 3), get(3, "a", "1", 5, 6)).ok());
    }

    @Test
    void keysAreCheckedIndependentlyAndTheFailingKeyIsNamed() {
        LinearizabilityChecker.Result r = check(
                set(1, "a", "1", 1, 2), get(1, "a", "1", 3, 4),
                set(1, "b", "1", 5, 6), get(1, "b", null, 7, 8));
        assertEquals(Status.VIOLATION, r.status());
        assertEquals("b", r.key());
    }

    @Test
    void theViolationReportNamesTheOperationsThatCouldNotBePlaced() {
        LinearizabilityChecker.Result r = check(set(1, "a", "1", 1, 2), get(2, "a", "ghost", 3, 4));
        assertTrue(r.explanation().contains("ghost"), r.explanation());
    }

    @Test
    void whenTheBudgetRunsOutTheAnswerIsGaveUpNeverAGuess() {
        LinearizabilityChecker tiny = new LinearizabilityChecker(1);
        LinearizabilityChecker.Result r = tiny.check(List.of(
                set(1, "a", "1", 1, 2), get(1, "a", "1", 3, 4), get(1, "a", "1", 5, 6)));
        assertEquals(Status.GAVE_UP, r.status());
    }

    @Test
    void badArgumentsAreRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Operation.set(1, "a", "v", 5, 4));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Operation.get(1, "a", true, null, 1, 2));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LinearizabilityChecker(0));
    }

    // ---------------------------------------------------------------- generated histories

    /**
     * Builds a history that IS linearizable by construction: operations take effect, one at a time, at chosen points;
     * each one's interval is stretched around its point by a random amount, and the answers come from a plain map.
     */
    private static List<Operation> generateLinearizable(long seed, int operations, int keys, int width, boolean pending) {
        Random random = new Random(seed);
        Map<String, String> model = new HashMap<>();
        List<Operation> out = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            long point = 1_000 + i * 10L;
            long invoke = point - random.nextInt(width + 1);
            long returned = point + random.nextInt(width + 1);
            int client = 1 + random.nextInt(4);
            String key = "k" + random.nextInt(keys);
            int pick = random.nextInt(10);
            if (pick < 4) {
                String value = "v" + i;
                model.put(key, value);
                out.add(pending && random.nextInt(8) == 0
                        ? Operation.pendingSet(client, key, value, invoke)
                        : Operation.set(client, key, value, invoke, returned));
            } else if (pick < 8) {
                String value = model.get(key);
                out.add(Operation.get(client, key, value != null, value, invoke, returned));
            } else {
                boolean existed = model.remove(key) != null;
                out.add(pending && random.nextInt(8) == 0
                        ? Operation.pendingDelete(client, key, invoke)
                        : Operation.delete(client, key, existed, invoke, returned));
            }
        }
        return out;
    }

    @Test
    void randomHistoriesLinearizableByConstructionAreAccepted() {
        for (long seed = 1; seed <= 60; seed++) {
            for (int width : new int[] {0, 5, 25, 60}) {
                LinearizabilityChecker.Result r = checker.check(generateLinearizable(seed, 80, 3, width, seed % 2 == 0));
                assertTrue(r.ok(), "seed " + seed + " width " + width + ": " + r.explanation());
            }
        }
    }

    @Test
    void aReadOfAValueNeverWrittenIsAlwaysRejectedWhereverItIsInserted() {
        for (long seed = 1; seed <= 60; seed++) {
            List<Operation> history = generateLinearizable(seed, 80, 3, 25, false);
            Random random = new Random(seed);
            int at = random.nextInt(history.size());
            Operation donor = history.get(at);
            history.set(at, Operation.get(donor.client(), donor.key(), true, "never-written", donor.invoke(),
                    donor.returned()));
            assertEquals(Status.VIOLATION, checker.check(history).status(), "seed " + seed);
        }
    }

    @Test
    void aLongHistoryWithManyOverlapsIsDecidedQuickly() {
        List<Operation> history = generateLinearizable(7, 2_000, 1, 40, false);
        long start = System.nanoTime();
        LinearizabilityChecker.Result r = checker.check(history);
        long millis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(r.ok(), r.explanation());
        assertTrue(millis < 20_000, "took " + millis + " ms");
    }
}
