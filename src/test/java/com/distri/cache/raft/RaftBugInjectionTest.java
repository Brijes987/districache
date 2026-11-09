package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * Tests of the tests. A checker that never fails proves nothing, so these switch on real Raft mistakes and require
 * the chaos harness to notice. Two other mistakes (committing an old-term entry by counting, and committing past the
 * verified prefix) need an exact interleaving that random chaos does not reach; {@link RaftNodeTest} scripts those.
 */
class RaftBugInjectionTest {

    private static int violations(RaftConfig.Fault fault, Chaos.Profile profile, int seeds) {
        int found = 0;
        for (long seed = 1; seed <= seeds; seed++) {
            try {
                Chaos.fullRun(seed, 3_000, profile, RaftConfig.defaults().withFaults(fault));
            } catch (AssertionError e) {
                found++;
            }
        }
        return found;
    }

    @Test
    void votingWithoutTheLogUpToDateCheckIsCaught() {
        int found = violations(RaftConfig.Fault.SKIP_LOG_UP_TO_DATE_CHECK, Chaos.Profile.normal(), 40);
        assertTrue(found >= 20, "expected most seeds to expose the bug, only " + found + "/40 did");
    }

    @Test
    void truncatingTheLogOnEveryAppendIsCaught() {
        int found = violations(RaftConfig.Fault.TRUNCATE_ON_EVERY_APPEND, Chaos.Profile.normal(), 40);
        assertTrue(found >= 10, "expected many seeds to expose the bug, only " + found + "/40 did");
    }

    @Test
    void theCorrectImplementationIsNeverFlaggedOnTheSameSeeds() {
        for (long seed = 1; seed <= 40; seed++) {
            try {
                Chaos.fullRun(seed, 3_000, Chaos.Profile.normal(), RaftConfig.defaults());
            } catch (AssertionError e) {
                fail("false alarm on seed " + seed + ": " + e.getMessage());
            }
        }
    }
}
