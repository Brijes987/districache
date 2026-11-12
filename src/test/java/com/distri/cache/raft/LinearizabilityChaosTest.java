package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.LinearizabilityChecker.Status;
import com.distri.cache.linearizability.Operation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

/**
 * Records what every client did and saw while servers crash, partitions come and go and the network loses and
 * duplicates messages, then asks the checker whether those histories could have come from a single, correct,
 * one-at-a-time key-value map. Then switches on deliberate bugs and requires the checker to notice.
 */
class LinearizabilityChaosTest {

    private final LinearizabilityChecker checker = new LinearizabilityChecker();

    private LinearizabilityChecker.Result run(long seed, Chaos.Profile profile, boolean checkQuorum,
            ReplicatedCache.Fault fault, Path dir) {
        ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile,
                RaftConfig.defaults().withCheckQuorum(checkQuorum), fault, dir, fault == null);
        return checker.check(r.history);
    }

    @Test
    void theCorrectSystemIsLinearizableUnderFaultsAcross120Runs() {
        int reads = 0;
        int writes = 0;
        for (long seed = 1; seed <= 60; seed++) {
            for (Chaos.Profile profile : new Chaos.Profile[] {Chaos.Profile.normal(), Chaos.Profile.brutal()}) {
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile,
                        RaftConfig.defaults().withCheckQuorum(seed % 3 != 0), null, null);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
                for (Operation op : r.history) {
                    if (op.kind() == Operation.Kind.GET) {
                        reads++;
                    } else {
                        writes++;
                    }
                }
            }
        }
        assertTrue(reads > 5_000 && writes > 10_000, "too little checked: " + reads + " reads, " + writes + " writes");
    }

    @Test
    void theCorrectSystemIsAlsoLinearizableWhenServersRecoverFromFilesWithTornTails() throws IOException {
        for (long seed = 201; seed <= 206; seed++) {
            Path dir = Files.createTempDirectory("raft-lin");
            try {
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 2_500, Chaos.Profile.brutal(),
                        RaftConfig.defaults(), null, dir);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
            } finally {
                try (var walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
        }
    }

    @Test
    void aLeaderAnsweringReadsFromItsOwnCopyIsCaught() {
        int violations = 0;
        String report = "";
        for (long seed = 1; seed <= 60; seed++) {
            LinearizabilityChecker.Result res = run(seed, Chaos.Profile.brutal(), true, ReplicatedCache.Fault.LOCAL_READS, null);
            assertTrue(res.status() != Status.GAVE_UP, "the checker must decide, seed " + seed);
            if (res.status() == Status.VIOLATION) {
                violations++;
                report = res.explanation();
            }
        }
        assertTrue(violations >= 10, "a stale-read bug should be caught in many runs, got " + violations + "/60");
        assertTrue(report.contains("NOT LINEARIZABLE"), report);
    }

    @Test
    void aLeaderAcknowledgingBeforeTheEntryIsAppliedIsCaught() {
        int violations = 0;
        for (long seed = 1; seed <= 30; seed++) {
            LinearizabilityChecker.Result res = run(seed, Chaos.Profile.normal(), true, ReplicatedCache.Fault.ACK_BEFORE_COMMIT, null);
            if (res.status() == Status.VIOLATION) {
                violations++;
            }
        }
        assertTrue(violations >= 25, "got " + violations + "/30");
    }

    @Test
    void withGroupCommitTheSystemIsStillLinearizableUnderFaults() {
        long largest = 0;
        int ops = 0;
        for (long seed = 501; seed <= 560; seed++) {
            for (Chaos.Profile profile : new Chaos.Profile[] {Chaos.Profile.normal(), Chaos.Profile.brutal()}) {
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile,
                        RaftConfig.defaults().withCheckQuorum(seed % 3 != 0), null, null, true, true);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
                largest = Math.max(largest, r.largestBatch);
                ops += r.history.size();
            }
        }
        assertTrue(largest >= 2, "group commit must actually have batched: " + largest);
        assertTrue(ops > 10_000, "too little checked: " + ops);
    }

    @Test
    void withSnapshotsTheSystemIsStillLinearizableUnderFaults() {
        long installed = 0;
        int ops = 0;
        for (long seed = 801; seed <= 840; seed++) {
            for (Chaos.Profile profile : new Chaos.Profile[] {Chaos.Profile.normal(), Chaos.Profile.brutal()}) {
                ReplicatedChaos.Result r = ReplicatedChaos.run(seed, 3_000, profile,
                        RaftConfig.defaults().withSnapshotEvery(10), null, null, true, seed % 2 == 1);
                LinearizabilityChecker.Result res = checker.check(r.history);
                assertEquals(Status.LINEARIZABLE, res.status(), "seed " + seed + "\n" + res.explanation());
                installed += r.snapshotsInstalled;
                ops += r.history.size();
            }
        }
        assertTrue(installed > 0, "InstallSnapshot must have happened in some run");
        assertTrue(ops > 8_000, "too little checked: " + ops);
    }
}
