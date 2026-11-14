package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

/**
 * Clients that retry through crashes, partitions and a lossy, duplicating network. Checks (on the real
 * {@link ReplicatedCache}): every server ends with identical data, and no request was executed more than once on any
 * server even though retries put duplicates into the log.
 *
 * <p>This does NOT check that the answers clients received were correct; {@link LinearizabilityChaosTest} does.
 */
class ReplicatedChaosTest {

    private static ReplicatedChaos.Result run(long seed, int steps, Path dir) {
        return ReplicatedChaos.run(seed, steps, Chaos.Profile.normal(), RaftConfig.defaults(), null, dir);
    }

    @Test
    void retriesNeverExecuteARequestTwiceAndAllServersEndIdentical() {
        int acknowledged = 0;
        int retries = 0;
        int duplicates = 0;
        for (long seed = 1; seed <= 30; seed++) {
            ReplicatedChaos.Result o = run(seed, 3_000, null);
            acknowledged += o.acknowledged;
            retries += o.retries;
            duplicates += o.duplicatesInLog;
        }
        assertTrue(acknowledged > 300, "the clients must get real work done, got " + acknowledged);
        assertTrue(retries > 50, "the faults must force retries, got " + retries);
        assertTrue(duplicates > 0, "the test must put duplicate requests into the log, or it proves nothing");
    }

    @Test
    void theSameHoldsWhenEveryServerRecoversFromFilesWithTornTailsAfterEachCrash() throws IOException {
        int acknowledged = 0;
        for (long seed = 101; seed <= 110; seed++) {
            Path dir = Files.createTempDirectory("raft-chaos");
            try {
                acknowledged += run(seed, 2_000, dir).acknowledged;
            } finally {
                try (var walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
        }
        assertTrue(acknowledged > 100, "got " + acknowledged);
    }

    @Test
    void theSameSeedGivesTheSameOutcome() {
        ReplicatedChaos.Result a = run(5, 2_000, null);
        ReplicatedChaos.Result b = run(5, 2_000, null);
        assertEquals(a.acknowledged, b.acknowledged);
        assertEquals(a.retries, b.retries);
        assertEquals(a.duplicatesInLog, b.duplicatesInLog);
        assertEquals(a.history.size(), b.history.size());
    }

    @Test
    void withGroupCommitRetriesStillNeverExecuteTwiceAndAllServersEndIdentical() {
        int acknowledged = 0;
        long batches = 0;
        long entries = 0;
        long largest = 0;
        for (long seed = 301; seed <= 330; seed++) {
            ReplicatedChaos.Result o = ReplicatedChaos.run(seed, 3_000, Chaos.Profile.normal(), RaftConfig.defaults(),
                    null, null, true, true);
            acknowledged += o.acknowledged;
            batches += o.batches;
            entries += o.batchedEntries;
            largest = Math.max(largest, o.largestBatch);
        }
        assertTrue(acknowledged > 300, "the clients must get real work done, got " + acknowledged);
        assertTrue(largest >= 2, "no batch ever held two requests, so group commit was not exercised: " + largest);
        assertTrue(entries > batches, "mean batch size must exceed 1: " + entries + " entries in " + batches);
    }

    @Test
    void withGroupCommitTheSameHoldsWhenServersRecoverFromFilesWithTornTails() throws IOException {
        int acknowledged = 0;
        for (long seed = 401; seed <= 410; seed++) {
            Path dir = Files.createTempDirectory("raft-chaos-gc");
            try {
                acknowledged += ReplicatedChaos.run(seed, 2_000, Chaos.Profile.brutal(), RaftConfig.defaults(), null,
                        dir, true, true).acknowledged;
            } finally {
                try (var walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
        }
        assertTrue(acknowledged > 100, "got " + acknowledged);
    }

    @Test
    void withSnapshotsReplicasStillAgreeAndNothingRunsTwiceAndLaggingFollowersAreCaughtUpBySnapshot() {
        int acknowledged = 0;
        long taken = 0;
        long installed = 0;
        for (long seed = 601; seed <= 640; seed++) {
            ReplicatedChaos.Result o = ReplicatedChaos.run(seed, 3_000, Chaos.Profile.normal(),
                    RaftConfig.defaults().withSnapshotEvery(10), null, null, true, seed % 2 == 0);
            acknowledged += o.acknowledged;
            taken += o.snapshotsTaken;
            installed += o.snapshotsInstalled;
        }
        assertTrue(acknowledged > 400, "got " + acknowledged);
        assertTrue(taken > 100, "snapshots must really be taken: " + taken);
        assertTrue(installed > 0, "no follower was ever caught up by a snapshot, so InstallSnapshot is untested");
    }

    @Test
    void withSnapshotsServersThatRestartFromFilesWithTornTailsComeBackCorrect() throws IOException {
        int acknowledged = 0;
        long taken = 0;
        for (long seed = 701; seed <= 716; seed++) {
            Path dir = Files.createTempDirectory("raft-chaos-snap");
            try {
                ReplicatedChaos.Result o = ReplicatedChaos.run(seed, 2_000, Chaos.Profile.brutal(),
                        RaftConfig.defaults().withSnapshotEvery(6), null, dir, true, true);
                acknowledged += o.acknowledged;
                taken += o.snapshotsTaken;
            } finally {
                try (var walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
        }
        assertTrue(acknowledged > 100, "got " + acknowledged);
        assertTrue(taken > 50, "snapshots must really be taken: " + taken);
    }

    private static int runsCaught(RaftConfig.Fault fault, int firstSeed, int lastSeed) {
        int caught = 0;
        for (long seed = firstSeed; seed <= lastSeed; seed++) {
            try {
                ReplicatedChaos.run(seed, 3_000, Chaos.Profile.brutal(),
                        RaftConfig.defaults().withSnapshotEvery(8).withFaults(fault), null, null, true, seed % 2 == 0);
            } catch (AssertionError | RuntimeException e) {
                caught++;
            }
        }
        return caught;
    }

    @Test
    void aSnapshotTakenBeforeTheLastEntryIsAppliedIsCaughtByTheReplicaChecks() {
        assertTrue(runsCaught(RaftConfig.Fault.SNAPSHOT_BEFORE_APPLY, 901, 930) > 0,
                "followers that install such a snapshot hold different data; the checks must notice");
    }

    @Test
    void installingASnapshotOlderThanWhatWasCommittedIsCaught() {
        assertTrue(runsCaught(RaftConfig.Fault.INSTALL_STALE_SNAPSHOT, 951, 990) > 0,
                "rolling a follower's state back must be noticed");
    }

    @Test
    void snapshotsSentInChunksStillLeaveReplicasInAgreementAndNothingRunsTwice() {
        int acknowledged = 0;
        long installed = 0;
        long chunks = 0;
        for (long seed = 1_301; seed <= 1_340; seed++) {
            ReplicatedChaos.Result o = ReplicatedChaos.run(seed, 3_000, Chaos.Profile.brutal(),
                    RaftConfig.defaults().withSnapshotEvery(10).withSnapshotChunkBytes(24), null, null, true,
                    seed % 2 == 0);
            acknowledged += o.acknowledged;
            installed += o.snapshotsInstalled;
            chunks += o.snapshotChunksReceived;
        }
        assertTrue(acknowledged > 300, "got " + acknowledged);
        assertTrue(installed > 0, "no follower was ever caught up by a snapshot");
        assertTrue(chunks > installed, "snapshots must really have been split: " + chunks + " chunks for " + installed
                + " installs");
    }

    @Test
    void aFollowerThatAppendsSnapshotChunksWhereverTheyArriveIsCaught() {
        int caught = 0;
        for (long seed = 1_401; seed <= 1_440; seed++) {
            try {
                ReplicatedChaos.run(seed, 3_000, Chaos.Profile.brutal(), RaftConfig.defaults().withSnapshotEvery(8)
                        .withSnapshotChunkBytes(24).withFaults(RaftConfig.Fault.SNAPSHOT_CHUNK_IGNORE_OFFSET), null, null,
                        true, seed % 2 == 0);
            } catch (AssertionError | RuntimeException e) {
                caught++;
            }
        }
        assertTrue(caught > 0, "duplicated or reordered chunks must corrupt some follower and the checks must notice");
    }

    @Test
    void replicasStillAgreeAndNothingRunsTwiceWhenSnapshotsAreFinishedInTheBackground() {
        int acknowledged = 0;
        long taken = 0;
        long installed = 0;
        for (long seed = 1_601; seed <= 1_640; seed++) {
            ReplicatedChaos.Result o = ReplicatedChaos.run(seed, 3_000, Chaos.Profile.brutal(),
                    RaftConfig.defaults().withSnapshotEvery(8), null, null, true, seed % 2 == 0,
                    ReplicatedCache.ReadMode.LOG, 30);
            acknowledged += o.acknowledged;
            taken += o.snapshotsTaken;
            installed += o.snapshotsInstalled;
        }
        assertTrue(acknowledged > 300, "got " + acknowledged);
        assertTrue(taken > 0, "no background snapshot was ever committed");
        assertTrue(installed > 0, "no follower was ever caught up by a snapshot");
    }
}
