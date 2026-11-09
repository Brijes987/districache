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
}
