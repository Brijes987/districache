package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Snapshots in storage: what compaction keeps, what survives a restart, and what a crash in the middle leaves. */
class SnapshotStorageTest {

    private static LogEntry e(long term, String c) {
        return new LogEntry(term, c.getBytes());
    }

    private static Path tempDir() throws IOException {
        return Files.createTempDirectory("raft-snapshot-test");
    }

    private static void deleteTree(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static List<LogEntry> tail(RaftStorage s) {
        List<LogEntry> out = new ArrayList<>();
        for (long i = s.snapshotIndex() + 1; i <= s.lastIndex(); i++) {
            out.add(s.entry(i));
        }
        return out;
    }

    private static void fill(RaftStorage s) {
        s.saveTermAndVote(3, 1);
        s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d"), e(3, "e")));
    }

    // ---- behaviour shared by both implementations

    private static void checkSuffixKeptWhenTheLogHoldsTheSnapshotsLastEntry(RaftStorage s) {
        fill(s);
        s.installSnapshot(3, 2, "state3".getBytes());
        assertEquals(3, s.snapshotIndex());
        assertEquals(2, s.snapshotTerm());
        assertEquals(5, s.lastIndex());
        assertEquals(List.of(e(2, "d"), e(3, "e")), tail(s));
        assertEquals(2, s.termAt(3), "the term of the snapshot's last entry stays answerable");
        assertEquals(2, s.termAt(4));
        assertArrayEquals("state3".getBytes(), s.snapshot().state());
        assertThrows(IndexOutOfBoundsException.class, () -> s.entry(3));
        assertThrows(IndexOutOfBoundsException.class, () -> s.entry(1));
    }

    private static void checkWholeLogDiscardedWhenItDoesNotMatchTheSnapshot(RaftStorage s) {
        fill(s);
        s.installSnapshot(4, 9, "x".getBytes()); // we hold index 4, but with term 2: a different history
        assertEquals(4, s.snapshotIndex());
        assertEquals(4, s.lastIndex(), "nothing after the snapshot survives");
        assertEquals(List.of(), tail(s));
        assertEquals(9, s.termAt(4));
    }

    private static void checkSnapshotBeyondTheLogReplacesIt(RaftStorage s) {
        fill(s);
        s.installSnapshot(40, 5, "far".getBytes());
        assertEquals(40, s.lastIndex());
        assertEquals(List.of(), tail(s));
        s.append(List.of(e(5, "next")));
        assertEquals(41, s.lastIndex());
        assertEquals(e(5, "next"), s.entry(41));
    }

    private static void checkOlderOrEqualSnapshotIsIgnoredAndTruncationBelowItIsRefused(RaftStorage s) {
        fill(s);
        s.installSnapshot(3, 2, "new".getBytes());
        s.installSnapshot(2, 1, "old".getBytes());
        s.installSnapshot(3, 2, "dup".getBytes());
        assertEquals(3, s.snapshotIndex());
        assertArrayEquals("new".getBytes(), s.snapshot().state());
        assertThrows(IllegalStateException.class, () -> s.truncateFrom(3));
        s.truncateFrom(5); // the live part can still be truncated
        assertEquals(4, s.lastIndex());
    }

    private static void checkNoSnapshotMeansNullAndZero(RaftStorage s) {
        assertNull(s.snapshot());
        assertEquals(0, s.snapshotIndex());
        assertEquals(0, s.termAt(0));
    }

    @Test
    void inMemorySuffixKept() {
        checkSuffixKeptWhenTheLogHoldsTheSnapshotsLastEntry(new InMemoryRaftStorage());
    }

    @Test
    void inMemoryMismatchDiscards() {
        checkWholeLogDiscardedWhenItDoesNotMatchTheSnapshot(new InMemoryRaftStorage());
    }

    @Test
    void inMemoryBeyondLog() {
        checkSnapshotBeyondTheLogReplacesIt(new InMemoryRaftStorage());
    }

    @Test
    void inMemoryOlderIgnored() {
        checkOlderOrEqualSnapshotIsIgnoredAndTruncationBelowItIsRefused(new InMemoryRaftStorage());
        checkNoSnapshotMeansNullAndZero(new InMemoryRaftStorage());
    }

    @Test
    void fileSameAsInMemory() throws IOException {
        for (int variant = 0; variant < 4; variant++) {
            Path dir = tempDir();
            try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
                switch (variant) {
                    case 0 -> checkSuffixKeptWhenTheLogHoldsTheSnapshotsLastEntry(s);
                    case 1 -> checkWholeLogDiscardedWhenItDoesNotMatchTheSnapshot(s);
                    case 2 -> checkSnapshotBeyondTheLogReplacesIt(s);
                    default -> {
                        checkOlderOrEqualSnapshotIsIgnoredAndTruncationBelowItIsRefused(s);
                    }
                }
            } finally {
                deleteTree(dir);
            }
        }
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            checkNoSnapshotMeansNullAndZero(s);
        } finally {
            deleteTree(dir);
        }
    }

    // ---- what survives a restart

    @Test
    void aCompactedLogAndItsSnapshotSurviveReopeningAndTheLogCanKeepGrowing() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                s.installSnapshot(3, 2, "state3".getBytes());
                s.append(List.of(e(3, "f")));
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(3, s.snapshotIndex());
                assertEquals(2, s.snapshotTerm());
                assertEquals(6, s.lastIndex());
                assertEquals(List.of(e(2, "d"), e(3, "e"), e(3, "f")), tail(s));
                assertArrayEquals("state3".getBytes(), s.snapshot().state());
                assertEquals(3, s.currentTerm(), "term and vote are untouched by compaction");
                assertEquals(1, s.votedFor());
                s.truncateFrom(5);
                s.append(List.of(e(4, "g")));
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(List.of(e(2, "d"), e(4, "g")), tail(s));
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void compactingAgainAndAgainKeepsOnlyTheNewest() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                for (int round = 0; round < 5; round++) {
                    List<LogEntry> more = new ArrayList<>();
                    for (int i = 0; i < 10; i++) {
                        more.add(e(1, "r" + round + "-" + i));
                    }
                    s.append(more);
                    s.installSnapshot(s.lastIndex() - 2, 1, ("round" + round).getBytes());
                }
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(50, s.lastIndex());
                assertEquals(48, s.snapshotIndex());
                assertEquals(2, tail(s).size());
                assertArrayEquals("round4".getBytes(), s.snapshot().state());
            }
            assertTrue(Files.size(dir.resolve("raft.log")) < 200, "the file holds only the live tail");
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aTornTailAfterACompactionIsStillRepairedAndTheHeaderIsKept() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                s.installSnapshot(3, 2, "state3".getBytes());
                s.append(List.of(e(3, "f")));
            }
            Path log = dir.resolve("raft.log");
            byte[] bytes = Files.readAllBytes(log);
            Files.write(log, java.util.Arrays.copyOf(bytes, bytes.length - 3)); // cut the last record
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(3, s.snapshotIndex());
                assertEquals(List.of(e(2, "d"), e(3, "e")), tail(s));
                s.append(List.of(e(3, "again")));
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(List.of(e(2, "d"), e(3, "e"), e(3, "again")), tail(s));
            }
        } finally {
            deleteTree(dir);
        }
    }

    // ---- crashes in the middle of a compaction

    @Test
    void aCrashAfterTheSnapshotWasWrittenButBeforeTheLogWasRewrittenIsFinishedOnOpen() throws IOException {
        Path dir = tempDir();
        try {
            byte[] oldLog;
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                oldLog = Files.readAllBytes(dir.resolve("raft.log"));
                s.installSnapshot(3, 2, "state3".getBytes());
            }
            Files.write(dir.resolve("raft.log"), oldLog); // the rewrite "never happened"
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(3, s.snapshotIndex());
                assertEquals(5, s.lastIndex());
                assertEquals(List.of(e(2, "d"), e(3, "e")), tail(s));
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) { // and the finished state is stable
                assertEquals(List.of(e(2, "d"), e(3, "e")), tail(s));
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aCrashMidInstallOfASnapshotFromALeaderDiscardsTheOldLogOnOpen() throws IOException {
        Path dir = tempDir();
        try {
            byte[] oldLog;
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                oldLog = Files.readAllBytes(dir.resolve("raft.log"));
                s.installSnapshot(40, 5, "far".getBytes());
            }
            Files.write(dir.resolve("raft.log"), oldLog);
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(40, s.snapshotIndex());
                assertEquals(40, s.lastIndex());
                assertEquals(List.of(), tail(s));
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void leftoverTemporaryFilesFromACrashAreIgnored() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
            }
            Files.write(dir.resolve("raft.snapshot.tmp"), new byte[] {1, 2, 3});
            Files.write(dir.resolve("raft.log.tmp"), new byte[] {9});
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(5, s.lastIndex());
                assertEquals(0, s.snapshotIndex());
            }
            assertTrue(!Files.exists(dir.resolve("raft.snapshot.tmp")));
        } finally {
            deleteTree(dir);
        }
    }

    // ---- corruption is refused, never guessed around

    @Test
    void aCorruptSnapshotFileMakesOpeningFail() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                s.installSnapshot(3, 2, "state3".getBytes());
            }
            Path snap = dir.resolve("raft.snapshot");
            byte[] bytes = Files.readAllBytes(snap);
            bytes[bytes.length - 6] ^= 0x55; // inside the state
            Files.write(snap, bytes);
            assertThrows(IOException.class, () -> new FileRaftStorage(dir));
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aMissingSnapshotFileWithACompactedLogMakesOpeningFail() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                s.installSnapshot(3, 2, "state3".getBytes());
            }
            Files.delete(dir.resolve("raft.snapshot"));
            assertThrows(IOException.class, () -> new FileRaftStorage(dir));
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aCorruptLogHeaderMakesOpeningFail() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                fill(s);
                s.installSnapshot(3, 2, "state3".getBytes());
            }
            Path log = dir.resolve("raft.log");
            byte[] bytes = Files.readAllBytes(log);
            bytes[9] ^= 0x01; // inside the base index
            Files.write(log, bytes);
            assertThrows(IOException.class, () -> new FileRaftStorage(dir));
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aSnapshotLargerThanTheLimitIsRefused() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        fill(s);
        assertThrows(IllegalArgumentException.class,
                () -> s.installSnapshot(3, 2, new byte[RaftStorage.MAX_SNAPSHOT_BYTES + 1]));
    }
}
