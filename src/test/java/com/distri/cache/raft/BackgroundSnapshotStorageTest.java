package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** {@code prepareSnapshot} / {@code commitSnapshot} on real files: the slow half can run beside appends and a crash loses nothing. */
class BackgroundSnapshotStorageTest {

    private static LogEntry e(long term, String c) {
        return new LogEntry(term, c.getBytes());
    }

    private static Path tempDir() throws IOException {
        return Files.createTempDirectory("raft-bg-snapshot-test");
    }

    private static void deleteTree(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static List<String> tail(RaftStorage s) {
        List<String> out = new ArrayList<>();
        for (long i = s.snapshotIndex() + 1; i <= s.lastIndex(); i++) {
            out.add(new String(s.entry(i).command()));
        }
        return out;
    }

    private static long strayFiles(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("raft.snapshot.bg.")).count();
        }
    }

    @Test
    void preparingChangesNothingTheStorageReportsOrKeeps() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c")));
            s.prepareSnapshot(2, 1, "state2".getBytes());
            assertEquals(0, s.snapshotIndex());
            assertNull(s.snapshot());
            assertEquals(List.of("a", "b", "c"), tail(s));
        }
        deleteTree(dir);
    }

    @Test
    void aCrashAfterPreparingButBeforeCommittingLeavesTheOldStateAndNoStrayFile() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c")));
            s.prepareSnapshot(2, 1, "state2".getBytes());
            assertEquals(1, strayFiles(dir));
        } // the process dies here
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            assertEquals(0, s.snapshotIndex());
            assertEquals(List.of("a", "b", "c"), tail(s));
            assertEquals(0, strayFiles(dir), "the uncommitted file is cleaned up at the next start");
        }
        deleteTree(dir);
    }

    @Test
    void committingInstallsTheSnapshotKeepsTheLaterEntriesAndSurvivesARestart() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c")));
            RaftStorage.PreparedSnapshot p = s.prepareSnapshot(2, 1, "state2".getBytes());
            s.append(List.of(e(2, "d"))); // written while the snapshot was being prepared
            assertTrue(s.commitSnapshot(p));
            assertEquals(2, s.snapshotIndex());
            assertEquals(1, s.snapshotTerm());
            assertArrayEquals("state2".getBytes(), s.snapshot().state());
            assertEquals(List.of("c", "d"), tail(s));
            assertEquals(0, strayFiles(dir));
            assertThrows(IndexOutOfBoundsException.class, () -> s.entry(1), "the covered entries were dropped from the log");
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            assertEquals(2, s.snapshotIndex());
            assertArrayEquals("state2".getBytes(), s.snapshot().state());
            assertEquals(List.of("c", "d"), tail(s));
        }
        deleteTree(dir);
    }

    @Test
    void aPreparedSnapshotOlderThanTheInstalledOneIsRefusedAndRemoved() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d")));
            RaftStorage.PreparedSnapshot old = s.prepareSnapshot(2, 1, "old".getBytes());
            s.installSnapshot(4, 2, "newer".getBytes()); // from a leader, meanwhile
            assertFalse(s.commitSnapshot(old));
            assertEquals(4, s.snapshotIndex());
            assertArrayEquals("newer".getBytes(), s.snapshot().state());
            assertEquals(0, strayFiles(dir));
        }
        deleteTree(dir);
    }

    @Test
    void twoPreparedSnapshotsDoNotShareAFileAndTheNewerCommitMakesTheOlderObsolete() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d")));
            RaftStorage.PreparedSnapshot first = s.prepareSnapshot(2, 1, "first".getBytes());
            RaftStorage.PreparedSnapshot second = s.prepareSnapshot(3, 2, "second".getBytes());
            assertEquals(2, strayFiles(dir));
            assertTrue(s.commitSnapshot(second));
            assertFalse(s.commitSnapshot(first));
            assertArrayEquals("second".getBytes(), s.snapshot().state());
            assertEquals(List.of("d"), tail(s));
        }
        deleteTree(dir);
    }

    @Test
    void preparingOnAnotherThreadWhileTheNodeKeepsAppendingCorruptsNothing() throws Exception {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            List<LogEntry> first = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                first.add(e(1, "e" + i));
            }
            s.append(first);
            byte[] big = new byte[8 * 1024 * 1024];
            java.util.Arrays.fill(big, (byte) 7);
            AtomicReference<RaftStorage.PreparedSnapshot> prepared = new AtomicReference<>();
            Thread worker = new Thread(() -> prepared.set(s.prepareSnapshot(40, 1, big)));
            worker.start();
            for (int i = 51; i <= 300; i++) { // the "node's thread" keeps appending, one entry at a time
                s.append(List.of(e(1, "e" + i)));
            }
            worker.join();
            assertTrue(s.commitSnapshot(prepared.get()));
            assertEquals(40, s.snapshotIndex());
            assertEquals(300, s.lastIndex());
            assertEquals("e41", new String(s.entry(41).command()));
            assertEquals("e300", new String(s.entry(300).command()));
        }
        try (FileRaftStorage s = new FileRaftStorage(dir, false)) {
            assertEquals(40, s.snapshotIndex());
            assertEquals(8 * 1024 * 1024, s.snapshot().state().length);
            assertEquals(300, s.lastIndex());
            assertEquals("e300", new String(s.entry(300).command()));
        }
        deleteTree(dir);
    }

    @Test
    void theDefaultStorageStillWorksThroughTheSameTwoSteps() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c")));
        RaftStorage.PreparedSnapshot p = s.prepareSnapshot(2, 1, "state2".getBytes());
        assertEquals(0, s.snapshotIndex());
        assertTrue(s.commitSnapshot(p));
        assertEquals(2, s.snapshotIndex());
        assertFalse(s.commitSnapshot(s.prepareSnapshot(1, 1, "x".getBytes())));
    }
}
