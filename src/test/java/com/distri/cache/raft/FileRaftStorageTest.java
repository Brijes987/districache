package com.distri.cache.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class FileRaftStorageTest {

    private static Path tempDir() throws IOException {
        return Files.createTempDirectory("raft-storage-test");
    }

    private static void deleteTree(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static LogEntry entry(long term, String command) {
        return new LogEntry(term, command.getBytes());
    }

    private static List<LogEntry> contents(RaftStorage s) {
        List<LogEntry> out = new ArrayList<>();
        for (long i = 1; i <= s.lastIndex(); i++) {
            out.add(s.entry(i));
        }
        return out;
    }

    @Test
    void everythingSurvivesAReopen() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(0, s.currentTerm());
                assertEquals(-1, s.votedFor());
                assertEquals(0, s.lastIndex());
                s.saveTermAndVote(7, 2);
                s.append(List.of(entry(1, "a"), entry(1, "bb"), LogEntry.noop(3)));
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(7, s.currentTerm());
                assertEquals(2, s.votedFor());
                assertEquals(List.of(entry(1, "a"), entry(1, "bb"), LogEntry.noop(3)), contents(s));
                assertNull(s.droppedTailReason(), "a cleanly closed log has nothing to repair");
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void truncationIsDurableAndTheLogCanGrowAgain() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                s.append(List.of(entry(1, "a"), entry(1, "b"), entry(1, "c")));
                s.truncateFrom(2);
                assertEquals(1, s.lastIndex());
                s.append(List.of(entry(2, "x")));
                s.truncateFrom(99); // beyond the end: nothing to do
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(List.of(entry(1, "a"), entry(2, "x")), contents(s));
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aCrashAtAnyByteOfTheLogLeavesAWholePrefixAndTheLogStaysUsable() throws IOException {
        Path source = tempDir();
        try {
            List<LogEntry> all = List.of(entry(1, "alpha"), entry(2, ""), entry(2, "a longer command body"));
            try (FileRaftStorage s = new FileRaftStorage(source)) {
                s.append(all);
            }
            byte[] full = Files.readAllBytes(source.resolve("raft.log"));
            int previousCount = 0;
            for (int cut = 0; cut <= full.length; cut++) {
                Path dir = tempDir();
                try {
                    Files.write(dir.resolve("raft.log"), java.util.Arrays.copyOf(full, cut));
                    try (FileRaftStorage s = new FileRaftStorage(dir)) {
                        int count = (int) s.lastIndex();
                        assertTrue(count >= previousCount && count <= all.size(), "cut " + cut);
                        previousCount = count;
                        assertEquals(all.subList(0, count), contents(s), "cut " + cut);
                        s.append(List.of(entry(9, "after")));
                    }
                    try (FileRaftStorage s = new FileRaftStorage(dir)) {
                        assertEquals(entry(9, "after"), s.entry(s.lastIndex()), "cut " + cut);
                        assertNull(s.droppedTailReason(), "the repair must have been written back, cut " + cut);
                    }
                } finally {
                    deleteTree(dir);
                }
            }
            assertEquals(all.size(), previousCount, "the uncut file must load completely");
        } finally {
            deleteTree(source);
        }
    }

    @Test
    void aDamagedLastRecordIsDroppedButADamagedMiddleRecordRefusesToOpen() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                s.append(List.of(entry(1, "first"), entry(1, "second"), entry(1, "third")));
            }
            Path log = dir.resolve("raft.log");
            byte[] original = Files.readAllBytes(log);

            byte[] lastDamaged = original.clone();
            lastDamaged[lastDamaged.length - 1] ^= 0x40;
            Files.write(log, lastDamaged);
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(2, s.lastIndex());
                assertNotNull(s.droppedTailReason());
            }

            byte[] middleDamaged = original.clone();
            middleDamaged[8 + 8 + 2] ^= 0x40; // inside the first record's command; later records follow
            Files.write(log, middleDamaged);
            IOException e = assertThrows(IOException.class, () -> new FileRaftStorage(dir));
            assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aZeroFilledTailLeftByACrashIsDropped() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                s.append(List.of(entry(1, "keep")));
            }
            Path log = dir.resolve("raft.log");
            byte[] original = Files.readAllBytes(log);
            byte[] padded = java.util.Arrays.copyOf(original, original.length + 40);
            Files.write(log, padded);
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(List.of(entry(1, "keep")), contents(s));
                assertNotNull(s.droppedTailReason());
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void aCorruptMetaFileRefusesToOpenAndAStaleTempFileIsIgnored() throws IOException {
        Path dir = tempDir();
        try {
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                s.saveTermAndVote(5, 1);
            }
            Files.write(dir.resolve("raft.meta.tmp"), new byte[] {1, 2, 3}); // a crash mid-replace
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                assertEquals(5, s.currentTerm());
                assertEquals(1, s.votedFor());
            }
            assertFalse(Files.exists(dir.resolve("raft.meta.tmp")));

            Path meta = dir.resolve("raft.meta");
            byte[] bytes = Files.readAllBytes(meta);
            bytes[3] ^= 1;
            Files.write(meta, bytes);
            assertThrows(IOException.class, () -> new FileRaftStorage(dir));
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void behavesLikeTheInMemoryStorageUnderRandomOperationsAndReopens() throws IOException {
        for (long seed = 1; seed <= 25; seed++) {
            Random random = new Random(seed);
            Path dir = tempDir();
            try {
                InMemoryRaftStorage model = new InMemoryRaftStorage();
                FileRaftStorage real = new FileRaftStorage(dir);
                for (int step = 0; step < 150; step++) {
                    int op = random.nextInt(10);
                    if (op < 5) {
                        List<LogEntry> batch = new ArrayList<>();
                        for (int i = random.nextInt(4); i >= 0; i--) {
                            byte[] command = new byte[random.nextInt(30)];
                            random.nextBytes(command);
                            batch.add(new LogEntry(1 + random.nextInt(5), command));
                        }
                        model.append(batch);
                        real.append(batch);
                    } else if (op < 7) {
                        long from = 1 + random.nextInt(Math.max(1, (int) model.lastIndex() + 1));
                        model.truncateFrom(from);
                        real.truncateFrom(from);
                    } else if (op < 9) {
                        long term = random.nextInt(50);
                        int vote = random.nextInt(4) - 1;
                        model.saveTermAndVote(term, vote);
                        real.saveTermAndVote(term, vote);
                    } else {
                        real.close();
                        real = new FileRaftStorage(dir);
                    }
                    assertEquals(model.lastIndex(), real.lastIndex(), "seed " + seed + " step " + step);
                    assertEquals(model.currentTerm(), real.currentTerm());
                    assertEquals(model.votedFor(), real.votedFor());
                    assertEquals(contents(model), contents(real), "seed " + seed + " step " + step);
                }
                real.close();
            } finally {
                deleteTree(dir);
            }
        }
    }

    @Test
    void aVoteSurvivesARestartSoTheServerCannotVoteTwiceInOneTerm() throws IOException {
        Path dir = tempDir();
        try {
            List<RaftMessage> sent = new ArrayList<>();
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                RaftNode node = new RaftNode(2, List.of(0, 1), s, (to, m) -> sent.add(m), (i, c) -> { },
                        RaftConfig.defaults(), new Random(1));
                node.receive(0, new RaftMessage.RequestVote(4, 0, 0, 0));
                assertTrue(((RaftMessage.RequestVoteReply) sent.get(sent.size() - 1)).voteGranted());
            }
            try (FileRaftStorage s = new FileRaftStorage(dir)) {
                RaftNode node = new RaftNode(2, List.of(0, 1), s, (to, m) -> sent.add(m), (i, c) -> { },
                        RaftConfig.defaults(), new Random(1));
                assertEquals(4, node.term());
                node.receive(1, new RaftMessage.RequestVote(4, 1, 0, 0));
                assertFalse(((RaftMessage.RequestVoteReply) sent.get(sent.size() - 1)).voteGranted(),
                        "the vote for server 0 was forgotten by the restart");
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void anAbsurdlyLargeCommandIsRejectedBeforeAnythingIsWritten() throws IOException {
        Path dir = tempDir();
        try (FileRaftStorage s = new FileRaftStorage(dir)) {
            byte[] big = new byte[FileRaftStorage.MAX_COMMAND_BYTES + 1];
            assertThrows(IllegalArgumentException.class, () -> s.append(List.of(new LogEntry(1, big))));
            assertEquals(0, s.lastIndex());
        } finally {
            deleteTree(dir);
        }
    }
}
