package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Persistence: the append-only log, recovery after a restart, and compaction. */
class AppendOnlyLogTest {

    private static final AppendOnlyLog.FsyncPolicy NO_SYNC = AppendOnlyLog.FsyncPolicy.NO;

    private static Path newLogPath() throws IOException {
        Path dir = Files.createTempDirectory("districache-aof-test");
        dir.toFile().deleteOnExit();
        Path file = dir.resolve("test.aof");
        file.toFile().deleteOnExit();
        return file;
    }

    private static ShardedLRUCache<String, String> newCache(int capacity) {
        return new ShardedLRUCache<>(capacity, 1, 0);
    }

    private static CacheServer startServer(ShardedLRUCache<String, String> cache, AppendOnlyLog log)
            throws IOException {
        return new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16, log).start();
    }

    // ------------------------------------------------------------------ recovery

    @Test
    void writesSurviveARestart() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(100);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "a", "1");
            c.call("SET", "b", "2");
            c.call("SET", "a", "one");          // overwrite
            c.call("SET", "gone", "x");
            assertEquals(":", String.valueOf(c.call("DEL", "gone", "never-existed").type()));
        }
        log.close();

        ShardedLRUCache<String, String> restarted = newCache(100);
        AppendOnlyLog log2 = AppendOnlyLog.open(path, NO_SYNC, 0, restarted);
        assertEquals(Optional.of("one"), restarted.get("a"));
        assertEquals(Optional.of("2"), restarted.get("b"));
        assertEquals(Optional.empty(), restarted.get("gone"));
        assertEquals(2, restarted.size());
        assertEquals(0, log2.replayResult().truncatedBytes());
        log2.close();
    }

    @Test
    void aDeleteThatRemovedNothingIsNotLogged() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(10);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            c.call("DEL", "nothing");
        }
        long size = log.sizeBytes();
        log.close();
        assertEquals(0, size);
    }

    @Test
    void aTtlKeepsItsAbsoluteExpiryAcrossARestart() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(100);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "short", "v", "PX", "400");
            c.call("SET", "long", "v", "EX", "3600");
        }
        log.close();
        Thread.sleep(600); // the short TTL passes while the server is "down"

        ShardedLRUCache<String, String> restarted = newCache(100);
        AppendOnlyLog.open(path, NO_SYNC, 0, restarted).close();
        assertEquals(Optional.empty(), restarted.get("short"));
        assertEquals(Optional.of("v"), restarted.get("long"));
    }

    @Test
    void aReplayedPastExpiryRemovesAnEarlierValueOfTheSameKey() throws Exception {
        Path path = newLogPath();
        long past = System.currentTimeMillis() - 1000;
        Files.write(path, concat(
                AppendOnlyLog.encode("SET", "k", "old"),
                AppendOnlyLog.encode("SET", "k", "new", "PXAT", Long.toString(past))));
        ShardedLRUCache<String, String> cache = newCache(10);
        AppendOnlyLog.open(path, NO_SYNC, 0, cache).close();
        assertEquals(Optional.empty(), cache.get("k"));
    }

    @Test
    void anIncompleteLastRecordIsDroppedAndTheFileRepaired() throws Exception {
        Path path = newLogPath();
        byte[] whole = concat(AppendOnlyLog.encode("SET", "a", "1"), AppendOnlyLog.encode("SET", "b", "2"));
        byte[] third = AppendOnlyLog.encode("SET", "c", "333333");
        // every possible cut of the third record: it must always be dropped, the first two kept
        for (int cut = 1; cut < third.length; cut++) {
            Files.write(path, concat(whole, java.util.Arrays.copyOf(third, cut)));
            ShardedLRUCache<String, String> cache = newCache(10);
            AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
            assertEquals(2, log.replayResult().records(), "cut at " + cut);
            assertEquals(cut, log.replayResult().truncatedBytes(), "cut at " + cut);
            assertEquals(whole.length, Files.size(path), "file repaired, cut at " + cut);
            assertEquals(Optional.empty(), cache.get("c"));
            log.close();
        }
    }

    @Test
    void appendsAfterARepairAreReplayedToo() throws Exception {
        Path path = newLogPath();
        byte[] third = AppendOnlyLog.encode("SET", "c", "3");
        Files.write(path, concat(AppendOnlyLog.encode("SET", "a", "1"), java.util.Arrays.copyOf(third, third.length - 3)));
        ShardedLRUCache<String, String> cache = newCache(10);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        log.lock();
        try {
            log.append("SET", "d", "4");
        } finally {
            log.unlock();
        }
        log.close();
        ShardedLRUCache<String, String> again = newCache(10);
        AppendOnlyLog.open(path, NO_SYNC, 0, again).close();
        assertEquals(Optional.of("1"), again.get("a"));
        assertEquals(Optional.of("4"), again.get("d"));
        assertEquals(Optional.empty(), again.get("c"));
    }

    @Test
    void aMalformedRecordInTheMiddleStopsStartupInsteadOfLosingData() throws Exception {
        Path path = newLogPath();
        byte[] bytes = concat(
                AppendOnlyLog.encode("SET", "a", "1"),
                "*2\r\n$3\r\nSET\r\nX\r\n".getBytes(StandardCharsets.US_ASCII), // '$' expected
                AppendOnlyLog.encode("SET", "b", "2"));
        Files.write(path, bytes);
        IOException e = assertThrows(IOException.class, () -> AppendOnlyLog.open(path, NO_SYNC, 0, newCache(10)));
        assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
        assertEquals(bytes.length, Files.size(path), "the file must be left untouched");
    }

    @Test
    void aWellFormedButUnknownCommandIsRejectedToo() throws Exception {
        Path path = newLogPath();
        Files.write(path, AppendOnlyLog.encode("FLUSHALL"));
        assertThrows(IOException.class, () -> AppendOnlyLog.open(path, NO_SYNC, 0, newCache(10)));
    }

    // ------------------------------------------------------------------ compaction

    @Test
    void compactionShrinksTheLogAndKeepsTheData() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(100);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            for (int i = 0; i < 500; i++) {
                c.call("SET", "hot", "value-" + i);   // 500 overwrites of one key
            }
            c.call("SET", "ttl", "t", "EX", "3600");
            c.call("SET", "dead", "x");
            c.call("DEL", "dead");
            long before = log.sizeBytes();
            assertEquals(new RespTestClient.Reply('+', "OK"), c.call("REWRITEAOF"));
            long after = log.sizeBytes();
            assertTrue(after < before / 10, "log should shrink a lot: " + before + " -> " + after);
            c.call("SET", "after", "compaction"); // the log is still appendable
        }
        log.close();
        assertFalse(Files.exists(path.resolveSibling("test.aof.rewrite.tmp")));

        ShardedLRUCache<String, String> restarted = newCache(100);
        AppendOnlyLog.open(path, NO_SYNC, 0, restarted).close();
        assertEquals(Optional.of("value-499"), restarted.get("hot"));
        assertEquals(Optional.of("t"), restarted.get("ttl"));
        assertEquals(Optional.of("compaction"), restarted.get("after"));
        assertEquals(Optional.empty(), restarted.get("dead"));
        assertEquals(3, restarted.size());
    }

    @Test
    void compactionKeepsTheRelativeLruOrder() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(3);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "a", "1");
            c.call("SET", "b", "2");
            c.call("SET", "c", "3");
            c.call("GET", "a");                 // recency is now b (oldest), c, a (newest)
            c.call("REWRITEAOF");
        }
        log.close();
        ShardedLRUCache<String, String> restarted = newCache(3);
        AppendOnlyLog.open(path, NO_SYNC, 0, restarted).close();
        restarted.put("d", "4");                // evicts the least recently used
        assertEquals(Optional.empty(), restarted.get("b"));
        assertEquals(Optional.of("1"), restarted.get("a"));
        assertEquals(Optional.of("3"), restarted.get("c"));
    }

    @Test
    void theLogCompactsItselfPastTheThreshold() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(100);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 2_000, cache);
        long max = 0;
        try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
            for (int i = 0; i < 2_000; i++) {
                c.call("SET", "k" + (i % 5), "value-number-" + i);
                max = Math.max(max, log.sizeBytes());
            }
        }
        long finalSize = log.sizeBytes();
        log.close();
        // without compaction 2000 records of ~50 bytes would be ~100 KB
        assertTrue(max < 20_000, "log should stay small, peak was " + max);
        ShardedLRUCache<String, String> restarted = newCache(100);
        AppendOnlyLog.open(path, NO_SYNC, 0, restarted).close();
        assertEquals(5, restarted.size());
        assertEquals(Optional.of("value-number-1999"), restarted.get("k4"));
        assertTrue(finalSize > 0);
    }

    @Test
    void rewriteaofWithoutPersistenceIsAnError() throws Exception {
        ShardedLRUCache<String, String> cache = newCache(10);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 4).start();
                RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("REWRITEAOF").type());
        }
    }

    // ------------------------------------------------------------------ the log order is the apply order

    @Test
    void logOrderMatchesApplyOrderUnderConcurrentWrites() throws Exception {
        Path path = newLogPath();
        ShardedLRUCache<String, String> cache = newCache(1000);
        AppendOnlyLog log = AppendOnlyLog.open(path, NO_SYNC, 0, cache);
        int threads = 8;
        try (CacheServer server = startServer(cache, log)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                futures.add(pool.submit(() -> {
                    try (RespTestClient c = new RespTestClient(server.port())) {
                        java.util.Random rnd = new java.util.Random(id);
                        for (int i = 0; i < 400; i++) {
                            String key = "k" + rnd.nextInt(6); // few keys: heavy contention
                            if (rnd.nextInt(4) == 0) {
                                c.call("DEL", key);
                            } else {
                                c.call("SET", key, "t" + id + "-" + i);
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }
        log.close();
        // Replaying the log must reproduce exactly the final state the live cache reached.
        ShardedLRUCache<String, String> replayed = newCache(1000);
        AppendOnlyLog.open(path, NO_SYNC, 0, replayed).close();
        for (int k = 0; k < 6; k++) {
            assertEquals(cache.get("k" + k), replayed.get("k" + k), "key k" + k);
        }
        assertEquals(cache.size(), replayed.size());
    }

    // ------------------------------------------------------------------ fsync policies and PXAT

    @Test
    void everyFsyncPolicyWritesAndRecovers() throws Exception {
        for (AppendOnlyLog.FsyncPolicy policy : AppendOnlyLog.FsyncPolicy.values()) {
            Path path = newLogPath();
            ShardedLRUCache<String, String> cache = newCache(10);
            AppendOnlyLog log = AppendOnlyLog.open(path, policy, 0, cache);
            try (CacheServer server = startServer(cache, log); RespTestClient c = new RespTestClient(server.port())) {
                c.call("SET", "k", "v-" + policy);
            }
            log.close();
            ShardedLRUCache<String, String> restarted = newCache(10);
            AppendOnlyLog.open(path, policy, 0, restarted).close();
            assertEquals(Optional.of("v-" + policy), restarted.get("k"), policy.name());
        }
    }

    @Test
    void fsyncPolicyNamesAreParsedStrictly() {
        assertEquals(AppendOnlyLog.FsyncPolicy.ALWAYS, AppendOnlyLog.FsyncPolicy.parse("Always"));
        assertEquals(AppendOnlyLog.FsyncPolicy.EVERYSEC, AppendOnlyLog.FsyncPolicy.parse("everysec"));
        assertThrows(IllegalArgumentException.class, () -> AppendOnlyLog.FsyncPolicy.parse("sometimes"));
    }

    @Test
    void setAcceptsAnAbsoluteExpiryWithPxat() throws Exception {
        ShardedLRUCache<String, String> cache = newCache(10);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 4).start();
                RespTestClient c = new RespTestClient(server.port())) {
            long future = System.currentTimeMillis() + 60_000;
            assertEquals('+', c.call("SET", "k", "v", "PXAT", Long.toString(future)).type());
            assertEquals("v", c.call("GET", "k").text());
            c.call("SET", "p", "v");
            assertEquals('+', c.call("SET", "p", "v2", "PXAT", "1").type()); // far in the past
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "p"));
            assertEquals('-', c.call("SET", "x", "v", "PXAT", "0").type());
            assertEquals('-', c.call("SET", "x", "v", "PXAT", "abc").type());
        }
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) {
            n += p.length;
        }
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }
}
