package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The event-loop server over real sockets, and a differential test against the blocking server. */
class NioCacheServerTest {

    private static NioCacheServer start(int capacity, int shards, int maxClients, int ioThreads) throws IOException {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(capacity, shards, 0);
        return new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, maxClients).ioThreads(ioThreads).start();
    }

    private static NioCacheServer start(int maxClients) throws IOException {
        return start(1000, 4, maxClients, 2);
    }

    // ---------------------------------------------------------------- basics (same checks as CacheServerTest)

    @Test
    void basicRoundTrip() throws Exception {
        try (NioCacheServer server = start(8); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals(new RespTestClient.Reply('+', "PONG"), c.call("PING"));
            assertEquals(new RespTestClient.Reply('+', "OK"), c.call("SET", "k", "v"));
            assertEquals(new RespTestClient.Reply('$', "v"), c.call("GET", "k"));
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "missing"));
            assertEquals(new RespTestClient.Reply(':', "1"), c.call("DEL", "k"));
            assertEquals(new RespTestClient.Reply(':', "0"), c.call("DBSIZE"));
        }
    }

    @Test
    void errorsKeepTheConnectionOpen() throws Exception {
        try (NioCacheServer server = start(8); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("NOPE").type());
            assertEquals('-', c.call("GET").type());
            assertEquals(new RespTestClient.Reply('+', "PONG"), c.call("PING"));
        }
    }

    @Test
    void ttlExpiresOverTheWire() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 50);
        try (NioCacheServer server = new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, 8).start();
                RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "k", "v", "PX", "300");
            assertEquals("v", c.call("GET", "k").text());
            Thread.sleep(700);
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void pipelinedCommandsAreAnsweredInOrder() throws Exception {
        try (NioCacheServer server = start(8); RespTestClient c = new RespTestClient(server.port())) {
            ByteArrayOutputStream batch = new ByteArrayOutputStream();
            batch.writeBytes(RespTestClient.encode("SET", "a", "1"));
            batch.writeBytes(RespTestClient.encode("GET", "a"));
            batch.writeBytes(RespTestClient.encode("DBSIZE"));
            batch.writeBytes(RespTestClient.encode("PING"));
            c.sendRaw(batch.toByteArray());
            assertEquals(new RespTestClient.Reply('+', "OK"), c.read());
            assertEquals(new RespTestClient.Reply('$', "1"), c.read());
            assertEquals(new RespTestClient.Reply(':', "1"), c.read());
            assertEquals(new RespTestClient.Reply('+', "PONG"), c.read());
        }
    }

    @Test
    void commandSplitAcrossSeveralWritesIsReassembled() throws Exception {
        try (NioCacheServer server = start(8); RespTestClient c = new RespTestClient(server.port())) {
            for (byte b : RespTestClient.encode("SET", "key", "value")) {
                c.sendRaw(new byte[] {b});
                Thread.sleep(0, 200_000);
            }
            assertEquals(new RespTestClient.Reply('+', "OK"), c.read());
            assertEquals("value", c.call("GET", "key").text());
        }
    }

    @Test
    void aOneMegabyteValueRoundTrips() throws Exception {
        String big = "x".repeat(1 << 20);
        try (NioCacheServer server = start(8); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals("OK", c.call("SET", "big", big).text());
            assertEquals(big, c.call("GET", "big").text());
        }
    }

    // ---------------------------------------------------------------- limits and lifecycle

    @Test
    void connectionsBeyondTheLimitAreRejectedAndSlotsAreFreed() throws Exception {
        try (NioCacheServer server = start(1)) {
            RespTestClient first = new RespTestClient(server.port());
            assertEquals("PONG", first.call("PING").text());
            try (RespTestClient second = new RespTestClient(server.port())) {
                RespTestClient.Reply reply = second.read();
                assertEquals('-', reply.type());
                assertTrue(reply.text().contains("max number of clients"));
                assertTrue(second.isClosedByServer());
            }
            first.close();
            boolean accepted = false;
            for (int i = 0; i < 100 && !accepted; i++) {
                try (RespTestClient third = new RespTestClient(server.port())) {
                    third.send("PING");
                    accepted = "PONG".equals(third.read().text());
                }
                if (!accepted) Thread.sleep(50);
            }
            assertTrue(accepted, "slot was not released after the client disconnected");
        }
    }

    @Test
    void closeStopsTheServerAndDropsClients() throws Exception {
        NioCacheServer server = start(8);
        int port = server.port();
        RespTestClient c = new RespTestClient(port);
        assertEquals("PONG", c.call("PING").text());
        server.close();
        server.close(); // idempotent
        assertTrue(c.isClosedByServer());
        c.close();
        assertThrows(IOException.class, () -> {
            try (Socket s = new Socket("127.0.0.1", port)) {
                s.getOutputStream().write(1);
                s.getInputStream().read();
            }
        });
    }

    @Test
    void theConnectionCountFollowsConnectsAndDisconnects() throws Exception {
        try (NioCacheServer server = start(50)) {
            List<RespTestClient> clients = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                RespTestClient c = new RespTestClient(server.port());
                c.call("PING");
                clients.add(c);
            }
            assertEquals(20, server.connectionCount());
            for (RespTestClient c : clients) {
                c.close();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (server.connectionCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, server.connectionCount());
        }
    }

    @Test
    void invalidConstructorArgumentsAndLifecycle() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 1, 0);
        assertThrows(IllegalArgumentException.class, () -> new NioCacheServer(null, "127.0.0.1", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new NioCacheServer(cache, "127.0.0.1", 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new NioCacheServer(cache, "127.0.0.1", 0, 1).ioThreads(0));
        assertThrows(IllegalArgumentException.class, () -> new NioCacheServer(cache, "127.0.0.1", 0, 1,
                new CacheServer.Security(null, javax.net.ssl.SSLContext.getDefault())));
        try (NioCacheServer server = start(1)) {
            assertThrows(IllegalStateException.class, server::start);
            assertThrows(IllegalStateException.class, () -> server.ioThreads(2));
        }
    }

    // ---------------------------------------------------------------- what an event loop must get right

    @Test
    void manyIdleConnectionsDoNotNeedManyThreads() throws Exception {
        int before = Thread.getAllStackTraces().size();
        try (NioCacheServer server = start(1000, 4, 600, 2)) {
            List<RespTestClient> clients = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                RespTestClient c = new RespTestClient(server.port());
                assertEquals("PONG", c.call("PING").text());
                clients.add(c);
            }
            assertEquals(500, server.connectionCount());
            int during = Thread.getAllStackTraces().size();
            // 2 loops + 1 acceptor; the client side holds no threads either. Allow a few for the JVM and test runner.
            assertTrue(during - before <= 10, "threads went from " + before + " to " + during);
            // and every one of them is still served
            for (RespTestClient c : clients) {
                assertEquals("PONG", c.call("PING").text());
            }
            for (RespTestClient c : clients) {
                c.close();
            }
        }
    }

    @Test
    void severalLoopsServeConcurrentClientsCorrectly() throws Exception {
        try (NioCacheServer server = start(100_000, 16, 64, 4)) {
            int clients = 8;
            int perClient = 400;
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int t = 0; t < clients; t++) {
                final int id = t;
                results.add(pool.submit(() -> {
                    try (RespTestClient c = new RespTestClient(server.port())) {
                        for (int i = 0; i < perClient; i++) {
                            String k = "c" + id + "-" + i;
                            if (!"OK".equals(c.call("SET", k, "v" + i).text())) return false;
                            if (!("v" + i).equals(c.call("GET", k).text())) return false;
                        }
                        return true;
                    }
                }));
            }
            for (Future<Boolean> f : results) {
                assertTrue(f.get(60, TimeUnit.SECONDS), "a client saw a wrong reply");
            }
            pool.shutdown();
            try (RespTestClient c = new RespTestClient(server.port())) {
                assertEquals(String.valueOf(clients * perClient), c.call("DBSIZE").text());
            }
        }
    }

    @Test
    void aClientThatDoesNotReadItsRepliesIsBoundedAndDoesNotStallOthers() throws Exception {
        try (NioCacheServer server = start(1000, 4, 8, 1)) { // ONE loop: a stall would hit everyone
            String value = "v".repeat(100_000);
            try (RespTestClient setup = new RespTestClient(server.port())) {
                assertEquals("OK", setup.call("SET", "big", value).text());
            }
            int requests = 300; // 30 MB of replies, far above the 256 KiB high-water mark
            Socket slow = new Socket();
            slow.connect(new InetSocketAddress("127.0.0.1", server.port()), 3000);
            slow.setSoTimeout(10_000);
            ByteArrayOutputStream batch = new ByteArrayOutputStream();
            for (int i = 0; i < requests; i++) {
                batch.writeBytes(RespTestClient.encode("GET", "big"));
            }
            Thread writer = new Thread(() -> {
                try {
                    slow.getOutputStream().write(batch.toByteArray());
                    slow.getOutputStream().flush();
                } catch (IOException ignored) {
                    // the test ends the socket
                }
            });
            writer.setDaemon(true);
            writer.start();
            Thread.sleep(500); // the slow client now has far more replies owed than the server will queue
            // another client on the same single loop is answered promptly
            try (RespTestClient other = new RespTestClient(server.port())) {
                long t0 = System.nanoTime();
                assertEquals("PONG", other.call("PING").text());
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) < 2000);
            }
            // and the slow client, once it reads, gets every reply, whole and in order
            InputStream in = slow.getInputStream();
            long expectedBytes = (long) requests * ("$100000\r\n".length() + 100_000 + 2);
            long got = 0;
            byte[] buf = new byte[65536];
            while (got < expectedBytes) {
                int n = in.read(buf);
                assertTrue(n > 0, "connection ended after " + got + " of " + expectedBytes);
                got += n;
            }
            assertEquals(expectedBytes, got);
            slow.close();
            writer.join(5000);
        }
    }

    // ---------------------------------------------------------------- password handling without sleeping

    private static NioCacheServer startWithPassword(int ioThreads) throws IOException {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 2, 0);
        return new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16,
                new CacheServer.Security("secret", null)).ioThreads(ioThreads).start();
    }

    @Test
    void commandsAreRefusedUntilAuthenticated() throws Exception {
        try (NioCacheServer server = startWithPassword(1); RespTestClient c = new RespTestClient(server.port())) {
            RespTestClient.Reply r = c.call("GET", "k");
            assertEquals('-', r.type());
            assertTrue(r.text().startsWith("NOAUTH"));
            assertEquals("OK", c.call("AUTH", "secret").text());
            assertEquals("OK", c.call("SET", "k", "v").text());
        }
    }

    @Test
    void aFailedAuthIsDelayedButDoesNotBlockOtherConnectionsOnTheSameLoop() throws Exception {
        try (NioCacheServer server = startWithPassword(1); // one loop: sleeping would stall B
                RespTestClient a = new RespTestClient(server.port());
                RespTestClient b = new RespTestClient(server.port())) {
            assertEquals("OK", b.call("AUTH", "secret").text());
            long t0 = System.nanoTime();
            a.send("AUTH", "wrong");
            Thread.sleep(30); // let the loop see A's failed attempt
            long b0 = System.nanoTime();
            assertEquals("PONG", b.call("PING").text());
            long bMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - b0);
            RespTestClient.Reply wrong = a.read();
            long aMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertEquals('-', wrong.type());
            assertTrue(wrong.text().startsWith("WRONGPASS"));
            assertTrue(bMillis < CommandExecutor.FAILED_AUTH_DELAY_MILLIS / 2, "B waited " + bMillis + " ms");
            assertTrue(aMillis >= CommandExecutor.FAILED_AUTH_DELAY_MILLIS - 20, "A got its reply after " + aMillis + " ms");
        }
    }

    @Test
    void tooManyFailedAttemptsCloseTheConnection() throws Exception {
        try (NioCacheServer server = startWithPassword(1); RespTestClient c = new RespTestClient(server.port())) {
            for (int i = 0; i < CommandExecutor.MAX_FAILED_AUTH_ATTEMPTS; i++) {
                assertTrue(c.call("AUTH", "nope").text().startsWith("WRONGPASS"));
            }
            assertTrue(c.isClosedByServer());
        }
    }

    @Test
    void aConnectionThatNeverAuthenticatesIsClosed() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 2, 0);
        NioCacheServer server = new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16,
                new CacheServer.Security("secret", null));
        server.setPreAuthTimeoutMillis(300);
        server.start();
        try (RespTestClient silent = new RespTestClient(server.port())) {
            long t0 = System.nanoTime();
            assertTrue(silent.isClosedByServer());
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(ms >= 200 && ms < 3000, "closed after " + ms + " ms");
        } finally {
            server.close();
        }
    }

    @Test
    void anAuthenticatedConnectionIsNotClosedByThePreAuthTimeout() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 2, 0);
        NioCacheServer server = new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16,
                new CacheServer.Security("secret", null));
        server.setPreAuthTimeoutMillis(300);
        server.start();
        try (RespTestClient c = new RespTestClient(server.port())) {
            assertEquals("OK", c.call("AUTH", "secret").text());
            Thread.sleep(800);
            assertEquals("PONG", c.call("PING").text());
        } finally {
            server.close();
        }
    }

    // ---------------------------------------------------------------- closing without losing the last reply

    @Test
    void anErrorReplyIsDeliveredEvenWhenTheClientKeepsSendingAfterIt() throws Exception {
        try (NioCacheServer server = start(8); Socket s = new Socket("127.0.0.1", server.port())) {
            s.setSoTimeout(5000);
            s.getOutputStream().write("*abc\r\n".getBytes());
            s.getOutputStream().write(new byte[2 << 20]); // far more than the server will ever parse
            s.shutdownOutput();
            assertEquals("-ERR Protocol error: invalid multibulk length\r\n", new String(s.getInputStream().readAllBytes()));
        }
    }

    @Test
    void aConnectionTheServerHasDecidedToCloseIsDroppedEvenIfTheClientNeverClosesItsEnd() throws Exception {
        try (NioCacheServer server = start(8); Socket s = new Socket("127.0.0.1", server.port())) {
            s.setSoTimeout(5000);
            s.getOutputStream().write(RespTestClient.encode("QUIT"));
            assertEquals("+OK\r\n", new String(s.getInputStream().readAllBytes())); // end-of-stream arrives at once
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (server.connectionCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(0, server.connectionCount(), "the server kept a connection it had decided to close");
        }
    }

    // ---------------------------------------------------------------- persistence

    @Test
    void writesAreLoggedAndSurviveARestart() throws Exception {
        Path dir = Files.createTempDirectory("districache-nio-aof");
        Path file = dir.resolve("test.aof");
        AppendOnlyLog.FsyncPolicy noSync = AppendOnlyLog.FsyncPolicy.NO;
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 1, 0);
        AppendOnlyLog log = AppendOnlyLog.open(file, noSync, 0, cache);
        try (NioCacheServer server = new NioCacheServer(cache, CacheServer.DEFAULT_HOST, 0, 8, log,
                CacheServer.Security.NONE).start(); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "a", "1");
            c.call("SET", "a", "one");
            c.call("SET", "gone", "x");
            c.call("DEL", "gone");
        }
        log.close();
        ShardedLRUCache<String, String> restarted = new ShardedLRUCache<>(100, 1, 0);
        AppendOnlyLog log2 = AppendOnlyLog.open(file, noSync, 0, restarted);
        assertEquals(Optional.of("one"), restarted.get("a"));
        assertEquals(Optional.empty(), restarted.get("gone"));
        assertEquals(1, restarted.size());
        log2.close();
    }

    // ---------------------------------------------------------------- differential test

    /** Sends {@code input}, signals end of input, and returns every byte the server sends back until it closes. */
    private static byte[] exchange(int port, byte[] input) throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 3000);
            s.setSoTimeout(10_000);
            Thread writer = new Thread(() -> {
                try {
                    s.getOutputStream().write(input);
                    s.getOutputStream().flush();
                    s.shutdownOutput();
                } catch (IOException ignored) {
                    // the server may close first on a protocol error
                }
            });
            writer.setDaemon(true);
            writer.start();
            ByteArrayOutputStream got = new ByteArrayOutputStream();
            byte[] buf = new byte[65536];
            try {
                int n;
                while ((n = s.getInputStream().read(buf)) > 0) {
                    got.write(buf, 0, n);
                }
            } catch (java.net.SocketException reset) {
                // a reset after a protocol error: whatever arrived before it still counts
            }
            writer.join(5000);
            return got.toByteArray();
        }
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            b.writeBytes(p);
        }
        return b.toByteArray();
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    @Test
    void theEventLoopServerAnswersByteForByteLikeTheBlockingServer() throws Exception {
        String big = "q".repeat(1 << 20);
        StringBuilder nineMeg = new StringBuilder("*9\r\n");
        for (int i = 0; i < 9; i++) {
            nineMeg.append("$").append(RespReader.MAX_BULK_BYTES).append("\r\n")
                    .append("x".repeat(RespReader.MAX_BULK_BYTES)).append("\r\n");
        }
        List<byte[]> cases = new ArrayList<>(Arrays.asList(
                RespTestClient.encode("PING"),
                RespTestClient.encode("PING", "hello"),
                RespTestClient.encode("ECHO", "hi"),
                RespTestClient.encode("ECHO"),
                cat(RespTestClient.encode("SET", "a", "1"), RespTestClient.encode("GET", "a"),
                        RespTestClient.encode("DBSIZE"), RespTestClient.encode("DEL", "a", "zz"),
                        RespTestClient.encode("GET", "a")),
                cat(RespTestClient.encode("SET", "k", "v", "EX", "100"), RespTestClient.encode("GET", "k")),
                cat(RespTestClient.encode("SET", "k", "v", "EX", "zero"), RespTestClient.encode("SET", "k")),
                cat(RespTestClient.encode("SET", "big", big), RespTestClient.encode("GET", "big")),
                cat(RespTestClient.encode("SET", "a", "\u00e9\u4e2d\u6587"), RespTestClient.encode("GET", "a")),
                RespTestClient.encode("NOPE", "x"),
                RespTestClient.encode("AUTH", "x"),
                RespTestClient.encode("PSYNC", "?", "0"),
                RespTestClient.encode("REPLINFO"),
                cat(RespTestClient.encode("PING"), RespTestClient.encode("QUIT"), RespTestClient.encode("PING")),
                ascii("PING\r\nPING extra\r\n  ECHO   spaced   \r\n\r\nPING\r\n"),
                ascii("PING\nPING\n"),
                ascii("*0\r\n*0\r\nPING\r\n"),
                ascii("*1\r\n$0\r\n\r\n"),
                ascii("*abc\r\n"),
                ascii("*-1\r\nPING\r\n"),
                ascii("*" + (RespReader.MAX_ARGS + 1) + "\r\n"),
                ascii("*1\r\n:5\r\nPING\r\n"),
                ascii("*1\r\n$x\r\n"),
                ascii("*1\r\n$-1\r\n"),
                ascii("*1\r\n$" + (RespReader.MAX_BULK_BYTES + 1) + "\r\n"),
                ascii("*1\r\n$3\r\nabcXY"),
                ascii("*1\r\n$3\r\nabc\r\n*1\r\n$4\r\nPING\r\n"),
                ascii("x".repeat(RespReader.MAX_LINE_BYTES + 10)),
                ascii("*" + "9".repeat(RespReader.MAX_LINE_BYTES + 10)),
                ascii("a ".repeat(RespReader.MAX_ARGS + 5) + "\r\n"),
                ascii(nineMeg.toString()),
                cat(RespTestClient.encode("PING"), ascii("*2\r\n$4\r\nECHO\r\n$10\r\nshort")), // ends mid-command
                cat(RespTestClient.encode("PING"), ascii("*2\r\n$4\r\nEC")),
                new byte[0]));
        // a long pipeline
        ByteArrayOutputStream pipeline = new ByteArrayOutputStream();
        for (int i = 0; i < 2000; i++) {
            pipeline.writeBytes(RespTestClient.encode("SET", "k" + i, "v" + i));
            pipeline.writeBytes(RespTestClient.encode("GET", "k" + i));
        }
        cases.add(pipeline.toByteArray());

        int n = 0;
        for (byte[] input : cases) {
            ShardedLRUCache<String, String> c1 = new ShardedLRUCache<>(10_000, 4, 0);
            ShardedLRUCache<String, String> c2 = new ShardedLRUCache<>(10_000, 4, 0);
            try (CacheServer blocking = new CacheServer(c1, CacheServer.DEFAULT_HOST, 0, 8).start();
                    NioCacheServer nio = new NioCacheServer(c2, CacheServer.DEFAULT_HOST, 0, 8).start()) {
                byte[] expected = exchange(blocking.port(), input);
                byte[] actual = exchange(nio.port(), input);
                if (!Arrays.equals(expected, actual)) {
                    String head = new String(input, 0, Math.min(input.length, 60), StandardCharsets.ISO_8859_1);
                    fail("case " + n + " (" + head.replace("\r", "\\r").replace("\n", "\\n") + "...): blocking server sent "
                            + expected.length + " bytes, event-loop server sent " + actual.length + "\nblocking: "
                            + preview(expected) + "\nevent loop: " + preview(actual));
                }
            }
            n++;
        }
    }

    private static String preview(byte[] b) {
        return new String(b, 0, Math.min(b.length, 120), StandardCharsets.ISO_8859_1).replace("\r", "\\r").replace("\n", "\\n");
    }
}
