package com.distri.cache.client;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.server.CacheServer;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CacheClientTest {

    private static CacheServer startServer(int capacity) throws IOException {
        return new CacheServer(new ShardedLRUCache<>(capacity, 4, 0), CacheServer.DEFAULT_HOST, 0, 16).start();
    }

    @Test
    void setGetDeleteRoundTrip() throws Exception {
        try (CacheServer server = startServer(100); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            assertEquals("PONG", c.ping());
            assertTrue(c.get("k").isEmpty());
            c.set("k", "v");
            assertEquals("v", c.get("k").orElseThrow());
            assertEquals(1, c.dbSize());
            assertTrue(c.delete("k"));
            assertFalse(c.delete("k"));
            assertTrue(c.get("k").isEmpty());
            assertEquals(0, c.dbSize());
        }
    }

    @Test
    void unicodeAndSpacesAndEmptyValuesSurvive() throws Exception {
        try (CacheServer server = startServer(100); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            c.set("नाम key", "Asha  नमस्ते\r\nline2");
            assertEquals("Asha  नमस्ते\r\nline2", c.get("नाम key").orElseThrow());
            c.set("empty", "");
            assertEquals("", c.get("empty").orElseThrow());
        }
    }

    @Test
    void ttlExpires() throws Exception {
        try (CacheServer server = startServer(100); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            c.set("k", "v", 300); // generous, so a slow machine cannot expire it before the first check
            assertEquals("v", c.get("k").orElseThrow());
            Thread.sleep(700);
            assertTrue(c.get("k").isEmpty());
        }
    }

    @Test
    void largeValueRoundTrips() throws Exception {
        try (CacheServer server = startServer(100); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            String big = "x".repeat(500_000);
            c.set("big", big);
            assertEquals(big, c.get("big").orElseThrow());
        }
    }

    @Test
    void oversizedArgumentIsRejectedBeforeAnythingIsSent() {
        // no server is running on this port: the check must happen before any connection attempt
        CacheClient c = new CacheClient("127.0.0.1", 6399, 200, 200);
        String tooBig = "x".repeat(CacheServer.MAX_ARGUMENT_BYTES + 1);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> c.set("k", tooBig));
        assertTrue(e.getMessage().contains("limit"));
        assertThrows(IllegalArgumentException.class, () -> c.get(tooBig));
        c.close();
    }

    @Test
    void argumentOfExactlyTheLimitIsAccepted() throws Exception {
        try (CacheServer server = startServer(100); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            String max = "y".repeat(CacheServer.MAX_ARGUMENT_BYTES);
            c.set("k", max);
            assertEquals(max, c.get("k").orElseThrow());
        }
    }

    @Test
    void validationRejectsBadArguments() throws Exception {
        try (CacheClient c = new CacheClient("127.0.0.1", 6380)) {
            assertThrows(IllegalArgumentException.class, () -> c.get(null));
            assertThrows(IllegalArgumentException.class, () -> c.set(null, "v"));
            assertThrows(IllegalArgumentException.class, () -> c.set("k", null));
            assertThrows(IllegalArgumentException.class, () -> c.set("k", "v", 0));
            assertThrows(IllegalArgumentException.class, () -> c.delete(null));
        }
        assertThrows(IllegalArgumentException.class, () -> new CacheClient("", 6380));
        assertThrows(IllegalArgumentException.class, () -> new CacheClient("h", 0));
        assertThrows(IllegalArgumentException.class, () -> new CacheClient("h", 70_000));
    }

    @Test
    void serverErrorBecomesAnExceptionButKeepsTheConnection() throws Exception {
        // a fake server that answers the first request with an error and the second with PONG
        try (ServerSocket fake = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            Thread t = new Thread(() -> {
                try (Socket s = fake.accept()) {
                    byte[] buf = new byte[256];
                    s.getInputStream().read(buf);
                    s.getOutputStream().write("-ERR boom\r\n".getBytes());
                    s.getInputStream().read(buf);
                    s.getOutputStream().write("+PONG\r\n".getBytes());
                    s.getOutputStream().flush();
                    Thread.sleep(300);
                } catch (Exception ignored) {
                    // the test asserts on the client side
                }
            });
            t.setDaemon(true);
            t.start();
            try (CacheClient c = new CacheClient("127.0.0.1", fake.getLocalPort(), 1000, 2000)) {
                CacheClientException e = assertThrows(CacheClientException.class, () -> c.get("k"));
                assertTrue(e.getMessage().contains("boom"));
                assertEquals("PONG", c.ping()); // same connection, still usable
            }
        }
    }

    @Test
    void timeoutThrowsAndTheClientRecovers() throws Exception {
        // a server that accepts but never answers the first connection, then behaves on the second
        try (ServerSocket fake = new ServerSocket(0, 2, java.net.InetAddress.getByName("127.0.0.1"))) {
            Thread t = new Thread(() -> {
                try {
                    Socket silent = fake.accept();
                    silent.getInputStream().read(new byte[256]); // swallow the request, never reply
                    Socket good = fake.accept();
                    good.getInputStream().read(new byte[256]);
                    good.getOutputStream().write("+PONG\r\n".getBytes());
                    good.getOutputStream().flush();
                    Thread.sleep(500);
                    silent.close();
                    good.close();
                } catch (Exception ignored) {
                    // the test asserts on the client side
                }
            });
            t.setDaemon(true);
            t.start();
            try (CacheClient c = new CacheClient("127.0.0.1", fake.getLocalPort(), 1000, 200)) {
                long start = System.nanoTime();
                assertThrows(CacheClientException.class, c::ping);
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 3000, "timeout was not honoured");
                assertEquals("PONG", c.ping()); // reconnected on a fresh connection
            }
        }
    }

    @Test
    void connectionRefusedIsReportedAndLaterCallsReconnect() throws Exception {
        CacheServer server = startServer(100);
        int port = server.port();
        CacheClient c = new CacheClient("127.0.0.1", port, 1000, 2000);
        c.set("k", "v");
        server.close();
        assertThrows(CacheClientException.class, () -> c.get("k"));
        // restart on the same port; retry binding briefly in case the OS has not released it yet
        CacheServer second = null;
        for (int i = 0; i < 40 && second == null; i++) {
            try {
                second = new CacheServer(new ShardedLRUCache<>(100, 4, 0), CacheServer.DEFAULT_HOST, port, 16).start();
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        assertNotNull(second, "could not rebind the port");
        try {
            assertTrue(c.get("k").isEmpty(), "the new server starts empty");
            c.set("k", "again");
            assertEquals("again", c.get("k").orElseThrow());
        } finally {
            second.close();
            c.close();
        }
    }

    @Test
    void closedClientRefusesFurtherUse() throws Exception {
        try (CacheServer server = startServer(100)) {
            CacheClient c = new CacheClient("127.0.0.1", server.port());
            c.ping();
            c.close();
            c.close(); // idempotent
            assertThrows(IllegalStateException.class, c::ping);
        }
    }

    @Test
    void oneClientSharedByManyThreadsStaysCorrect() throws Exception {
        try (CacheServer server = startServer(10_000); CacheClient c = new CacheClient("127.0.0.1", server.port())) {
            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                results.add(pool.submit(() -> {
                    for (int n = 0; n < 200; n++) {
                        String key = "t" + id + ":" + n;
                        c.set(key, "v" + n);
                        if (!("v" + n).equals(c.get(key).orElse(null))) return false;
                    }
                    return true;
                }));
            }
            for (Future<Boolean> f : results) assertTrue(f.get(60, TimeUnit.SECONDS));
            pool.shutdown();
            assertEquals(threads * 200L, c.dbSize());
        }
    }
}
