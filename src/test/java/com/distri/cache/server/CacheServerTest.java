package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Starts a real server on a free port and talks to it over a real socket. */
class CacheServerTest {

    private static CacheServer start(int capacity, int shards, int maxClients) throws IOException {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(capacity, shards, 0);
        return new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, maxClients).start();
    }

    @Test
    void basicRoundTrip() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
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
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("NOPE").type());
            assertEquals('-', c.call("GET").type());
            assertEquals(new RespTestClient.Reply('+', "PONG"), c.call("PING"));
        }
    }

    @Test
    void ttlExpiresOverTheWire() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "k", "v", "PX", "300");
            assertEquals("v", c.call("GET", "k").text());
            Thread.sleep(700);
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "k"));
        }
    }

    @Test
    void evictsLeastRecentlyUsedWhenFull() throws Exception {
        try (CacheServer server = start(3, 1, 8); RespTestClient c = new RespTestClient(server.port())) {
            for (String k : new String[] {"a", "b", "c"}) c.call("SET", k, k);
            c.call("GET", "a");            // a becomes most recent
            c.call("SET", "d", "d");       // evicts b
            assertEquals("a", c.call("GET", "a").text());
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "b"));
        }
    }

    @Test
    void pipelinedCommandsAreAnsweredInOrder() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            java.io.ByteArrayOutputStream batch = new java.io.ByteArrayOutputStream();
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
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            byte[] all = RespTestClient.encode("SET", "key", "value");
            for (byte b : all) {
                c.sendRaw(new byte[] {b});
            }
            assertEquals(new RespTestClient.Reply('+', "OK"), c.read());
            assertEquals("value", c.call("GET", "key").text());
        }
    }

    @Test
    void inlineCommandWorks() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            c.sendRaw("PING\r\n".getBytes());
            assertEquals(new RespTestClient.Reply('+', "PONG"), c.read());
        }
    }

    @Test
    void protocolErrorClosesTheConnection() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            c.sendRaw("*1\r\nXYZ\r\n".getBytes());
            RespTestClient.Reply reply = c.read();
            assertEquals('-', reply.type());
            assertTrue(reply.text().startsWith("ERR Protocol error"));
            assertTrue(c.isClosedByServer());
        }
    }

    @Test
    void quitClosesTheConnection() throws Exception {
        try (CacheServer server = start(100, 4, 8); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals(new RespTestClient.Reply('+', "OK"), c.call("QUIT"));
            assertTrue(c.isClosedByServer());
        }
    }

    @Test
    void oneClientDisconnectingDoesNotAffectOthers() throws Exception {
        try (CacheServer server = start(100, 4, 8);
             RespTestClient a = new RespTestClient(server.port());
             RespTestClient b = new RespTestClient(server.port())) {
            a.call("SET", "shared", "yes");
            a.close();
            assertEquals("yes", b.call("GET", "shared").text());
        }
    }

    @Test
    void manyClientsAtOnce() throws Exception {
        int clients = 8;
        int perClient = 300;
        try (CacheServer server = start(10_000, 8, 32)) {
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                final int id = i;
                results.add(pool.submit(() -> {
                    try (RespTestClient c = new RespTestClient(server.port())) {
                        for (int n = 0; n < perClient; n++) {
                            String key = "c" + id + ":" + n;
                            if (!"OK".equals(c.call("SET", key, "v" + n).text())) return false;
                            if (!("v" + n).equals(c.call("GET", key).text())) return false;
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
    void connectionsBeyondTheLimitAreRejectedAndSlotsAreFreed() throws Exception {
        try (CacheServer server = start(100, 4, 1)) {
            RespTestClient first = new RespTestClient(server.port());
            assertEquals("PONG", first.call("PING").text()); // makes sure the first slot is taken
            try (RespTestClient second = new RespTestClient(server.port())) {
                RespTestClient.Reply reply = second.read();
                assertEquals('-', reply.type());
                assertTrue(reply.text().contains("max number of clients"));
                assertTrue(second.isClosedByServer());
            }
            first.close();
            // the slot must come back once the first client has gone
            boolean accepted = false;
            for (int i = 0; i < 50 && !accepted; i++) {
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
        CacheServer server = start(100, 4, 8);
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
            }
        });
    }

    @Test
    void bindsToLoopbackByDefault() throws Exception {
        try (CacheServer server = start(10, 1, 1)) {
            assertTrue(server.port() > 0);
            // the default host is the loopback address
            assertEquals("127.0.0.1", CacheServer.DEFAULT_HOST);
        }
    }

    @Test
    void invalidConstructorArguments() {
        assertThrows(IllegalArgumentException.class, () -> new CacheServer(null, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new CacheServer(new ShardedLRUCache<>(10, 1, 0), "127.0.0.1", 0, 0));
    }

    @Test
    void cannotBeStartedTwice() throws Exception {
        try (CacheServer server = start(10, 1, 1)) {
            assertThrows(IllegalStateException.class, server::start);
        }
    }
}
