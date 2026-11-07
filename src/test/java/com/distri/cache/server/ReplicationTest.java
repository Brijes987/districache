package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.CacheSnapshotEntry;
import com.distri.cache.ShardedLRUCache;
import com.distri.cache.client.CacheClient;
import com.distri.cache.client.ClientSecurity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Primary/replica replication over real sockets. All timers are shortened so failures are noticed quickly. */
class ReplicationTest {

    private static final String PASSWORD = "repl-pass";

    /** A cache + replication + server, started on a free (or given) port. */
    private static final class Node implements AutoCloseable {
        final ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10_000, 4, 0);
        final Replication replication;
        final CacheServer server;
        final AppendOnlyLog log;

        Node(Replication.Config config, String password, javax.net.ssl.SSLContext tls, int port, AppendOnlyLog log,
                ShardedLRUCache<String, String> existing) throws IOException {
            this.log = log;
            this.replication = new Replication(cache, log, config);
            this.server = new CacheServer(cache, CacheServer.DEFAULT_HOST, port, 64, log,
                    new CacheServer.Security(password, tls), replication).start();
        }

        static Node start() throws IOException {
            return new Node(Replication.Config.fast(), null, null, 0, null, null);
        }

        static Node start(Replication.Config config) throws IOException {
            return new Node(config, null, null, 0, null, null);
        }

        int port() {
            return server.port();
        }

        RespTestClient client() throws IOException {
            return new RespTestClient(port());
        }

        /** Makes this node a replica of {@code primary} by sending REPLICAOF over the wire. */
        void replicateFrom(Node primary) throws IOException {
            try (RespTestClient c = client()) {
                assertEquals('+', c.call("REPLICAOF", "127.0.0.1", Integer.toString(primary.port())).type());
            }
        }

        ReplicaLink link() {
            return replication.link();
        }

        @Override
        public void close() {
            server.close();
            replication.close();
            cache.shutdown();
        }
    }

    // ------------------------------------------------------------------ helpers

    @FunctionalInterface
    private interface Check {
        boolean test() throws Exception;
    }

    private static void await(String what, Check condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("timed out waiting for: " + what);
    }

    /** key -> value, with the absolute expiry kept separately. */
    private static Map<String, CacheSnapshotEntry<String, String>> state(ShardedLRUCache<String, String> cache) {
        Map<String, CacheSnapshotEntry<String, String>> m = new HashMap<>();
        for (CacheSnapshotEntry<String, String> e : cache.snapshot()) {
            m.put(e.key(), e);
        }
        return m;
    }

    /** Same keys, same values, and expiry times within 2 s (the replica applies PXAT a few ms later). */
    private static boolean sameContents(ShardedLRUCache<String, String> a, ShardedLRUCache<String, String> b) {
        Map<String, CacheSnapshotEntry<String, String>> x = state(a);
        Map<String, CacheSnapshotEntry<String, String>> y = state(b);
        if (!x.keySet().equals(y.keySet())) {
            return false;
        }
        for (String k : x.keySet()) {
            CacheSnapshotEntry<String, String> e1 = x.get(k);
            CacheSnapshotEntry<String, String> e2 = y.get(k);
            if (!e1.value().equals(e2.value())) {
                return false;
            }
            if ((e1.expiresAtMillis() > 0) != (e2.expiresAtMillis() > 0)) {
                return false;
            }
            if (e1.expiresAtMillis() > 0 && Math.abs(e1.expiresAtMillis() - e2.expiresAtMillis()) > 2_000) {
                return false;
            }
        }
        return true;
    }

    private static void awaitSame(Node primary, Node replica) throws Exception {
        await("replica to match the primary", () -> sameContents(primary.cache, replica.cache));
    }

    /** Waits until the replica has applied every byte the primary has published (needed when the end state is empty). */
    private static void awaitCaughtUp(Node primary, Node replica) throws Exception {
        await("replica to apply the whole stream",
                () -> replica.link().offset() == primary.replication.hub().offset());
    }

    private static Map<String, String> info(Node node) throws IOException {
        try (RespTestClient c = node.client()) {
            String text = c.call("REPLINFO").text();
            Map<String, String> m = new HashMap<>();
            for (String line : text.split("\n")) {
                int colon = line.indexOf(':');
                if (colon > 0) {
                    m.put(line.substring(0, colon), line.substring(colon + 1));
                }
            }
            return m;
        }
    }

    private static void set(Node n, String k, String v) throws IOException {
        try (RespTestClient c = n.client()) {
            assertEquals(new RespTestClient.Reply('+', "OK"), c.call("SET", k, v));
        }
    }

    // ------------------------------------------------------------------ full resync

    @Test
    void aNewReplicaGetsAFullCopyIncludingTtls() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            try (RespTestClient c = primary.client()) {
                c.call("SET", "a", "1");
                c.call("SET", "b", "2");
                c.call("SET", "ttl", "t", "EX", "3600");
                c.call("SET", "gone", "x");
                c.call("DEL", "gone");
            }
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            assertEquals(3, replica.cache.size());
            assertEquals(Optional.of("t"), replica.cache.get("ttl"));
            assertTrue(state(replica.cache).get("ttl").expiresAtMillis() > System.currentTimeMillis() + 3_000_000);
            assertEquals(1, replica.link().fullSyncs());
            assertEquals(0, replica.link().partialSyncs());
        }
    }

    @Test
    void aReplicaThrowsAwayItsOldDataOnAFullResync() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            set(replica, "stale", "old"); // written while it was still a primary
            set(primary, "fresh", "new");
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            assertEquals(Optional.empty(), replica.cache.get("stale"));
            assertEquals(Optional.of("new"), replica.cache.get("fresh"));
        }
    }

    @Test
    void anExpiredEntryIsNotCopied() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            try (RespTestClient c = primary.client()) {
                c.call("SET", "short", "v", "PX", "150");
                c.call("SET", "keep", "v");
            }
            Thread.sleep(400);
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            assertEquals(Optional.empty(), replica.cache.get("short"));
            assertEquals(Optional.of("v"), replica.cache.get("keep"));
        }
    }

    // ------------------------------------------------------------------ the live stream

    @Test
    void writesAfterTheSyncReachTheReplicaInOrder() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link() != null && replica.link().isConnected());
            try (RespTestClient c = primary.client()) {
                c.call("SET", "k", "v1");
                c.call("SET", "k", "v2");
                c.call("SET", "other", "x");
                c.call("DEL", "other", "missing");
                c.call("SET", "k", "v3", "EX", "3600");
            }
            awaitSame(primary, replica);
            assertEquals(Optional.of("v3"), replica.cache.get("k"));
            assertEquals(Optional.empty(), replica.cache.get("other"));
        }
    }

    @Test
    void aWriteThatTargetsAnAlreadyPassedExpiryDeletesTheKeyOnTheReplicaToo() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            try (RespTestClient c = primary.client()) {
                c.call("SET", "k", "v");
                c.call("SET", "k", "v2", "PXAT", "1");
            }
            awaitCaughtUp(primary, replica); // not awaitSame: both ends are empty, which is also where it started
            assertEquals(Optional.empty(), primary.cache.get("k"));
            assertEquals(Optional.empty(), replica.cache.get("k"));
        }
    }

    @Test
    void replicaOffsetsMatchThePrimaryAndLagIsReported() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            for (int i = 0; i < 50; i++) {
                set(primary, "k" + i, "v" + i);
            }
            awaitSame(primary, replica);
            // offsets are bytes of the stream; the replica reports the same number once caught up
            await("offsets to match", () -> Long.parseLong(info(primary).get("offset")) == replica.link().offset());
            await("ack to arrive", () -> info(primary).get("replica0") != null
                    && info(primary).get("replica0").contains("lag_bytes=0"));
            assertEquals("primary", info(primary).get("role"));
            assertEquals("1", info(primary).get("replicas"));
            assertEquals("replica", info(replica).get("role"));
            assertEquals("up", info(replica).get("link"));
            assertEquals(info(primary).get("replid"), info(replica).get("replid"));
        }
    }

    @Test
    void anIdleLinkSurvivesLongerThanTheReadTimeoutBecauseOfHeartbeats() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            Thread.sleep(3_500); // more than twice the 1.5 s read timeout, with no writes at all
            assertTrue(replica.link().isConnected());
            assertEquals(1, replica.link().connections(), "it must not have reconnected");
            long before = replica.link().offset();
            assertTrue(before > 0, "heartbeats advance the offset");
        }
    }

    @Test
    void concurrentWritersAndALateReplicaStillConverge() throws Exception {
        // The replica attaches while 4 writers are busy: the copy and the stream must join with no gap and no repeat.
        try (Node primary = Node.start(); Node replica = Node.start()) {
            ExecutorService pool = Executors.newFixedThreadPool(4);
            List<Future<?>> futures = new ArrayList<>();
            java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            for (int t = 0; t < 4; t++) {
                final int id = t;
                futures.add(pool.submit(() -> {
                    try (RespTestClient c = primary.client()) {
                        Random rnd = new Random(id);
                        int i = 0;
                        while (!stop.get()) {
                            String key = "k" + rnd.nextInt(40);
                            if (rnd.nextInt(4) == 0) {
                                c.call("DEL", key);
                            } else {
                                c.call("SET", key, "t" + id + "-" + (i++));
                            }
                        }
                    }
                    return null;
                }));
            }
            Thread.sleep(300);
            replica.replicateFrom(primary);
            Thread.sleep(700);
            stop.set(true);
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();
            awaitSame(primary, replica);
            await("offsets to match", () -> Long.parseLong(info(primary).get("offset")) == replica.link().offset());
            assertEquals(1, replica.link().fullSyncs());
        }
    }

    // ------------------------------------------------------------------ resuming

    @Test
    void aDroppedConnectionResumesWithoutACopy() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            set(primary, "before", "1");
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            awaitSame(primary, replica);
            primary.replication.hub().dropReplicaConnections();
            set(primary, "during-gap", "2");
            set(primary, "before", "changed");
            await("reconnect with a partial resync", () -> replica.link().partialSyncs() == 1);
            awaitSame(primary, replica);
            assertEquals(1, replica.link().fullSyncs(), "only the first connection needed a copy");
            assertEquals(Optional.of("changed"), replica.cache.get("before"));
            assertEquals(Optional.of("2"), replica.cache.get("during-gap"));
        }
    }

    @Test
    void aReplicaThatFellOutOfTheBacklogGetsAFullCopy() throws Exception {
        Replication.Config tinyBacklog = Replication.Config.fast().withBacklog(300);
        try (Node primary = Node.start(tinyBacklog); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            primary.replication.hub().dropReplicaConnections();
            for (int i = 0; i < 100; i++) { // far more than 300 bytes while the replica is away
                set(primary, "k" + i, "value-" + i);
            }
            await("a second full copy", () -> replica.link().fullSyncs() == 2);
            awaitSame(primary, replica);
            assertEquals(100, replica.cache.size());
        }
    }

    @Test
    void aReplicaKeepsServingWhenThePrimaryDiesAndFullySyncsWithAnyNewPrimaryOnThatPort() throws Exception {
        Node primary = Node.start();
        int port = primary.port();
        try (Node replica = Node.start()) {
            set(primary, "old", "1");
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            primary.close();
            await("link down", () -> !replica.link().isConnected());
            assertEquals(Optional.of("1"), replica.cache.get("old")); // still readable, possibly stale
            try (RespTestClient c = replica.client()) {
                assertEquals("1", c.call("GET", "old").text());
            }
            // a different server (new history) now answers on the same address
            try (Node second = new Node(Replication.Config.fast(), null, null, port, null, null)) {
                set(second, "new", "2");
                await("full copy from the new primary", () -> replica.link().isConnected()
                        && replica.link().fullSyncs() == 2);
                awaitSame(second, replica);
                assertEquals(Optional.empty(), replica.cache.get("old"));
            }
        }
    }

    // ------------------------------------------------------------------ roles

    @Test
    void aReplicaRefusesClientWritesButServesReads() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            set(primary, "k", "v");
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            try (RespTestClient c = replica.client()) {
                assertEquals("v", c.call("GET", "k").text());
                RespTestClient.Reply set = c.call("SET", "k", "other");
                assertEquals('-', set.type());
                assertTrue(set.text().startsWith("READONLY"), set.text());
                assertTrue(c.call("DEL", "k").text().startsWith("READONLY"));
                assertEquals("v", c.call("GET", "k").text()); // unchanged
            }
        }
    }

    @Test
    void aReplicaDoesNotAcceptReplicasOfItsOwn() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start()) {
            replica.replicateFrom(primary);
            await("link up", () -> replica.link().isConnected());
            try (RespTestClient c = replica.client()) {
                RespTestClient.Reply r = c.call("PSYNC", "?", "-1");
                assertEquals('-', r.type());
                assertTrue(r.text().contains("replica"), r.text());
            }
        }
    }

    @Test
    void promotingAReplicaMakesItWritableWithANewHistory() throws Exception {
        try (Node primary = Node.start(); Node replica = Node.start(); Node second = Node.start()) {
            set(primary, "k", "v");
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            String oldId = info(replica).get("replid");

            try (RespTestClient c = replica.client()) {
                assertEquals(new RespTestClient.Reply('+', "OK"), c.call("REPLICAOF", "NO", "ONE"));
                assertEquals(new RespTestClient.Reply('+', "OK"), c.call("SET", "after", "promotion"));
            }
            assertEquals("primary", info(replica).get("role"));
            assertNotEquals(oldId, info(replica).get("replid"));
            assertEquals(Optional.of("v"), replica.cache.get("k")); // it kept what it had

            second.replicateFrom(replica); // and can feed a new replica
            awaitSame(replica, second);
            assertEquals(Optional.of("promotion"), second.cache.get("after"));
            // the old primary is unaffected and independent
            assertEquals(Optional.empty(), primary.cache.get("after"));
        }
    }

    @Test
    void aPrimaryCanBeTurnedIntoAReplica() throws Exception {
        try (Node a = Node.start(); Node b = Node.start()) {
            set(a, "from-a", "1");
            set(b, "from-b", "2");
            a.replicateFrom(b); // a's own data is replaced by b's
            awaitSame(b, a);
            assertEquals(Optional.empty(), a.cache.get("from-a"));
            assertEquals(Optional.of("2"), a.cache.get("from-b"));
        }
    }

    @Test
    void replicationCommandsAreErrorsWhenReplicationIsNotEnabled() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 2, 0);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 4).start();
                RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("PSYNC", "?", "-1").type());
            assertEquals('-', c.call("REPLICAOF", "NO", "ONE").type());
            assertTrue(c.call("REPLINFO").text().contains("replication:disabled"));
        }
    }

    @Test
    void badReplicationArgumentsAreErrors() throws Exception {
        try (Node n = Node.start(); RespTestClient c = n.client()) {
            assertEquals('-', c.call("REPLICAOF", "host", "notaport").type());
            assertEquals('-', c.call("REPLICAOF", "host", "0").type());
            assertEquals('-', c.call("REPLICAOF", "host", "70000").type());
            assertEquals('-', c.call("REPLICAOF", "one-arg").type());
            assertEquals('-', c.call("PSYNC", "?", "abc").type());
            assertEquals('-', c.call("REPLCONF", "ACK", "5").type()); // only valid on a replication connection
        }
    }

    // ------------------------------------------------------------------ security of the link

    @Test
    void theLinkAuthenticatesWithThePrimaryPassword() throws Exception {
        try (Node primary = new Node(Replication.Config.fast(), PASSWORD, null, 0, null, null);
                Node replica = Node.start(Replication.Config.fast().withSecurity(ClientSecurity.password(PASSWORD)))) {
            try (CacheClient c = new CacheClient("127.0.0.1", primary.port(), 3000, 5000, ClientSecurity.password(PASSWORD))) {
                c.set("k", "v");
            }
            replica.replicateFrom(primary);
            awaitSame(primary, replica);
            assertEquals(Optional.of("v"), replica.cache.get("k"));
        }
    }

    @Test
    void aWrongOrMissingLinkPasswordNeverReplicates() throws Exception {
        try (Node primary = new Node(Replication.Config.fast(), PASSWORD, null, 0, null, null);
                Node wrong = Node.start(Replication.Config.fast().withSecurity(ClientSecurity.password("nope")));
                Node none = Node.start()) {
            try (CacheClient c = new CacheClient("127.0.0.1", primary.port(), 3000, 5000, ClientSecurity.password(PASSWORD))) {
                c.set("secret", "data");
            }
            wrong.replicateFrom(primary);
            none.replicateFrom(primary);
            Thread.sleep(1_500);
            assertFalse(wrong.link().isConnected());
            assertFalse(none.link().isConnected());
            assertEquals(0, wrong.cache.size(), "no data may leak without the password");
            assertEquals(0, none.cache.size());
            assertEquals(0, primary.replication.hub().replicaCount());
            assertFalse(info(wrong).get("last_error") == null);
        }
    }

    @Test
    void thePsyncCommandNeedsAuthenticationLikeAnyOther() throws Exception {
        try (Node primary = new Node(Replication.Config.fast(), PASSWORD, null, 0, null, null);
                RespTestClient c = primary.client()) {
            RespTestClient.Reply r = c.call("PSYNC", "?", "-1");
            assertTrue(r.text().startsWith("NOAUTH"), r.text());
            assertEquals(0, primary.replication.hub().replicaCount());
        }
    }

    @Test
    void theLinkCanUseTlsAndAPassword() throws Exception {
        Path keystore = Paths.get(ReplicationTest.class.getResource("/tls/server.p12").toURI());
        Path trust = Paths.get(ReplicationTest.class.getResource("/tls/trust.p12").toURI());
        char[] storePassword = "changeit".toCharArray();
        ClientSecurity linkSecurity = ClientSecurity.tls(trust, storePassword, PASSWORD);
        try (Node primary = new Node(Replication.Config.fast(), PASSWORD, Tls.serverContext(keystore, storePassword),
                0, null, null);
                Node replica = Node.start(Replication.Config.fast().withSecurity(linkSecurity))) {
            try (CacheClient c = new CacheClient("localhost", primary.port(), 3000, 5000, linkSecurity)) {
                c.set("k", "over-tls");
            }
            // the certificate is for "localhost", so the replica must connect by that name
            try (RespTestClient c = replica.client()) {
                assertEquals('+', c.call("REPLICAOF", "localhost", Integer.toString(primary.port())).type());
            }
            awaitSame(primary, replica);
            assertEquals(Optional.of("over-tls"), replica.cache.get("k"));
        }
    }

    // ------------------------------------------------------------------ with persistence

    @Test
    void aReplicaWithItsOwnLogRecoversLocallyAndTheLogMatchesTheCopy() throws Exception {
        Path dir = Files.createTempDirectory("districache-repl-aof");
        Path file = dir.resolve("replica.aof");
        file.toFile().deleteOnExit();
        dir.toFile().deleteOnExit();
        try (Node primary = Node.start()) {
            set(primary, "a", "1");
            set(primary, "b", "2");
            ShardedLRUCache<String, String> replicaCache = new ShardedLRUCache<>(10_000, 4, 0);
            AppendOnlyLog log = AppendOnlyLog.open(file, AppendOnlyLog.FsyncPolicy.NO, 0, replicaCache);
            Replication replication = new Replication(replicaCache, log, Replication.Config.fast());
            try (CacheServer server = new CacheServer(replicaCache, CacheServer.DEFAULT_HOST, 0, 8, log,
                    CacheServer.Security.NONE, replication).start()) {
                replication.replicaOf("127.0.0.1", primary.port());
                await("copy", () -> sameContents(primary.cache, replicaCache) && replicaCache.size() == 2);
                try (RespTestClient c = primary.client()) {
                    c.call("DEL", "a");
                    c.call("SET", "c", "3");
                }
                await("stream", () -> sameContents(primary.cache, replicaCache));
            } finally {
                replication.close();
                log.close();
                replicaCache.shutdown();
            }
            // a restart of the replica alone, before it reaches the primary again: its own log has the data
            ShardedLRUCache<String, String> restarted = new ShardedLRUCache<>(10_000, 4, 0);
            AppendOnlyLog.open(file, AppendOnlyLog.FsyncPolicy.NO, 0, restarted).close();
            assertEquals(Optional.empty(), restarted.get("a"));
            assertEquals(Optional.of("2"), restarted.get("b"));
            assertEquals(Optional.of("3"), restarted.get("c"));
            assertEquals(2, restarted.size());
        }
    }

    // ------------------------------------------------------------------ the hub on its own

    @Test
    void theStreamOffsetCountsBytesAndTheBacklogIsBounded() {
        try (ReplicationHub hub = new ReplicationHub(100, 60_000, 60_000)) {
            String id = hub.replid();
            assertEquals(40, id.length());
            assertEquals(0, hub.offset());
            byte[] record = AppendOnlyLog.encode("SET", "k", "v"); // 24 bytes
            for (int i = 0; i < 10; i++) {
                hub.publish(record);
            }
            assertEquals(10L * record.length, hub.offset());
            assertTrue(hub.backlogBytes() <= 100 + record.length, "backlog must stay near its limit");
            assertTrue(hub.backlogBytes() >= record.length, "and keep at least the newest record");
            hub.startNewHistory(500, true);
            assertNotEquals(id, hub.replid());
            assertEquals(500, hub.offset());
            assertEquals(0, hub.backlogBytes());
        }
    }
}
