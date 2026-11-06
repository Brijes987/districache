package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Password authentication (AUTH). TLS is covered in the client package's SecureClientTest. */
class AuthTest {

    private static final String PASSWORD = "correct horse battery staple";

    private static CacheServer start(String password) throws IOException {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
        return new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16,
                new CacheServer.Security(password, null)).start();
    }

    private static RespTestClient.Reply ok() {
        return new RespTestClient.Reply('+', "OK");
    }

    @Test
    void withoutAPasswordConfiguredEverythingWorksAndAuthIsAnError() throws Exception {
        try (CacheServer server = start(null); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals(ok(), c.call("SET", "k", "v"));
            RespTestClient.Reply r = c.call("AUTH", "anything");
            assertEquals('-', r.type());
            assertTrue(r.text().contains("without any password"), r.text());
            assertEquals("v", c.call("GET", "k").text()); // still usable
        }
    }

    @Test
    void everyCommandIsRefusedBeforeAuth() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            for (String[] cmd : new String[][] {{"PING"}, {"GET", "k"}, {"SET", "k", "v"}, {"DEL", "k"},
                    {"DBSIZE"}, {"ECHO", "x"}, {"REWRITEAOF"}, {"NOSUCHCOMMAND"}}) {
                RespTestClient.Reply r = c.call(cmd);
                assertEquals('-', r.type(), cmd[0]);
                assertTrue(r.text().startsWith("NOAUTH"), cmd[0] + " -> " + r.text());
            }
        }
    }

    @Test
    void nothingIsStoredByARefusedWrite() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "k", "v");
            assertEquals(ok(), c.call("AUTH", PASSWORD));
            assertEquals(new RespTestClient.Reply('$', null), c.call("GET", "k"));
        }
    }

    @Test
    void theRightPasswordUnlocksTheConnection() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals(ok(), c.call("AUTH", PASSWORD));
            assertEquals(ok(), c.call("SET", "k", "v"));
            assertEquals("v", c.call("GET", "k").text());
            assertEquals("PONG", c.call("PING").text());
        }
    }

    @Test
    void authIsPerConnection() throws Exception {
        try (CacheServer server = start(PASSWORD);
                RespTestClient a = new RespTestClient(server.port());
                RespTestClient b = new RespTestClient(server.port())) {
            assertEquals(ok(), a.call("AUTH", PASSWORD));
            assertEquals(ok(), a.call("SET", "k", "v"));
            assertTrue(b.call("GET", "k").text().startsWith("NOAUTH"));
        }
    }

    @Test
    void aWrongPasswordIsRejectedAndTheConnectionStaysLocked() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            RespTestClient.Reply r = c.call("AUTH", "wrong");
            assertEquals('-', r.type());
            assertTrue(r.text().startsWith("WRONGPASS"), r.text());
            assertTrue(c.call("GET", "k").text().startsWith("NOAUTH"));
            // near misses: a prefix, an extension and a different case must all fail
            assertEquals('-', c.call("AUTH", PASSWORD.substring(0, PASSWORD.length() - 1)).type());
            assertEquals('-', c.call("AUTH", PASSWORD + "x").type());
            assertEquals('-', c.call("AUTH", PASSWORD.toUpperCase()).type());
            assertEquals(ok(), c.call("AUTH", PASSWORD)); // and the right one still works afterwards
        }
    }

    @Test
    void theTwoArgumentFormIsAcceptedOnlyForTheDefaultUser() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("AUTH", "admin", PASSWORD).type());
            assertEquals(ok(), c.call("AUTH", "default", PASSWORD));
            assertEquals("PONG", c.call("PING").text());
        }
    }

    @Test
    void authWithTheWrongNumberOfArgumentsIsAnError() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("AUTH").type());
            assertEquals('-', c.call("AUTH", "a", "b", "c").type());
        }
    }

    @Test
    void tooManyWrongGuessesCloseTheConnection() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            for (int i = 0; i < CommandExecutor.MAX_FAILED_AUTH_ATTEMPTS; i++) {
                assertTrue(c.call("AUTH", "guess" + i).text().startsWith("WRONGPASS"));
            }
            assertTrue(c.isClosedByServer());
        }
    }

    @Test
    void aSuccessfulAuthResetsTheFailureCount() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            for (int i = 0; i < CommandExecutor.MAX_FAILED_AUTH_ATTEMPTS - 1; i++) {
                c.call("AUTH", "guess");
            }
            assertEquals(ok(), c.call("AUTH", PASSWORD));
            for (int i = 0; i < CommandExecutor.MAX_FAILED_AUTH_ATTEMPTS - 1; i++) {
                assertTrue(c.call("AUTH", "guess").text().startsWith("WRONGPASS")); // not closed yet
            }
        }
    }

    @Test
    void aFailedGuessIsSlowedDown() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            long start = System.nanoTime();
            c.call("AUTH", "wrong");
            long millis = (System.nanoTime() - start) / 1_000_000;
            assertTrue(millis >= CommandExecutor.FAILED_AUTH_DELAY_MILLIS - 20, "took only " + millis + " ms");
        }
    }

    @Test
    void quitIsAllowedBeforeAuth() throws Exception {
        try (CacheServer server = start(PASSWORD); RespTestClient c = new RespTestClient(server.port())) {
            assertEquals(ok(), c.call("QUIT"));
            assertTrue(c.isClosedByServer());
        }
    }

    @Test
    void aClientThatNeverAuthenticatesIsDisconnected() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
        CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16,
                new CacheServer.Security(PASSWORD, null));
        server.setPreAuthTimeoutMillis(300);
        server.start();
        try (server; RespTestClient idle = new RespTestClient(server.port());
                RespTestClient authed = new RespTestClient(server.port())) {
            assertEquals(ok(), authed.call("AUTH", PASSWORD));
            Thread.sleep(800);
            assertTrue(idle.isClosedByServer(), "an unauthenticated connection must not live past the timeout");
            // an authenticated connection is not subject to the pre-auth limit
            assertEquals("PONG", authed.call("PING").text());
        }
    }

    @Test
    void thePasswordIsNeverWrittenToThePersistenceLog() throws Exception {
        Path dir = Files.createTempDirectory("districache-auth-test");
        Path file = dir.resolve("a.aof");
        file.toFile().deleteOnExit();
        dir.toFile().deleteOnExit();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 1, 0);
        AppendOnlyLog log = AppendOnlyLog.open(file, AppendOnlyLog.FsyncPolicy.NO, 0, cache);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 16, log,
                new CacheServer.Security(PASSWORD, null)).start();
                RespTestClient c = new RespTestClient(server.port())) {
            c.call("AUTH", PASSWORD);
            c.call("SET", "k", "v");
        }
        log.close();
        String contents = Files.readString(file, StandardCharsets.UTF_8);
        assertFalse(contents.contains("AUTH"));
        assertFalse(contents.contains(PASSWORD));
        assertTrue(contents.contains("SET"));
    }

    @Test
    void secretsDoNotAppearInToString() {
        assertFalse(new CacheServer.Security(PASSWORD, null).toString().contains(PASSWORD));
    }

    @Test
    void emptyPasswordsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheServer.Security("", null));
        assertThrows(IllegalArgumentException.class, () -> new Authenticator(""));
        assertThrows(IllegalArgumentException.class, () -> new Authenticator(null));
    }

    @Test
    void theAuthenticatorComparesWholeValues() {
        Authenticator a = new Authenticator("secret");
        assertTrue(a.matches("secret".getBytes(StandardCharsets.UTF_8)));
        assertFalse(a.matches("secre".getBytes(StandardCharsets.UTF_8)));
        assertFalse(a.matches("secret ".getBytes(StandardCharsets.UTF_8)));
        assertFalse(a.matches(new byte[0]));
    }
}
