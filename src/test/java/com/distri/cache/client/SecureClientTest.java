package com.distri.cache.client;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.server.CacheServer;
import com.distri.cache.server.Tls;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/** Client and server together with a password and/or TLS, using throw-away test certificates. */
class SecureClientTest {

    private static final char[] STORE_PASSWORD = "changeit".toCharArray();
    private static final String PASSWORD = "s3cret-pass";

    private static Path resource(String name) throws Exception {
        return Paths.get(SecureClientTest.class.getResource("/tls/" + name).toURI());
    }

    private static CacheServer startServer(String password, String keystore) throws Exception {
        CacheServer.Security security = new CacheServer.Security(password,
                keystore == null ? null : Tls.serverContext(resource(keystore), STORE_PASSWORD));
        return new CacheServer(new ShardedLRUCache<>(100, 4, 0), CacheServer.DEFAULT_HOST, 0, 16, security).start();
    }

    private static CacheClient client(int port, ClientSecurity security) {
        return new CacheClient("localhost", port, 3000, 5000, security);
    }

    private static ClientSecurity trusting(String trustStore, String password) throws Exception {
        return ClientSecurity.tls(resource(trustStore), STORE_PASSWORD, password);
    }

    // ------------------------------------------------------------------ password only

    @Test
    void aClientWithThePasswordWorks() throws Exception {
        try (CacheServer server = startServer(PASSWORD, null);
                CacheClient c = client(server.port(), ClientSecurity.password(PASSWORD))) {
            c.set("k", "v");
            assertEquals("v", c.get("k").orElseThrow());
        }
    }

    @Test
    void aClientWithTheWrongOrNoPasswordIsRefused() throws Exception {
        try (CacheServer server = startServer(PASSWORD, null)) {
            try (CacheClient c = client(server.port(), ClientSecurity.password("wrong"))) {
                CacheClientException e = assertThrows(CacheClientException.class, () -> c.get("k"));
                assertTrue(e.getMessage().contains("authentication"), e.getMessage());
                assertFalse(e.getMessage().contains("wrong"), "the guess must not be echoed: " + e.getMessage());
            }
            try (CacheClient c = client(server.port(), ClientSecurity.NONE)) {
                CacheClientException e = assertThrows(CacheClientException.class, () -> c.get("k"));
                assertTrue(e.getMessage().contains("NOAUTH"), e.getMessage());
            }
        }
    }

    @Test
    void thePasswordIsSentAgainWhenTheClientReconnects() throws Exception {
        CacheServer first = startServer(PASSWORD, null);
        int port = first.port();
        try (CacheClient c = client(port, ClientSecurity.password(PASSWORD))) {
            c.set("k", "v");
            first.close();
            assertThrows(CacheClientException.class, () -> c.get("k")); // the old connection is gone
            CacheServer second = new CacheServer(new ShardedLRUCache<>(100, 4, 0), CacheServer.DEFAULT_HOST, port, 16,
                    new CacheServer.Security(PASSWORD, null)).start();
            try (second) {
                c.set("k2", "v2"); // reconnects and authenticates by itself
                assertEquals("v2", c.get("k2").orElseThrow());
            }
        }
    }

    @Test
    void aClientPasswordWithTheServerHavingNoneIsAnErrorNotASilentSuccess() throws Exception {
        try (CacheServer server = startServer(null, null);
                CacheClient c = client(server.port(), ClientSecurity.password("whatever"))) {
            assertThrows(CacheClientException.class, () -> c.get("k"));
        }
    }

    // ------------------------------------------------------------------ TLS

    @Test
    void tlsRoundTrip() throws Exception {
        try (CacheServer server = startServer(null, "server.p12");
                CacheClient c = client(server.port(), trusting("trust.p12", null))) {
            assertEquals("PONG", c.ping());
            c.set("k", "नमस्ते");
            assertEquals("नमस्ते", c.get("k").orElseThrow());
            assertTrue(c.delete("k"));
        }
    }

    @Test
    void tlsWithAPassword() throws Exception {
        try (CacheServer server = startServer(PASSWORD, "server.p12")) {
            try (CacheClient c = client(server.port(), trusting("trust.p12", PASSWORD))) {
                c.set("k", "v");
                assertEquals("v", c.get("k").orElseThrow());
            }
            try (CacheClient c = client(server.port(), trusting("trust.p12", "wrong"))) {
                assertThrows(CacheClientException.class, () -> c.get("k"));
            }
        }
    }

    @Test
    void aServerWhoseCertificateIsNotTrustedIsRefused() throws Exception {
        try (CacheServer server = startServer(null, "server.p12");
                CacheClient c = client(server.port(), trusting("other-trust.p12", null))) {
            assertThrows(CacheClientException.class, () -> c.ping());
        }
    }

    @Test
    void aCertificateForAnotherHostNameIsRefusedEvenThoughItIsTrusted() throws Exception {
        // wrongname-trust.p12 trusts this exact certificate, so only the host-name check can reject it
        try (CacheServer server = startServer(null, "wrongname-server.p12");
                CacheClient c = client(server.port(), trusting("wrongname-trust.p12", null))) {
            CacheClientException e = assertThrows(CacheClientException.class, () -> c.ping());
            assertTrue(e.getMessage().toLowerCase().contains("failed"), e.getMessage());
        }
    }

    @Test
    void aPlainClientCannotUseATlsServerAndTheServerSurvives() throws Exception {
        try (CacheServer server = startServer(null, "server.p12")) {
            try (CacheClient plain = client(server.port(), ClientSecurity.NONE)) {
                assertThrows(CacheClientException.class, () -> plain.ping());
            }
            try (CacheClient good = client(server.port(), trusting("trust.p12", null))) {
                assertEquals("PONG", good.ping()); // the failed handshake did not hurt the server
            }
        }
    }

    @Test
    void aTlsClientCannotUseAPlainServer() throws Exception {
        try (CacheServer server = startServer(null, null);
                CacheClient c = client(server.port(), trusting("trust.p12", null))) {
            assertThrows(CacheClientException.class, () -> c.ping());
        }
    }

    @Test
    void dataOnATlsConnectionIsNotReadableOnTheWire() throws Exception {
        try (CacheServer server = startServer(null, "server.p12");
                Socket raw = new Socket("127.0.0.1", server.port())) {
            raw.setSoTimeout(2000);
            raw.getOutputStream().write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            raw.getOutputStream().flush();
            InputStream in = raw.getInputStream();
            byte[] buf = new byte[64];
            int n;
            try {
                n = in.read(buf);
            } catch (IOException e) {
                n = -1; // reset or timeout: also fine, the server did not answer in clear text
            }
            String reply = n > 0 ? new String(buf, 0, n, StandardCharsets.ISO_8859_1) : "";
            assertFalse(reply.contains("PONG"), "a plain-text command must not be answered on a TLS port");
        }
    }

    @Test
    void aClusterClientUsesTheSameSettingsForEveryServer() throws Exception {
        try (CacheServer a = startServer(PASSWORD, "server.p12"); CacheServer b = startServer(PASSWORD, "server.p12");
                ClusterClient cluster = ClusterClient.of(trusting("trust.p12", PASSWORD),
                        "localhost:" + a.port(), "localhost:" + b.port())) {
            for (int i = 0; i < 50; i++) {
                cluster.set("key" + i, "v" + i);
            }
            for (int i = 0; i < 50; i++) {
                assertEquals("v" + i, cluster.get("key" + i).orElseThrow());
            }
            assertEquals(50, cluster.dbSize());
        }
    }

    @Test
    void secretsDoNotAppearInToString() {
        assertFalse(ClientSecurity.password(PASSWORD).toString().contains(PASSWORD));
        assertThrows(IllegalArgumentException.class, () -> new ClientSecurity("", null));
    }
}
