package com.distri.cache.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.server.Tls;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

/** TLS on the peer port and the client port of the replicated server. Fixtures: src/test/resources/tls. */
class ReplicatedTlsTest {

    private static final char[] PASSWORD = "changeit".toCharArray();

    private static Path fixture(String name) throws Exception {
        return Paths.get(ReplicatedTlsTest.class.getResource("/tls/" + name).toURI());
    }

    private static SSLContext serverContext(String keys, String trust) throws Exception {
        return Tls.mutualContext(fixture(keys), PASSWORD, fixture(trust), PASSWORD);
    }

    private static SSLContext clientContext(String trust) throws Exception {
        return Tls.clientContext(fixture(trust), PASSWORD);
    }

    private static TestCluster tlsCluster() throws Exception {
        TestCluster c = new TestCluster(3, null, null, true);
        c.tls = serverContext("server.p12", "trust.p12");
        c.clientTls = clientContext("trust.p12");
        return c;
    }

    @Test
    void aClusterOverTlsServesReadsAndWritesAndSurvivesALeaderFailure() throws Exception {
        try (TestCluster cluster = tlsCluster()) {
            cluster.startAll();
            int leader = cluster.awaitLeader(10_000);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 30; i++) {
                    client.set("k" + i, "v" + i);
                }
                for (int i = 0; i < 30; i++) {
                    assertEquals(Optional.of("v" + i), client.get("k" + i));
                }
                cluster.kill(leader);
                client.set("after-failover", "yes"); // a new leader must be elected over TLS peer links
                assertEquals(Optional.of("yes"), client.get("after-failover"));
                assertEquals(Optional.of("v7"), client.get("k7"), "nothing acknowledged before the failure is lost");
            }
        }
    }

    @Test
    void aClientWithoutTlsCannotUseATlsServer() throws Exception {
        try (TestCluster cluster = tlsCluster()) {
            cluster.startAll();
            cluster.awaitLeader(10_000);
            try (ReplicatedClient plain = new ReplicatedClient(cluster.clientAddresses(), cluster.clientSecret, 2_000)) {
                assertThrows(ReplicatedClientException.class, () -> plain.set("k", "v"));
            }
            // and the servers are unharmed by the attempt
            try (ReplicatedClient client = cluster.client(20_000)) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void aClientThatDoesNotTrustTheServersCertificateIsRefused() throws Exception {
        try (TestCluster cluster = tlsCluster()) {
            cluster.startAll();
            cluster.awaitLeader(10_000);
            try (ReplicatedClient stranger = new ReplicatedClient(cluster.clientAddresses(), cluster.clientSecret,
                    2_000, clientContext("other-trust.p12"))) {
                assertThrows(ReplicatedClientException.class, () -> stranger.set("k", "v"));
            }
        }
    }

    @Test
    void aServerWhoseCertificateTheOthersDoNotTrustCannotJoinAndTheRestKeepWorking() throws Exception {
        try (TestCluster cluster = tlsCluster()) {
            cluster.tlsByNode.put(2, serverContext("other-server.p12", "other-trust.p12")); // an outsider
            cluster.startAll();
            int leader = cluster.awaitLeader(10_000);
            assertTrue(leader == 0 || leader == 1, "the outsider must not be able to win an election, leader " + leader);
            try (ReplicatedClient client = cluster.client(20_000)) {
                for (int i = 0; i < 20; i++) {
                    client.set("k" + i, "v" + i);
                }
                assertEquals(Optional.of("v19"), client.get("k19"));
            }
            Thread.sleep(500);
            assertEquals(0, cluster.servers[2].runtimeForMetrics().lastApplied(),
                    "no entry may have reached the outsider");
            assertTrue(cluster.servers[leader].runtimeForMetrics().lastApplied() >= 20);
        }
    }

    @Test
    void thePeerPortRefusesAConnectionWithoutACertificateAndPlainBytes() throws Exception {
        try (TestCluster cluster = tlsCluster()) {
            cluster.startAll();
            int leader = cluster.awaitLeader(10_000);
            int peerPort = cluster.members.get(0).peerPort();
            // TLS, but this client presents no certificate: the peer port must refuse
            SSLContext noCertificate = clientContext("trust.p12");
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", peerPort), 2_000);
                raw.setSoTimeout(3_000);
                // The server must END the connection with a TLS failure (certificate required / bad certificate).
                // Merely going quiet or dropping malformed bytes later is not a refusal: it would also happen if the
                // server had accepted the connection and then waited for a hello that never comes.
                boolean refused = false;
                try {
                    Socket tls = Tls.connectTls(noCertificate, raw, "127.0.0.1", peerPort);
                    tls.getInputStream().read(); // TLS 1.3 reports the server's rejection on the first read
                } catch (javax.net.ssl.SSLException e) {
                    refused = true;
                } catch (java.net.SocketTimeoutException e) {
                    refused = false; // the server accepted the connection and is waiting for the peer hello
                }
                assertTrue(refused, "a connection without a certificate must not be accepted on the peer port");
            }
            // plain bytes on the TLS peer port: dropped
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", peerPort), 2_000);
                raw.setSoTimeout(3_000);
                raw.getOutputStream().write("not tls at all, just text\n".getBytes());
                raw.getOutputStream().flush();
                boolean closedOrAlert;
                try {
                    closedOrAlert = raw.getInputStream().read() != 22 /* a TLS handshake record, i.e. NOT a plaintext hello */;
                } catch (IOException e) {
                    closedOrAlert = true;
                }
                assertTrue(closedOrAlert);
            }
            try (ReplicatedClient client = cluster.client(20_000)) {
                client.set("still", "fine");
                assertEquals(Optional.of("fine"), client.get("still"));
            }
            assertTrue(cluster.servers[leader] != null);
        }
    }

    @Test
    void theHostNameInTheCertificateMustMatchTheAddressUsed() throws Exception {
        // a certificate issued for wrong.example, trusted by the client: only the host-name check can reject it
        try (ServerSocket listener = new ServerSocket(0, 5, java.net.InetAddress.getByName("127.0.0.1"))) {
            SSLContext wrongName = serverContext("wrongname-server.p12", "wrongname-trust.p12");
            Thread accept = new Thread(() -> {
                for (int i = 0; i < 2; i++) {
                    try (Socket s = listener.accept()) {
                        s.setSoTimeout(2_000);
                        Tls.acceptTls(wrongName, s, false).getInputStream().read();
                    } catch (IOException ignored) {
                        // the client walks away after the failed check
                    }
                }
            });
            accept.setDaemon(true);
            accept.start();
            SSLContext client = clientContext("wrongname-trust.p12");
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", listener.getLocalPort()), 2_000);
                raw.setSoTimeout(2_000);
                assertThrows(javax.net.ssl.SSLHandshakeException.class,
                        () -> Tls.connectTls(client, raw, "127.0.0.1", listener.getLocalPort()));
            }
        }
    }

    @Test
    void aMatchingCertificateIsAcceptedWhenTheNameFits() throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 5, java.net.InetAddress.getByName("127.0.0.1"))) {
            SSLContext server = serverContext("server.p12", "trust.p12");
            Thread accept = new Thread(() -> {
                try (Socket s = listener.accept()) {
                    s.setSoTimeout(2_000);
                    Socket tls = Tls.acceptTls(server, s, false);
                    tls.getOutputStream().write(42);
                    tls.getOutputStream().flush();
                    tls.getInputStream().read();
                } catch (IOException ignored) {
                    // finished
                }
            });
            accept.setDaemon(true);
            accept.start();
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", listener.getLocalPort()), 2_000);
                raw.setSoTimeout(2_000);
                Socket tls = Tls.connectTls(clientContext("trust.p12"), raw, "127.0.0.1", listener.getLocalPort());
                assertEquals(42, tls.getInputStream().read());
                assertFalse(tls.isClosed());
                String protocol = ((javax.net.ssl.SSLSocket) tls).getSession().getProtocol();
                assertTrue(protocol.equals("TLSv1.3") || protocol.equals("TLSv1.2"), "negotiated " + protocol);
            }
        }
    }

    /** Forwards one client connection to {@code target} and records every byte that crosses in either direction. */
    private static final class RecordingProxy implements AutoCloseable {
        final ServerSocket listener;
        final java.io.ByteArrayOutputStream recorded = new java.io.ByteArrayOutputStream();

        RecordingProxy(int targetPort) throws IOException {
            listener = new ServerSocket(0, 5, java.net.InetAddress.getByName("127.0.0.1"));
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        Socket client = listener.accept();
                        Socket upstream = new Socket("127.0.0.1", targetPort);
                        pump(client, upstream);
                        pump(upstream, client);
                    }
                } catch (IOException closed) {
                    // proxy closed
                }
            });
            t.setDaemon(true);
            t.start();
        }

        private void pump(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                byte[] buffer = new byte[4096];
                try {
                    int n;
                    while ((n = from.getInputStream().read(buffer)) > 0) {
                        synchronized (recorded) {
                            recorded.write(buffer, 0, n);
                        }
                        to.getOutputStream().write(buffer, 0, n);
                        to.getOutputStream().flush();
                    }
                } catch (IOException ended) {
                    // connection ended
                } finally {
                    try {
                        from.close();
                        to.close();
                    } catch (IOException ignored) {
                        // done
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        boolean sawOnTheWire(String text) {
            synchronized (recorded) {
                return new String(recorded.toByteArray(), java.nio.charset.StandardCharsets.ISO_8859_1).contains(text);
            }
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }
    }

    private static final String MARKER = "wire-marker-7f3a91c2d4e8";

    @Test
    void withoutTlsAValueCrossesTheWireReadableAsAControlForTheProxy() throws Exception {
        try (TestCluster cluster = new TestCluster(1, null, null, false)) {
            cluster.startAll();
            cluster.awaitLeader(5_000);
            try (RecordingProxy proxy = new RecordingProxy(cluster.members.get(0).clientPort());
                    ReplicatedClient client = new ReplicatedClient(
                            List.of(new InetSocketAddress("127.0.0.1", proxy.port())), cluster.clientSecret, 10_000)) {
                client.set("k", MARKER);
                assertEquals(Optional.of(MARKER), client.get("k"));
                assertTrue(proxy.sawOnTheWire(MARKER), "control: without TLS the value must be visible to the proxy");
            }
        }
    }

    @Test
    void withTlsTheValueAndTheKeyAreNotVisibleOnTheWire() throws Exception {
        try (TestCluster cluster = new TestCluster(1, null, null, false)) {
            cluster.tls = serverContext("server.p12", "trust.p12");
            cluster.startAll();
            cluster.awaitLeader(5_000);
            try (RecordingProxy proxy = new RecordingProxy(cluster.members.get(0).clientPort());
                    ReplicatedClient client = new ReplicatedClient(
                            List.of(new InetSocketAddress("127.0.0.1", proxy.port())), cluster.clientSecret, 10_000,
                            clientContext("trust.p12"))) {
                client.set(MARKER + "-key", MARKER);
                assertEquals(Optional.of(MARKER), client.get(MARKER + "-key"));
                assertFalse(proxy.sawOnTheWire(MARKER), "the value must not be readable on the wire");
                assertTrue(proxy.recorded.size() > 200, "something did cross: " + proxy.recorded.size() + " bytes");
            }
        }
    }

    @Test
    void aServerThatTrustsTheClusterButPresentsAnUntrustedCertificateCannotDisruptIt() throws Exception {
        // This outsider holds the (public) cluster certificate in its trust store, so it can verify the others, but its
        // own certificate is not trusted. If the peer port did not demand a certificate from the dialler, its
        // RequestVote messages with ever higher terms would get through and keep knocking the leader down.
        try (TestCluster cluster = tlsCluster()) {
            cluster.tlsByNode.put(2, serverContext("other-server.p12", "trust.p12"));
            cluster.startAll();
            int leader = cluster.awaitLeader(10_000);
            Thread.sleep(500);
            long termBefore = cluster.servers[leader].runtimeForMetrics().term();
            Thread.sleep(3_000); // the outsider's election timer fires about every half second
            assertEquals(leader, cluster.leader(), "the leader must not be replaced");
            assertEquals(termBefore, cluster.servers[leader].runtimeForMetrics().term(),
                    "the term must not move: no message from the outsider may get through");
        }
    }

    @Test
    void onlyTls13And12AreEnabledAndAnOlderClientIsRefused() throws Exception {
        assertEquals(List.of("TLSv1.3", "TLSv1.2"), List.of(Tls.PROTOCOLS));
        try (ServerSocket listener = new ServerSocket(0, 5, java.net.InetAddress.getByName("127.0.0.1"))) {
            SSLContext server = serverContext("server.p12", "trust.p12");
            Thread accept = new Thread(() -> {
                try (Socket s = listener.accept()) {
                    s.setSoTimeout(2_000);
                    Tls.acceptTls(server, s, false);
                } catch (IOException expected) {
                    // the old-protocol client must fail the handshake
                }
            });
            accept.setDaemon(true);
            accept.start();
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", listener.getLocalPort()), 2_000);
                raw.setSoTimeout(2_000);
                javax.net.ssl.SSLSocket old = (javax.net.ssl.SSLSocket) clientContext("trust.p12").getSocketFactory()
                        .createSocket(raw, "127.0.0.1", listener.getLocalPort(), true);
                try {
                    old.setEnabledProtocols(new String[] {"TLSv1.1"});
                    assertThrows(IOException.class, old::startHandshake);
                } catch (IllegalArgumentException unsupportedOnThisJdk) {
                    // this JDK cannot even offer TLS 1.1: the server could not accept it either
                }
            }
        }
    }
}
