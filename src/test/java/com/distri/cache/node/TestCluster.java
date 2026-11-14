package com.distri.cache.node;

import com.distri.cache.raft.RaftConfig;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/** Several {@link ReplicatedServer}s on localhost, each with its own directory; servers can be killed and restarted. */
final class TestCluster implements AutoCloseable {

    final Path root;
    final Map<Integer, ReplicatedServer.Member> members = new HashMap<>();
    final ReplicatedServer[] servers;
    final byte[] peerSecret;
    final byte[] clientSecret;
    final boolean fsync;
    final boolean checkQuorum;
    int maxBatch = ReplicatedServer.DEFAULT_MAX_BATCH; // set before start()
    com.distri.cache.raft.ReplicatedCache.ReadMode readMode = com.distri.cache.raft.ReplicatedCache.ReadMode.LOG; // set before start()
    javax.net.ssl.SSLContext tls;                       // set before start(); null = plain TCP
    final Map<Integer, javax.net.ssl.SSLContext> tlsByNode = new HashMap<>(); // overrides tls for one server
    javax.net.ssl.SSLContext clientTls;                 // used by client(); null = plain TCP
    int snapshotChunkBytes = RaftConfig.DEFAULT_SNAPSHOT_CHUNK_BYTES; // set before start()
    int snapshotEvery;                                 // set before start(); 0 = snapshots off

    TestCluster(int n) throws IOException {
        this(n, null, null, false);
    }

    TestCluster(int n, byte[] peerSecret, byte[] clientSecret, boolean fsync) throws IOException {
        this(n, peerSecret, clientSecret, fsync, true);
    }

    TestCluster(int n, byte[] peerSecret, byte[] clientSecret, boolean fsync, boolean checkQuorum)
            throws IOException {
        this.checkQuorum = checkQuorum;
        this.root = Files.createTempDirectory("raft-cluster");
        this.servers = new ReplicatedServer[n];
        this.peerSecret = peerSecret;
        this.clientSecret = clientSecret;
        this.fsync = fsync;
        for (int i = 0; i < n; i++) {
            members.put(i, new ReplicatedServer.Member("127.0.0.1", freePort(), freePort()));
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    ReplicatedServer.Config config(int i) {
        return ReplicatedServer.Config.of(i, members, root.resolve("node" + i))
                .withSecrets(peerSecret, clientSecret)
                .withTiming(new RaftConfig(20, 40, 4, 64, checkQuorum, Set.of(), snapshotEvery, snapshotChunkBytes), 5)
                .withFsync(fsync);
    }

    void start(int i) throws IOException {
        // The ports were picked from the operating system's ephemeral range. While a server is down, another
        // connection of this test (a peer dialling it, a client) can be handed its port as a source port, and a restart
        // then fails to bind for a moment. Retrying with a fresh server object is enough; real deployments use fixed ports.
        java.net.BindException last = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            ReplicatedServer s = new ReplicatedServer(config(i)).setMaxBatch(maxBatch).setReadMode(readMode);
            javax.net.ssl.SSLContext serverTls = tlsByNode.getOrDefault(i, tls);
            if (serverTls != null) {
                s.setTls(serverTls);
            }
            try {
                s.start();
                servers[i] = s;
                return;
            } catch (java.net.BindException e) {
                last = e;
                s.close();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw last;
    }

    void startAll() throws IOException {
        for (int i = 0; i < servers.length; i++) {
            start(i);
        }
    }

    /** Abrupt stop, like a crash: sockets closed, nothing flushed or announced. */
    void kill(int i) {
        if (servers[i] != null) {
            servers[i].close();
            servers[i] = null;
        }
    }

    boolean isUp(int i) {
        return servers[i] != null;
    }

    /** The live server that thinks it is leader and has the highest term, or -1. */
    int leader() {
        int best = -1;
        long bestTerm = -1;
        for (int i = 0; i < servers.length; i++) {
            if (servers[i] != null && servers[i].isLeader() && servers[i].term() > bestTerm) {
                best = i;
                bestTerm = servers[i].term();
            }
        }
        return best;
    }

    int awaitLeader(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
        while (System.nanoTime() < deadline) {
            int leader = leader();
            if (leader >= 0) {
                return leader;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no leader within " + timeoutMillis + " ms");
    }

    static void await(BooleanSupplier condition, long timeoutMillis, String what) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    List<InetSocketAddress> clientAddresses() {
        List<InetSocketAddress> out = new ArrayList<>();
        for (int i = 0; i < servers.length; i++) {
            out.add(new InetSocketAddress("127.0.0.1", members.get(i).clientPort()));
        }
        return out;
    }

    ReplicatedClient client(long operationTimeoutMillis) {
        return new ReplicatedClient(clientAddresses(), clientSecret, operationTimeoutMillis, clientTls);
    }

    /** A bare connection to one server's client port that has completed the handshake, for raw protocol tests. */
    static final class Raw implements AutoCloseable {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;

        Raw(int clientPort, byte[] secret) throws IOException {
            socket = new Socket("127.0.0.1", clientPort);
            socket.setSoTimeout(5_000);
            in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new java.io.BufferedOutputStream(socket.getOutputStream()));
            byte[] role = "raft-client".getBytes(StandardCharsets.UTF_8);
            out.writeInt(role.length + 4);
            out.write(role);
            out.writeInt(0);
            out.flush();
            Handshake.connect(in, out, secret, "raft-client", 0);
        }

        ClientProtocol.Response call(ClientProtocol.Request request) throws IOException {
            Frames.write(out, ClientProtocol.encode(request));
            out.flush();
            return ClientProtocol.decodeResponse(Frames.read(in));
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < servers.length; i++) {
            kill(i);
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
