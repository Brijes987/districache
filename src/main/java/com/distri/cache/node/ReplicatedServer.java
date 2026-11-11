package com.distri.cache.node;

import com.distri.cache.raft.FileRaftStorage;
import com.distri.cache.raft.RaftConfig;
import com.distri.cache.raft.RaftNode;
import com.distri.cache.raft.ReplicatedCache;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One member of a replicated cache group: a Raft node with its log on disk, talking to its peers over TCP, and
 * serving clients on a second port.
 *
 * <p>All state changes go through the Raft log, so every acknowledged write is on a majority of servers' disks. Reads
 * also go through the log (linearizable, but each read costs a log round trip). A server that is not the leader answers
 * {@code NOT_LEADER} with the leader's address when it knows it.
 *
 * <p>Limits: the group is fixed at start (no membership changes); the log is never compacted, so disk use and restart
 * time grow with the number of writes; the client port has no encryption and authenticates only if a client secret is
 * configured; peer links are authenticated by a shared secret if configured but are not encrypted.
 */
public final class ReplicatedServer implements AutoCloseable {

    /** Where one group member listens. */
    public record Member(String host, int peerPort, int clientPort) {}

    /**
     * @param id this server's id, a key of {@code members}
     * @param members every server in the group, including this one
     * @param dataDir where this server keeps its Raft state (created if missing)
     * @param peerSecret shared secret peers must prove they know, or null
     * @param clientSecret shared secret clients must prove they know, or null
     * @param raft timing, in ticks
     * @param tickMillis real time per tick
     * @param fsync force every log write to disk before acknowledging (leave on outside tests)
     */
    public record Config(int id, Map<Integer, Member> members, Path dataDir, byte[] peerSecret, byte[] clientSecret,
            RaftConfig raft, long tickMillis, boolean fsync, int cacheCapacity, long requestTimeoutMillis) {

        public Config {
            if (!members.containsKey(id)) {
                throw new IllegalArgumentException("members must include this server's id " + id);
            }
            members = Map.copyOf(members);
        }

        /** Election timeout 300 to 600 ms, heartbeat 60 ms, 10 000-entry cache, fsync on. */
        public static Config of(int id, Map<Integer, Member> members, Path dataDir) {
            return new Config(id, members, dataDir, null, null, new RaftConfig(30, 60, 6, 64, true, Set.of()), 10,
                    true, 10_000, 5_000);
        }

        public Config withSecrets(byte[] peerSecret, byte[] clientSecret) {
            return new Config(id, members, dataDir, peerSecret, clientSecret, raft, tickMillis, fsync, cacheCapacity,
                    requestTimeoutMillis);
        }

        public Config withTiming(RaftConfig raft, long tickMillis) {
            return new Config(id, members, dataDir, peerSecret, clientSecret, raft, tickMillis, fsync, cacheCapacity,
                    requestTimeoutMillis);
        }

        public Config withFsync(boolean fsync) {
            return new Config(id, members, dataDir, peerSecret, clientSecret, raft, tickMillis, fsync, cacheCapacity,
                    requestTimeoutMillis);
        }
    }

    private static final String CLIENT_ROLE = "raft-client";
    private static final int MAX_CLIENTS = 256;
    /** Up to this many queued client requests share one log append and fsync. */
    public static final int DEFAULT_MAX_BATCH = 256;

    private final Config config;
    private final Semaphore clientSlots = new Semaphore(MAX_CLIENTS);
    private final Set<Socket> clientSockets = ConcurrentHashMap.newKeySet();
    private FileRaftStorage storage;
    private ReplicatedCache cache;
    private PeerTransport transport;
    private NodeRuntime runtime;
    private ServerSocket clientListener;
    private volatile boolean closed;
    private volatile NodeMetrics metrics; // null: metrics off
    private int maxBatch = DEFAULT_MAX_BATCH;

    public ReplicatedServer(Config config) {
        this.config = config;
    }

    /**
     * Publishes this server's Raft state and client-request numbers into {@code registry}. Call before
     * {@link #start()}. The Raft gauges read a snapshot that the node's own thread publishes after every step, so a
     * scrape never touches the (not thread-safe) Raft node; they can be a few milliseconds behind.
     */
    public synchronized ReplicatedServer enableMetrics(com.distri.cache.metrics.MetricsRegistry registry) {
        if (runtime != null) {
            throw new IllegalStateException("call enableMetrics before start()");
        }
        this.metrics = new NodeMetrics(registry, this);
        return this;
    }

    /** Group commit size: 1 = every request is written and fsynced alone. Call before {@link #start()}. */
    public synchronized ReplicatedServer setMaxBatch(int maxBatch) {
        if (runtime != null) {
            throw new IllegalStateException("call setMaxBatch before start()");
        }
        if (maxBatch <= 0) {
            throw new IllegalArgumentException("maxBatch must be positive");
        }
        this.maxBatch = maxBatch;
        return this;
    }

    public void start() throws IOException {
        Member self = config.members().get(config.id());
        storage = new FileRaftStorage(config.dataDir(), config.fsync());
        try {
            Map<Integer, InetSocketAddress> peerAddresses = new HashMap<>();
            List<Integer> peerIds = new ArrayList<>();
            for (Map.Entry<Integer, Member> e : config.members().entrySet()) {
                if (e.getKey() != config.id()) {
                    peerAddresses.put(e.getKey(), new InetSocketAddress(e.getValue().host(), e.getValue().peerPort()));
                    peerIds.add(e.getKey());
                }
            }
            cache = new ReplicatedCache(config.cacheCapacity(), 10_000, System::currentTimeMillis);
            transport = new PeerTransport(config.id(), peerAddresses, config.peerSecret(),
                    (from, message) -> runtime.deliver(from, message));
            RaftNode node = new RaftNode(config.id(), peerIds, storage, transport, cache, config.raft(),
                    new Random(System.nanoTime() ^ config.id()));
            cache.attach(node);
            runtime = new NodeRuntime(node, cache, config.tickMillis(), maxBatch, this::close);
            transport.start(self.host(), self.peerPort());
            clientListener = new ServerSocket();
            clientListener.setReuseAddress(true);
            clientListener.bind(new InetSocketAddress(InetAddress.getByName(self.host()), self.clientPort()));
            runtime.start();
            Thread accept = new Thread(this::acceptClients, "raft-clients-" + config.id());
            accept.setDaemon(true);
            accept.start();
        } catch (IOException | RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    NodeRuntime runtimeForMetrics() {
        return runtime;
    }

    public int id() {
        return config.id();
    }

    public int peerPort() {
        return transport.port();
    }

    public int clientPort() {
        return clientListener.getLocalPort();
    }

    public boolean isLeader() {
        return runtime != null && runtime.role() == RaftNode.Role.LEADER && !closed;
    }

    public long term() {
        return runtime.term();
    }

    public long commitIndex() {
        return runtime.commitIndex();
    }

    /**
     * Test hook: cut this server off from these peers (both directions) while its client port keeps working, which
     * is what a network partition looks like. Pass an empty set to heal. Not for production use.
     */
    public void blockPeers(Set<Integer> peers) {
        transport.block(peers);
    }

    /** True if the server stopped itself after an unexpected error. */
    public boolean failed() {
        return runtime != null && runtime.failed();
    }

    /** This server's own copy of the data, read on the Raft thread. For tests and diagnostics. */
    public Map<String, String> localContents() throws InterruptedException {
        return runtime.inspect(cache::contents);
    }

    // ------------------------------------------------------------------ clients

    private void acceptClients() {
        while (!closed) {
            Socket socket;
            try {
                socket = clientListener.accept();
            } catch (IOException e) {
                return;
            }
            if (!clientSlots.tryAcquire()) {
                closeQuietly(socket);
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    serveClient(socket);
                } finally {
                    clientSlots.release();
                    clientSockets.remove(socket);
                    closeQuietly(socket);
                }
            }, "raft-client-conn-" + config.id());
            t.setDaemon(true);
            t.start();
        }
    }

    private void serveClient(Socket socket) {
        try {
            clientSockets.add(socket);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(3_000);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            byte[] hello = Frames.read(in, 256);
            if (hello.length < 4 || !CLIENT_ROLE.equals(new String(hello, 0, hello.length - 4,
                    java.nio.charset.StandardCharsets.UTF_8))) {
                return;
            }
            Handshake.accept(in, out, config.clientSecret(), CLIENT_ROLE, 0);
            socket.setSoTimeout(0);
            while (!closed) {
                ClientProtocol.Request request;
                try {
                    request = ClientProtocol.decodeRequest(Frames.read(in, 16 * 1024 * 1024));
                } catch (java.io.EOFException e) {
                    return;
                }
                ClientProtocol.Response response = handle(request);
                Frames.write(out, ClientProtocol.encode(response));
                out.flush();
            }
        } catch (IOException e) {
            // client went away, failed the handshake or sent garbage: drop the connection
        }
    }

    private ClientProtocol.Response handle(ClientProtocol.Request r) {
        NodeMetrics m = metrics;
        long startedAt = m != null ? System.nanoTime() : 0;
        ClientProtocol.Response response = handleRequest(r);
        if (m != null) {
            m.record(r, response, System.nanoTime() - startedAt);
        }
        return response;
    }

    private ClientProtocol.Response handleRequest(ClientProtocol.Request r) {
        CompletableFuture<ReplicatedCache.Reply> future;
        try {
            future = runtime.call(() -> switch (r.op()) {
                case ClientProtocol.SET -> cache.set(r.clientId(), r.seq(), r.key(), r.value(), r.ttlMillis());
                case ClientProtocol.DEL -> cache.delete(r.clientId(), r.seq(), r.key());
                default -> cache.get(r.clientId(), r.seq(), r.key());
            });
            ReplicatedCache.Reply reply = future.get(config.requestTimeoutMillis(), TimeUnit.MILLISECONDS);
            return switch (reply.kind()) {
                case OK -> new ClientProtocol.Response(ClientProtocol.OK, reply.found(),
                        reply.value() == null ? "" : reply.value(), "", "");
                case NOT_LEADER -> new ClientProtocol.Response(ClientProtocol.NOT_LEADER, false, "",
                        addressOf(reply.leaderHint()), "");
                case UNKNOWN -> new ClientProtocol.Response(ClientProtocol.UNKNOWN, false, "", "", "");
                case STALE -> new ClientProtocol.Response(ClientProtocol.STALE, false, "", "", "");
            };
        } catch (TimeoutException e) {
            // no answer in time (for example this leader lost its majority): the outcome is unknown, so say so
            return new ClientProtocol.Response(ClientProtocol.UNKNOWN, false, "", "", "");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            return new ClientProtocol.Response(ClientProtocol.ERROR, false, "", "", String.valueOf(cause.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ClientProtocol.Response(ClientProtocol.UNKNOWN, false, "", "", "");
        }
    }

    private String addressOf(int id) {
        Member m = config.members().get(id);
        return m == null ? "" : m.host() + ":" + m.clientPort();
    }

    // ------------------------------------------------------------------ lifecycle

    /** Stops abruptly, as a crash would: no goodbye to peers or clients. The log on disk stays as it is. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeQuietly();
    }

    private void closeQuietly() {
        closed = true;
        closeQuietly(clientListener);
        for (Socket s : clientSockets) {
            closeQuietly(s);
        }
        if (transport != null) {
            transport.close();
        }
        if (runtime != null) {
            runtime.close();
        }
        if (cache != null) {
            cache.abandonPending();
            cache.close();
        }
        try {
            if (storage != null) {
                storage.close();
            }
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    private static void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }
}
