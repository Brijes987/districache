package com.distri.cache.node;

import com.distri.cache.raft.RaftMessage;
import com.distri.cache.raft.Transport;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft messages between servers over TCP.
 *
 * <p>Each server keeps one outgoing connection per peer (written by one thread per peer, so a slow or dead peer cannot
 * delay the others) and accepts incoming connections from the peers. {@link #send} never blocks: it puts the message in
 * a bounded queue and, if the queue is full, drops it. Raft tolerates lost, delayed, duplicated and reordered messages,
 * so dropping is always safe; the heartbeat resends what matters. After a connection fails, queued messages are
 * discarded on reconnect, because stale ones would only waste bandwidth.
 *
 * <p>Peers prove they know the cluster secret with {@link Handshake}. Without a secret anyone who can reach the port can
 * inject Raft messages; configure one. Traffic is not encrypted.
 */
final class PeerTransport implements Transport, AutoCloseable {

    interface Sink {
        void deliver(int from, RaftMessage message);
    }

    private static final String ROLE = "raft-peer";
    private static final int QUEUE_CAPACITY = 4_096;
    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final int HANDSHAKE_TIMEOUT_MILLIS = 3_000;

    private final int selfId;
    private final Map<Integer, InetSocketAddress> peers;
    private final byte[] secret;
    private final Sink sink;
    private final Map<Integer, BlockingQueue<byte[]>> queues = new ConcurrentHashMap<>();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final Semaphore inboundSlots;
    private final AtomicLong dropped = new AtomicLong();
    private volatile boolean closed;
    private volatile Set<Integer> blocked = Set.of();
    private ServerSocket serverSocket;
    private final javax.net.ssl.SSLContext tls; // null: plain TCP

    PeerTransport(int selfId, Map<Integer, InetSocketAddress> peers, byte[] secret, Sink sink) {
        this(selfId, peers, secret, sink, null);
    }

    /** @param tls if not null, every peer connection uses TLS, and both ends must present a trusted certificate */
    PeerTransport(int selfId, Map<Integer, InetSocketAddress> peers, byte[] secret, Sink sink,
            javax.net.ssl.SSLContext tls) {
        this.tls = tls;
        this.selfId = selfId;
        this.peers = Map.copyOf(peers);
        this.secret = secret == null ? null : secret.clone();
        this.sink = sink;
        this.inboundSlots = new Semaphore(Math.max(4, peers.size() * 4));
        for (int peer : peers.keySet()) {
            queues.put(peer, new ArrayBlockingQueue<>(QUEUE_CAPACITY));
        }
    }

    void start(String bindHost, int port) throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(InetAddress.getByName(bindHost), port));
        spawn("raft-accept-" + selfId, this::acceptLoop);
        for (Map.Entry<Integer, InetSocketAddress> peer : peers.entrySet()) {
            spawn("raft-send-" + selfId + "-to-" + peer.getKey(), () -> sendLoop(peer.getKey(), peer.getValue()));
        }
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    /** Messages dropped because a peer's queue was full. For diagnostics. */
    long dropped() {
        return dropped.get();
    }

    /** Test hook: silently drop every message to and from these peers, as a network partition would. */
    void block(Set<Integer> peers) {
        this.blocked = Set.copyOf(peers);
    }

    @Override
    public void send(int to, RaftMessage message) {
        BlockingQueue<byte[]> queue = queues.get(to);
        if (queue == null || closed || blocked.contains(to)) {
            return;
        }
        if (!queue.offer(RaftMessageCodec.encode(message))) {
            dropped.incrementAndGet();
        }
    }

    // ------------------------------------------------------------------ outgoing

    private void sendLoop(int peer, InetSocketAddress address) {
        BlockingQueue<byte[]> queue = queues.get(peer);
        long backoff = 50;
        while (!closed) {
            Socket socket = new Socket();
            try {
                socket.connect(address, CONNECT_TIMEOUT_MILLIS);
                sockets.add(socket);
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(HANDSHAKE_TIMEOUT_MILLIS);
                Socket io = tls == null ? socket
                        : com.distri.cache.server.Tls.connectTls(tls, socket, address.getHostString(), address.getPort());
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(io.getOutputStream()));
                DataInputStream in = new DataInputStream(new BufferedInputStream(io.getInputStream()));
                writeHello(out);
                Handshake.connect(in, out, secret, ROLE, selfId);
                socket.setSoTimeout(0);
                queue.clear(); // anything queued while the link was down is stale
                backoff = 50;
                while (!closed) {
                    byte[] frame = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (frame != null) {
                        Frames.write(out, frame);
                        // drain what is already waiting so a burst goes out in one flush
                        while ((frame = queue.poll()) != null) {
                            Frames.write(out, frame);
                        }
                        out.flush();
                    }
                }
            } catch (IOException e) {
                // peer down, refused or the link broke: retry after a short pause
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                sockets.remove(socket);
                closeQuietly(socket);
            }
            if (closed) {
                return;
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            backoff = Math.min(backoff * 2, 1_000);
        }
    }

    private void writeHello(DataOutputStream out) throws IOException {
        byte[] role = ROLE.getBytes(StandardCharsets.UTF_8);
        DataOutputStream hello = out;
        hello.writeInt(role.length + 4);
        hello.write(role);
        hello.writeInt(selfId);
        hello.flush();
    }

    // ------------------------------------------------------------------ incoming

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                return; // closed
            }
            if (!inboundSlots.tryAcquire()) {
                closeQuietly(socket); // too many connections: refuse rather than exhaust threads
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    receiveLoop(socket);
                } finally {
                    inboundSlots.release();
                    sockets.remove(socket);
                    closeQuietly(socket);
                }
            }, "raft-recv-" + selfId);
            t.setDaemon(true);
            t.start();
        }
    }

    private void receiveLoop(Socket socket) {
        try {
            sockets.add(socket);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(HANDSHAKE_TIMEOUT_MILLIS);
            Socket io = tls == null ? socket : com.distri.cache.server.Tls.acceptTls(tls, socket, true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(io.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(io.getOutputStream()));
            byte[] hello = Frames.read(in, 256);
            if (hello.length < 4) {
                return;
            }
            String role = new String(hello, 0, hello.length - 4, StandardCharsets.UTF_8);
            int from = java.nio.ByteBuffer.wrap(hello, hello.length - 4, 4).getInt();
            if (!ROLE.equals(role) || !peers.containsKey(from)) {
                return; // not one of ours
            }
            Handshake.accept(in, out, secret, ROLE, from);
            socket.setSoTimeout(0);
            while (!closed) {
                RaftMessage message = RaftMessageCodec.decode(Frames.read(in));
                if (!blocked.contains(from)) {
                    sink.deliver(from, message);
                }
            }
        } catch (IOException e) {
            // connection ended, failed the handshake or sent something malformed: drop it
        }
    }

    private static void spawn(String name, Runnable body) {
        // started by start(); kept in a list so close() can interrupt them
        Thread t = new Thread(body, name);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(serverSocket);
        for (Socket s : sockets) {
            closeQuietly(s);
        }
    }

    private static void closeQuietly(java.io.Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }
}
