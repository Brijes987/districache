package com.distri.cache.node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;

/**
 * A client of a {@link ReplicatedServer} group that finds the leader, follows redirects and retries safely.
 *
 * <p>Every operation gets a sequence number once, and every retry of it (after a lost reply, a crashed leader, a
 * redirect) reuses that number. The servers remember the last number executed for this client, so a retry returns the
 * stored answer instead of running again: <b>each operation takes effect at most once</b>. If the whole deadline passes
 * without an answer, {@link ReplicatedClientException#outcomeUnknown()} tells the caller the operation may or may not
 * have happened.
 *
 * <p>One operation at a time per client; methods are synchronised. Use one client per thread for concurrency, because
 * the exactly-once guarantee relies on a client having a single request in flight. The group's servers are
 * identified by their client-port addresses.
 */
public final class ReplicatedClient implements AutoCloseable {

    private static final String ROLE = "raft-client";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final List<InetSocketAddress> servers;
    private final byte[] secret;
    private final javax.net.ssl.SSLContext tls; // null: plain TCP
    private final long operationTimeoutMillis;
    private final long clientId;
    private long seq;
    private int target;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public ReplicatedClient(List<InetSocketAddress> servers) {
        this(servers, null, 15_000);
    }

    /**
     * @param secret the group's client secret, or null
     * @param operationTimeoutMillis how long one operation keeps retrying before giving up
     */
    public ReplicatedClient(List<InetSocketAddress> servers, byte[] secret, long operationTimeoutMillis) {
        this(servers, secret, operationTimeoutMillis, null);
    }

    /**
     * @param tls if not null, connections use TLS: the server's certificate must be trusted by this context and match
     *     the host name in the address ({@link com.distri.cache.server.Tls#clientContext})
     */
    public ReplicatedClient(List<InetSocketAddress> servers, byte[] secret, long operationTimeoutMillis,
            javax.net.ssl.SSLContext tls) {
        this.tls = tls;
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("at least one server address is required");
        }
        if (operationTimeoutMillis <= 0) {
            throw new IllegalArgumentException("operationTimeoutMillis must be positive");
        }
        this.servers = List.copyOf(servers);
        this.secret = secret == null ? null : secret.clone();
        this.operationTimeoutMillis = operationTimeoutMillis;
        this.clientId = RANDOM.nextLong() & Long.MAX_VALUE; // a random positive id, unique in practice
        this.target = RANDOM.nextInt(servers.size());
    }

    public synchronized void set(String key, String value) throws ReplicatedClientException {
        set(key, value, -1);
    }

    /** @param ttlMillis time to live in milliseconds, or -1 for none */
    public synchronized void set(String key, String value, long ttlMillis) throws ReplicatedClientException {
        check(key, "key");
        check(value, "value");
        execute(ClientProtocol.SET, key, value, ttlMillis);
    }

    public synchronized Optional<String> get(String key) throws ReplicatedClientException {
        check(key, "key");
        ClientProtocol.Response r = execute(ClientProtocol.GET, key, "", -1);
        return r.found() ? Optional.of(r.value()) : Optional.empty();
    }

    /** @return true if the key existed and was removed */
    public synchronized boolean delete(String key) throws ReplicatedClientException {
        check(key, "key");
        return execute(ClientProtocol.DEL, key, "", -1).found();
    }

    public long clientId() {
        return clientId;
    }

    private static void check(String s, String what) {
        if (s == null) {
            throw new IllegalArgumentException(what + " must not be null");
        }
    }

    private ClientProtocol.Response execute(byte op, String key, String value, long ttl)
            throws ReplicatedClientException {
        long number = ++seq; // allocated once; every retry below reuses it
        long deadline = System.nanoTime() + operationTimeoutMillis * 1_000_000;
        boolean sent = false;
        Throwable lastProblem = null;
        long pause = 20;
        while (System.nanoTime() < deadline) {
            if (Thread.currentThread().isInterrupted()) {
                disconnect();
                throw new ReplicatedClientException("interrupted"
                        + (sent ? "; the operation may or may not have been executed" : ""), sent, null);
            }
            try {
                connect(target);
                long remainingMillis = Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
                socket.setSoTimeout((int) Math.min(remainingMillis, 8_000));
                Frames.write(out, ClientProtocol.encode(
                        new ClientProtocol.Request(op, clientId, number, key, value, ttl)));
                out.flush();
                sent = true;
                ClientProtocol.Response response = ClientProtocol.decodeResponse(Frames.read(in));
                switch (response.status()) {
                    case ClientProtocol.OK:
                        lastAnsweredBy = servers.get(target);
                        return response;
                    case ClientProtocol.NOT_LEADER:
                        int hinted = indexOf(response.hint());
                        if (hinted >= 0) {
                            target = hinted;
                            pause = 20;
                        } else {
                            target = (target + 1) % servers.size();
                            sleep(pause);
                            pause = Math.min(pause * 2, 400);
                        }
                        break;
                    case ClientProtocol.UNKNOWN:
                        sleep(pause);
                        pause = Math.min(pause * 2, 400);
                        break;
                    case ClientProtocol.ERROR:
                        throw new ReplicatedClientException("the server rejected the request: " + response.message(),
                                false, null);
                    default:
                        throw new IllegalStateException("server answered STALE to a request this client had just "
                                + "numbered; the client id is not unique or the client is used from two threads");
                }
            } catch (IOException e) {
                lastProblem = e;
                disconnect();
                target = (target + 1) % servers.size();
                sleep(pause);
                pause = Math.min(pause * 2, 400);
            }
        }
        disconnect();
        target = (target + 1) % servers.size(); // the server we were stuck on may be cut off: try another next time
        throw new ReplicatedClientException("no answer within " + operationTimeoutMillis + " ms"
                + (lastProblem == null ? "" : " (last problem: " + lastProblem + ")")
                + (sent ? "; the operation may or may not have been executed" : ""), sent, lastProblem);
    }

    private int indexOf(String hint) {
        if (hint.isEmpty()) {
            return -1;
        }
        for (int i = 0; i < servers.size(); i++) {
            InetSocketAddress a = servers.get(i);
            if ((a.getHostString() + ":" + a.getPort()).equals(hint)) {
                return i;
            }
        }
        return -1;
    }

    private void connect(int index) throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed() && connectedTo == index) {
            return;
        }
        disconnect();
        Socket s = new Socket();
        try {
            s.connect(servers.get(index), 500);
            s.setTcpNoDelay(true);
            s.setSoTimeout(3_000);
            InetSocketAddress address = servers.get(index);
            Socket io = tls == null ? s
                    : com.distri.cache.server.Tls.connectTls(tls, s, address.getHostString(), address.getPort());
            DataInputStream newIn = new DataInputStream(new BufferedInputStream(io.getInputStream()));
            DataOutputStream newOut = new DataOutputStream(new BufferedOutputStream(io.getOutputStream()));
            byte[] role = ROLE.getBytes(StandardCharsets.UTF_8);
            newOut.writeInt(role.length + 4);
            newOut.write(role);
            newOut.writeInt(0);
            newOut.flush();
            Handshake.connect(newIn, newOut, secret, ROLE, 0);
            socket = io;
            in = newIn;
            out = newOut;
            connectedTo = index;
        } catch (IOException e) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already failing
            }
            throw e;
        }
    }

    private int connectedTo = -1;
    private InetSocketAddress lastAnsweredBy;

    /**
     * The server that gave the most recent successful answer. Every successful answer comes from the leader, so after
     * an operation this is the leader's client address; null before the first success.
     */
    public synchronized InetSocketAddress lastAnsweredBy() {
        return lastAnsweredBy;
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
        }
        socket = null;
        connectedTo = -1;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void close() {
        disconnect();
    }
}
