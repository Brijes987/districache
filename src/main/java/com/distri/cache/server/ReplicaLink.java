package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.client.ClientSecurity;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The replica's side of replication: connects to a primary, asks to continue from the last offset it applied
 * ({@code PSYNC <replid> <offset>}), loads a full copy when told to, then applies every record the primary sends.
 * When the connection drops it keeps what it has (and keeps serving it, possibly stale) and reconnects with a
 * growing delay.
 *
 * <p>While a full resync is loading, the replica clears its cache first, so reads during that time see incomplete
 * data. Offsets are counted in bytes of the stream, using the reader's position, so they match the primary's.
 */
final class ReplicaLink implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ReplicaLink.class.getName());

    private final String host;
    private final int port;
    private final ClientSecurity security;
    private final ShardedLRUCache<String, String> cache;
    private final WriteCoordinator coordinator;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final long ackIntervalMillis;
    private final long maxBackoffMillis;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile Socket socket;

    private volatile String replid;            // the history this replica's data belongs to; null before the first sync
    private volatile long offset = -1;         // bytes of that history applied so far
    private volatile boolean connected;
    private volatile long lastIoMillis;
    private volatile String lastError = "";
    private final AtomicLong fullSyncs = new AtomicLong();
    private final AtomicLong partialSyncs = new AtomicLong();
    private final AtomicLong connections = new AtomicLong();

    ReplicaLink(String host, int port, ClientSecurity security, ShardedLRUCache<String, String> cache,
            WriteCoordinator coordinator, int connectTimeoutMillis, int readTimeoutMillis, long ackIntervalMillis,
            long maxBackoffMillis) {
        this.host = host;
        this.port = port;
        this.security = security;
        this.cache = cache;
        this.coordinator = coordinator;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.ackIntervalMillis = ackIntervalMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    String primaryAddress() {
        return host + ":" + port;
    }

    boolean isConnected() {
        return connected;
    }

    long offset() {
        return offset;
    }

    String replid() {
        return replid;
    }

    long fullSyncs() {
        return fullSyncs.get();
    }

    long partialSyncs() {
        return partialSyncs.get();
    }

    long connections() {
        return connections.get();
    }

    long lastIoAgeMillis() {
        return lastIoMillis == 0 ? -1 : System.currentTimeMillis() - lastIoMillis;
    }

    String lastError() {
        return lastError;
    }

    synchronized ReplicaLink start() {
        if (running) {
            throw new IllegalStateException("already started");
        }
        running = true;
        thread = new Thread(this::run, "districache-replica-link");
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    @Override
    public void close() {
        running = false;
        Socket s = socket;
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        long backoff = 100;
        while (running) {
            long before = System.currentTimeMillis();
            try {
                session();
            } catch (IOException | RuntimeException e) {
                if (running) {
                    lastError = String.valueOf(e.getMessage());
                    LOG.log(System.Logger.Level.WARNING, "replication link to " + primaryAddress() + " failed: " + e);
                }
            }
            connected = false;
            if (!running) {
                return;
            }
            // a session that lasted a while resets the delay; quick failures back off
            backoff = System.currentTimeMillis() - before > 5_000 ? 100 : Math.min(backoff * 2, maxBackoffMillis);
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void session() throws IOException {
        Socket s = security.open(host, port, connectTimeoutMillis, readTimeoutMillis);
        socket = s;
        connections.incrementAndGet();
        try (s) {
            OutputStream out = s.getOutputStream();
            RespReader reader = new RespReader(s.getInputStream());
            // Replies are single lines such as "+OK" or "+FULLRESYNC id 123 4"; the reader's inline-command
            // parser splits them on spaces, which is exactly what is needed here.
            if (security.password() != null) {
                out.write(AppendOnlyLog.encode("AUTH", security.password()));
                out.flush();
                List<byte[]> reply = readReply(reader);
                if (!text(reply.get(0)).equals("+OK")) {
                    throw new IOException("authentication to the primary failed: " + String.join(" ", strings(reply)));
                }
            }
            String askId = replid == null ? "?" : replid;
            long askOffset = replid == null ? -1 : offset;
            out.write(AppendOnlyLog.encode("PSYNC", askId, Long.toString(askOffset)));
            out.flush();
            List<byte[]> reply = readReply(reader);
            String kind = text(reply.get(0));
            long startOffset;
            if (kind.equals("+FULLRESYNC") && reply.size() == 4) {
                String newId = text(reply.get(1));
                startOffset = Long.parseLong(text(reply.get(2)));
                long count = Long.parseLong(text(reply.get(3)));
                coordinator.clearForResync();
                for (long i = 0; i < count; i++) {
                    List<byte[]> record = reader.readCommand();
                    if (record == null) {
                        throw new IOException("primary closed the connection during the full resync");
                    }
                    coordinator.applyReplicated(record);
                    lastIoMillis = System.currentTimeMillis();
                }
                replid = newId;
                offset = startOffset;
                coordinator.compactAfterResync();
                fullSyncs.incrementAndGet();
            } else if (kind.equals("+CONTINUE") && reply.size() >= 2) {
                startOffset = offset;
                replid = text(reply.get(1));
                partialSyncs.incrementAndGet();
            } else {
                throw new IOException("primary refused to replicate: " + String.join(" ", strings(reply)));
            }
            long base = reader.position(); // stream offsets are counted from here
            connected = true;
            lastError = "";
            lastIoMillis = System.currentTimeMillis();

            Thread acker = new Thread(() -> sendAcks(s, out), "districache-replica-acks");
            acker.setDaemon(true);
            acker.start();
            try {
                while (running) {
                    List<byte[]> record = reader.readCommand();
                    if (record == null) {
                        throw new IOException("primary closed the connection");
                    }
                    if (!record.isEmpty()) {
                        coordinator.applyReplicated(record);
                    }
                    offset = startOffset + (reader.position() - base);
                    lastIoMillis = System.currentTimeMillis();
                }
            } finally {
                acker.interrupt();
            }
        } finally {
            socket = null;
        }
    }

    private void sendAcks(Socket s, OutputStream out) {
        try {
            while (!Thread.currentThread().isInterrupted() && !s.isClosed()) {
                synchronized (out) {
                    out.write(AppendOnlyLog.encode("REPLCONF", "ACK", Long.toString(offset)));
                    out.flush();
                }
                Thread.sleep(ackIntervalMillis);
            }
        } catch (IOException | InterruptedException e) {
            // the session is ending
        }
    }

    private static List<byte[]> readReply(RespReader reader) throws IOException {
        List<byte[]> reply = reader.readCommand();
        if (reply == null || reply.isEmpty()) {
            throw new IOException("primary closed the connection");
        }
        return reply;
    }

    private static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static List<String> strings(List<byte[]> parts) {
        return parts.stream().map(ReplicaLink::text).toList();
    }
}
