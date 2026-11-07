package com.distri.cache.server;

import com.distri.cache.CacheSnapshotEntry;
import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The primary's side of replication: a numbered stream of every write, a bounded backlog of it, and one session per
 * connected replica.
 *
 * <p>The stream is the same records the append-only log holds ({@code SET}, {@code DEL}, plus {@code PING} as a
 * heartbeat). Its <em>offset</em> is the number of bytes published so far, so a replica that has applied everything
 * up to offset N can ask for "everything after N". The primary starts with a random replication id; a replica only
 * resumes from an offset if the id is the one it last saw.
 *
 * <p>A replica that connects is either continued (its offset is still inside the backlog) or fully resynchronised:
 * it gets a copy of the cache taken while writes are paused, together with the stream offset of that instant, and
 * then every write after it. A replica that falls further behind than the backlog is disconnected and will do a full
 * resync when it comes back.
 *
 * <p>This is <b>asynchronous</b> replication: the primary answers a client before any replica has the write.
 */
final class ReplicationHub implements AutoCloseable {

    static final long DEFAULT_BACKLOG_BYTES = 16L << 20;
    static final long DEFAULT_HEARTBEAT_MILLIS = 1_000;
    static final long DEFAULT_ACK_TIMEOUT_MILLIS = 60_000;
    static final int MAX_REPLICAS = 16;

    private static final System.Logger LOG = System.getLogger(ReplicationHub.class.getName());

    private final Object monitor = new Object();
    private final long backlogLimit;
    private final long ackTimeoutMillis;
    private final TreeMap<Long, byte[]> backlog = new TreeMap<>(); // start offset -> record; guarded by monitor
    private long backlogSize;                                       // guarded by monitor
    private long offset;                                            // guarded by monitor: end of the stream
    private String replid = newReplId();                            // guarded by monitor
    private volatile boolean primary = true;
    private volatile boolean closed;

    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private final AtomicLong fullSyncs = new AtomicLong();
    private final AtomicLong partialSyncs = new AtomicLong();
    private final ScheduledExecutorService heartbeat;

    ReplicationHub(long backlogBytes, long heartbeatMillis, long ackTimeoutMillis) {
        if (backlogBytes <= 0) {
            throw new IllegalArgumentException("backlog must be positive");
        }
        this.backlogLimit = backlogBytes;
        this.ackTimeoutMillis = ackTimeoutMillis;
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "districache-repl-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeat.scheduleWithFixedDelay(() -> {
            WriteCoordinator c = coordinator;
            if (primary && !sessions.isEmpty() && c != null) {
                c.publishHeartbeat();
            }
        }, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    }

    private volatile WriteCoordinator coordinator;

    /** The hub is created first; the coordinator that publishes into it is attached right after. */
    void attach(WriteCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    // ------------------------------------------------------------------ the stream

    boolean isPrimary() {
        return primary;
    }

    String replid() {
        synchronized (monitor) {
            return replid;
        }
    }

    long offset() {
        synchronized (monitor) {
            return offset;
        }
    }

    long backlogBytes() {
        synchronized (monitor) {
            return backlogSize;
        }
    }

    long fullSyncs() {
        return fullSyncs.get();
    }

    long partialSyncs() {
        return partialSyncs.get();
    }

    int replicaCount() {
        return sessions.size();
    }

    /** Appends one encoded record to the stream. The caller holds the write coordinator's lock. */
    void publish(byte[] record) {
        synchronized (monitor) {
            backlog.put(offset, record);
            backlogSize += record.length;
            offset += record.length;
            while (backlogSize > backlogLimit && backlog.size() > 1) {
                Map.Entry<Long, byte[]> oldest = backlog.pollFirstEntry();
                backlogSize -= oldest.getValue().length;
            }
            monitor.notifyAll();
        }
    }

    /**
     * Starts a new history: a new replication id and an empty backlog, continuing the offset numbering at
     * {@code newOffset}. Existing replica sessions are closed (their id is no longer valid).
     */
    void startNewHistory(long newOffset, boolean nowPrimary) {
        synchronized (monitor) {
            replid = newReplId();
            backlog.clear();
            backlogSize = 0;
            offset = newOffset;
            primary = nowPrimary;
            monitor.notifyAll();
        }
        for (Session s : sessions) {
            s.close();
        }
    }

    /** For tests: cut every replica connection without changing the history, as a network failure would. */
    void dropReplicaConnections() {
        for (Session s : sessions) {
            s.close();
        }
    }

    @Override
    public void close() {
        closed = true;
        heartbeat.shutdownNow();
        for (Session s : sessions) {
            s.close();
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private static String newReplId() {
        byte[] bytes = new byte[20];
        new SecureRandom().nextBytes(bytes);
        StringBuilder sb = new StringBuilder(40);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static final class FellBehind extends Exception {
        private static final long serialVersionUID = 1L;

        FellBehind() {
            super(null, null, false, false);
        }
    }

    /** The next record at {@code sent}, or null after {@code waitMillis} with nothing new. */
    private byte[] next(long sent, long waitMillis) throws FellBehind, InterruptedException {
        synchronized (monitor) {
            if (sent > offset) {
                throw new FellBehind(); // the history was reset
            }
            if (sent == offset) {
                if (waitMillis > 0 && !closed) {
                    monitor.wait(waitMillis);
                }
                if (sent == offset) {
                    return null;
                }
            }
            byte[] record = backlog.get(sent);
            if (record == null) {
                throw new FellBehind(); // trimmed from the backlog
            }
            return record;
        }
    }

    // ------------------------------------------------------------------ one replica

    /** One connected replica. */
    final class Session {
        final String address;
        final long startedMillis = System.currentTimeMillis();
        volatile long ackedOffset = -1;
        volatile long lastAckMillis = System.currentTimeMillis();
        volatile long sentOffset;
        volatile boolean closed;
        private final Socket socket;

        Session(Socket socket) {
            this.socket = socket;
            this.address = String.valueOf(socket.getRemoteSocketAddress());
        }

        void close() {
            closed = true;
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
    }

    /** A description of each connected replica for REPLINFO. */
    List<String> describeReplicas() {
        long end = offset();
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        int i = 0;
        for (Session s : sessions) {
            long acked = s.ackedOffset;
            out.add("replica" + i++ + ":addr=" + s.address + ",acked=" + acked
                    + ",lag_bytes=" + (acked < 0 ? "unknown" : Long.toString(end - acked))
                    + ",ack_age_ms=" + (System.currentTimeMillis() - s.lastAckMillis));
        }
        return out;
    }

    /**
     * Serves one replica on this connection until it disconnects or falls behind. Called on the connection's own
     * thread after the replica sent {@code PSYNC <replid> <offset>}; {@code reader} is handed over to read the
     * replica's acknowledgements.
     */
    void serveReplica(Socket socket, RespReader reader, RespWriter writer, String requestedId, long requestedOffset,
            ShardedLRUCache<String, String> cache, WriteCoordinator coordinator) throws IOException {
        if (sessions.size() >= MAX_REPLICAS) {
            writer.error("ERR too many replicas");
            writer.flush();
            return;
        }
        Session session = new Session(socket);
        sessions.add(session);
        try {
            String id;
            long start;
            List<CacheSnapshotEntry<String, String>> snapshot = null;
            // Writes are paused while the decision and (for a full resync) the copy are made, so the copy and the
            // stream offset describe the same instant.
            coordinator.lock();
            try {
                synchronized (monitor) {
                    id = replid;
                    boolean canContinue = requestedId.equals(replid) && requestedOffset >= 0
                            && requestedOffset <= offset
                            && (requestedOffset == offset || backlog.containsKey(requestedOffset));
                    if (canContinue) {
                        start = requestedOffset;
                    } else {
                        start = offset;
                    }
                    if (!canContinue) {
                        snapshot = cache.snapshot();
                    }
                }
            } finally {
                coordinator.unlock();
            }
            if (snapshot == null) {
                partialSyncs.incrementAndGet();
                writer.simple("CONTINUE " + id);
            } else {
                fullSyncs.incrementAndGet();
                writer.simple("FULLRESYNC " + id + " " + start + " " + snapshot.size());
                for (CacheSnapshotEntry<String, String> e : snapshot) {
                    writer.raw(e.expiresAtMillis() > 0
                            ? AppendOnlyLog.encode("SET", e.key(), e.value(), "PXAT", Long.toString(e.expiresAtMillis()))
                            : AppendOnlyLog.encode("SET", e.key(), e.value()));
                }
                snapshot = null; // let it be collected while streaming
            }
            writer.flush();
            session.sentOffset = start;
            session.lastAckMillis = System.currentTimeMillis();

            Thread acks = new Thread(() -> readAcks(session, reader), "districache-repl-acks");
            acks.setDaemon(true);
            acks.start();

            long sent = start;
            try {
                while (!session.closed && !closed && primary) {
                    byte[] record = next(sent, 200);
                    if (record == null) {
                        if (System.currentTimeMillis() - session.lastAckMillis > ackTimeoutMillis) {
                            LOG.log(System.Logger.Level.WARNING,
                                    "replica " + session.address + " sent no acknowledgement for " + ackTimeoutMillis + " ms; dropping it");
                            return;
                        }
                        continue;
                    }
                    while (record != null) {
                        writer.raw(record);
                        sent += record.length;
                        session.sentOffset = sent;
                        record = next(sent, 0);
                    }
                    writer.flush();
                }
            } catch (FellBehind e) {
                LOG.log(System.Logger.Level.INFO,
                        "replica " + session.address + " fell behind the backlog; it will need a full resync");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            sessions.remove(session);
            session.close();
        }
    }

    private void readAcks(Session session, RespReader reader) {
        try {
            while (!session.closed) {
                List<byte[]> command = reader.readCommand();
                if (command == null) {
                    return;
                }
                if (command.size() == 3 && text(command.get(0)).equalsIgnoreCase("REPLCONF")
                        && text(command.get(1)).equalsIgnoreCase("ACK")) {
                    try {
                        session.ackedOffset = Long.parseLong(text(command.get(2)));
                        session.lastAckMillis = System.currentTimeMillis();
                    } catch (NumberFormatException ignored) {
                        // a malformed ACK is ignored
                    }
                }
            }
        } catch (IOException e) {
            // the replica went away
        } finally {
            session.close();
        }
    }

    private static String text(byte[] b) {
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
}
