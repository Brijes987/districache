package com.distri.cache.raft;

import com.distri.cache.CacheSnapshotEntry;
import com.distri.cache.LRUCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * The cache as a Raft state machine, plus the front end a client talks to on the leader.
 *
 * <p><b>Writes always go through the log.</b> A request is appended on the leader, replicated to a majority,
 * committed, applied to the cache in log order, and only then answered. <b>Reads have two modes</b> ({@link ReadMode}).
 * In {@code LOG} mode (the default) a GET goes through the log too, which makes it linearizable without any lease or
 * clock reasoning at the price of a log append and fsync per read. In {@code READ_INDEX} mode a GET is answered from
 * the leader's copy after the leader confirms with a heartbeat round that it is still the leader (see
 * {@link RaftNode#beginRead}); that is also linearizable, and writes nothing to the log.
 *
 * <p><b>Retries are safe.</b> Each client numbers its requests. The state machine remembers, per client, the last
 * sequence number it executed and that request's result: a repeat of it returns the stored result without touching
 * the cache again, and an older number is refused as {@link Kind#STALE}. So a client that gets no answer (the leader
 * crashed, the reply was lost) retries the <em>same</em> number against any server, and the operation takes effect at
 * most once. This assumes a client has one request in flight at a time. The session table is bounded
 * ({@code maxSessions}); evicting a client's session means a very late retry from that client could apply twice.
 *
 * <p><b>Reply kinds.</b> {@link Kind#OK}: executed, the reply carries the result. {@link Kind#NOT_LEADER}: nothing
 * happened, ask {@code leaderHint} (or any server). {@link Kind#UNKNOWN}: leadership changed while the request was in
 * flight; it may or may not have been executed, so retry with the same number. {@link Kind#STALE}: an older request
 * than one already executed for this client.
 *
 * <p><b>Recovery.</b> The cache itself is not persisted: after a restart a node starts empty, restores the latest
 * snapshot if there is one, and re-applies the rest of the persisted log as the leader re-confirms the commit index.
 *
 * <p><b>TTLs.</b> A SET carries an absolute expiry fixed by the leader's clock when it accepts the request. Servers
 * then compare it with their own clocks, so skewed clocks can make servers disagree about whether a TTL'd key is still
 * visible; the log order (and therefore the ordinary key/value state) does not depend on clocks.
 *
 * <p>Not thread-safe: drive it and its {@link RaftNode} from one thread.
 */
public final class ReplicatedCache implements StateMachine, AutoCloseable {

    public enum Kind { OK, NOT_LEADER, UNKNOWN, STALE }

    /** How a GET is made linearizable. Writes are not affected. */
    public enum ReadMode { LOG, READ_INDEX }

    /** Known mistakes that tests can switch on to prove the linearizability checker notices them. TESTING ONLY. */
    enum Fault {
        /** A leader answers GET from its own copy without going through the log (stale after losing leadership). */
        LOCAL_READS,
        /** A leader answers SET and DEL as soon as it has appended the entry, before it is committed. */
        ACK_BEFORE_COMMIT
    }

    /**
     * @param found for GET and DEL: whether the key existed
     * @param value for GET: the value if found; otherwise null
     * @param leaderHint for NOT_LEADER: the believed leader, or -1
     */
    public record Reply(Kind kind, boolean found, String value, int leaderHint) {}

    private record Session(long lastSeq, Reply result) {}

    private record Pending(long term, long clientId, long seq, CompletableFuture<Reply> future) {}

    private record Queued(CacheCommand command, byte[] encoded, CompletableFuture<Reply> future) {}

    private record ReadRequest(String key, CompletableFuture<Reply> future) {}

    private record Stashed(long clientId, long seq, Reply result) {}

    private final LRUCache<String, String> cache;
    private final LongSupplier clock;
    private final int maxSessions;
    private final Map<Long, Session> sessions = new LinkedHashMap<>();
    private final Map<Long, Pending> pending = new HashMap<>();
    private final Map<Long, Stashed> stash = new HashMap<>();
    private final Map<String, Integer> executions; // null unless tracking is on (tests)
    private boolean submitting;
    private boolean batching;
    private volatile long batchesProposed;  // written by the owner thread only; readable by metrics
    private volatile long entriesBatched;
    private volatile long largestBatch;
    private final List<Queued> queued = new ArrayList<>();
    private RaftNode node;
    private Fault fault;
    private ReadMode readMode = ReadMode.LOG;
    private final Set<ReadRequest> readsInFlight = Collections.newSetFromMap(new IdentityHashMap<>());

    public ReplicatedCache(int capacity, int maxSessions, LongSupplier clock) {
        this(capacity, maxSessions, clock, false);
    }

    /** @param trackExecutions test hook: count how many times each (client, seq) was really executed */
    ReplicatedCache(int capacity, int maxSessions, LongSupplier clock, boolean trackExecutions) {
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("maxSessions must be positive");
        }
        this.cache = new LRUCache<>(capacity, 0); // no background thread: expiry is checked on access
        this.maxSessions = maxSessions;
        this.clock = clock;
        this.executions = trackExecutions ? new HashMap<>() : null;
    }

    /** Switches on a deliberate bug. For tests only; not reachable from outside the package. */
    void injectFault(Fault fault) {
        this.fault = fault;
    }

    /**
     * Group commit. With batching on, a request is not proposed at once: it waits in a queue until {@link #flush()},
     * which writes everything queued with one log append (one fsync). The caller (the node's single thread) should
     * run a batch of queued tasks and then call {@code flush()}. Off by default, so every request is proposed alone.
     */
    public void setBatching(boolean on) {
        if (!on && !queued.isEmpty()) {
            throw new IllegalStateException("flush() before switching batching off");
        }
        this.batching = on;
    }

    /** Number of {@link #flush()} calls that wrote at least one entry (each is one log append, one fsync). */
    public long batchesProposed() {
        return batchesProposed;
    }

    /** Total requests written through {@link #flush()}; divided by {@link #batchesProposed()} is the mean batch. */
    public long entriesBatched() {
        return entriesBatched;
    }

    public long largestBatch() {
        return largestBatch;
    }

    /** How many requests are waiting for {@link #flush()}. */
    public int queuedCount() {
        return queued.size();
    }

    /**
     * Proposes every queued request as one batch. Each request's future completes only once its own entry is
     * committed and applied (never earlier), or with NOT_LEADER / UNKNOWN if leadership is lost first.
     */
    public void flush() {
        if (queued.isEmpty()) {
            return;
        }
        List<Queued> batch = new ArrayList<>(queued);
        queued.clear();
        if (node.role() != RaftNode.Role.LEADER) { // demoted between submit and flush
            for (Queued q : batch) {
                q.future().complete(new Reply(Kind.NOT_LEADER, false, null, node.leaderId()));
            }
            return;
        }
        // Register the waiting futures BEFORE proposing: in a one-server group the entries commit and are applied
        // inside proposeBatch, and apply() must already find them. The indexes are known: they follow the log's end.
        long first = node.lastLogIndex() + 1;
        long term = node.term();
        for (int i = 0; i < batch.size(); i++) {
            Queued q = batch.get(i);
            pending.put(first + i, new Pending(term, q.command().clientId(), q.command().seq(), q.future()));
        }
        List<byte[]> encoded = new ArrayList<>(batch.size());
        for (Queued q : batch) {
            encoded.add(q.encoded());
        }
        RaftNode.ProposeResult r;
        try {
            r = node.proposeBatch(encoded);
        } catch (RuntimeException e) {
            for (int i = 0; i < batch.size(); i++) {
                pending.remove(first + i);
            }
            throw e;
        }
        if (r.accepted()) {
            batchesProposed++;
            entriesBatched += batch.size();
            if (batch.size() > largestBatch) {
                largestBatch = batch.size();
            }
        }
        if (!r.accepted() || r.index() != first) { // cannot happen on the owner thread; never guess if it does
            for (int i = 0; i < batch.size(); i++) {
                pending.remove(first + i);
            }
            Reply reply = r.accepted() ? unknown() : new Reply(Kind.NOT_LEADER, false, null, r.leaderHint());
            for (Queued q : batch) {
                q.future().complete(reply);
            }
        }
    }

    /** Chooses how GETs are made linearizable; see {@link ReadMode}. Takes effect for the next GET. */
    public void setReadMode(ReadMode mode) {
        this.readMode = java.util.Objects.requireNonNull(mode);
    }

    public ReadMode readMode() {
        return readMode;
    }

    /** Connects this state machine to the Raft node that feeds it. Needed before any request is submitted. */
    public void attach(RaftNode node) {
        this.node = node;
        node.setReadListener(new RaftNode.ReadListener() {
            @Override
            public void ready(Object token) {
                ReadRequest r = (ReadRequest) token;
                readsInFlight.remove(r);
                // peek, not get: a read that only the leader performs must not change the recency order, or the
                // servers would later evict different keys.
                Optional<String> value = cache.peek(r.key());
                r.future().complete(new Reply(Kind.OK, value.isPresent(), value.orElse(null), -1));
            }

            @Override
            public void failed(Object token) {
                ReadRequest r = (ReadRequest) token;
                readsInFlight.remove(r);
                r.future().complete(new Reply(Kind.NOT_LEADER, false, null, node.leaderId()));
            }
        });
    }

    // ------------------------------------------------------------------ client front end (leader)

    /** @param ttlMillis time to live in milliseconds, or -1 for none */
    public CompletableFuture<Reply> set(long clientId, long seq, String key, String value, long ttlMillis) {
        require(key, "key");
        require(value, "value");
        if (ttlMillis != -1 && ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be positive or -1");
        }
        long expiresAt = ttlMillis == -1 ? -1 : clock.getAsLong() + ttlMillis;
        return submit(CacheCommand.set(clientId, seq, key, value, expiresAt));
    }

    public CompletableFuture<Reply> delete(long clientId, long seq, String key) {
        require(key, "key");
        return submit(CacheCommand.delete(clientId, seq, key));
    }

    public CompletableFuture<Reply> get(long clientId, long seq, String key) {
        require(key, "key");
        if (fault == Fault.LOCAL_READS && node != null && node.role() == RaftNode.Role.LEADER) {
            Optional<String> local = cache.get(key);
            return CompletableFuture.completedFuture(new Reply(Kind.OK, local.isPresent(), local.orElse(null), -1));
        }
        if (readMode == ReadMode.READ_INDEX && node != null) {
            ReadRequest request = new ReadRequest(key, new CompletableFuture<>());
            readsInFlight.add(request);
            RaftNode.ReadStart start = node.beginRead(request);
            if (!start.accepted()) {
                readsInFlight.remove(request);
                return CompletableFuture.completedFuture(new Reply(Kind.NOT_LEADER, false, null, start.leaderHint()));
            }
            return request.future();
        }
        return submit(CacheCommand.get(clientId, seq, key));
    }

    private static void require(String s, String what) {
        if (s == null) {
            throw new IllegalArgumentException(what + " must not be null");
        }
    }

    private CompletableFuture<Reply> submit(CacheCommand command) {
        if (node == null) {
            throw new IllegalStateException("not attached to a RaftNode");
        }
        if (batching && fault != Fault.ACK_BEFORE_COMMIT) {
            if (node.role() != RaftNode.Role.LEADER) {
                return CompletableFuture.completedFuture(new Reply(Kind.NOT_LEADER, false, null, node.leaderId()));
            }
            CompletableFuture<Reply> future = new CompletableFuture<>();
            queued.add(new Queued(command, command.encode(), future));
            return future;
        }
        RaftNode.ProposeResult r;
        stash.clear();
        submitting = true;
        try {
            r = node.propose(command.encode()); // a one-server group commits and applies inside this call
        } finally {
            submitting = false;
        }
        if (!r.accepted()) {
            return CompletableFuture.completedFuture(new Reply(Kind.NOT_LEADER, false, null, r.leaderHint()));
        }
        Stashed already = stash.remove(r.index());
        stash.clear();
        if (already != null) {
            return CompletableFuture.completedFuture(already.clientId() == command.clientId()
                    && already.seq() == command.seq() ? already.result() : unknown());
        }
        if (fault == Fault.ACK_BEFORE_COMMIT && command.op() != CacheCommand.Op.GET) {
            boolean existed = cache.get(command.key()).isPresent();
            return CompletableFuture.completedFuture(new Reply(Kind.OK, command.op() == CacheCommand.Op.DEL && existed,
                    null, -1));
        }
        CompletableFuture<Reply> future = new CompletableFuture<>();
        Pending replaced = pending.put(r.index(), new Pending(r.term(), command.clientId(), command.seq(), future));
        if (replaced != null) {
            replaced.future().complete(unknown());
        }
        return future;
    }

    /**
     * Call regularly (every tick is fine): fails the requests of a leader that lost its leadership, since their
     * entries may now be overwritten and would otherwise never be answered.
     */
    public void poll() {
        if (pending.isEmpty()) {
            return;
        }
        var it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Pending p = it.next().getValue();
            if (node == null || node.role() != RaftNode.Role.LEADER || node.term() != p.term()) {
                p.future().complete(unknown());
                it.remove();
            }
        }
    }

    /** Fails everything in flight, for example when this server is shutting down. */
    public void abandonPending() {
        for (Queued q : queued) {
            q.future().complete(unknown());
        }
        queued.clear();
        for (Pending p : pending.values()) {
            p.future().complete(unknown());
        }
        pending.clear();
        for (ReadRequest r : new ArrayList<>(readsInFlight)) {
            r.future().complete(unknown());
        }
        readsInFlight.clear();
    }

    private static Reply unknown() {
        return new Reply(Kind.UNKNOWN, false, null, -1);
    }

    // ------------------------------------------------------------------ state machine

    @Override
    public void apply(long index, byte[] raw) {
        CacheCommand command = CacheCommand.decode(raw);
        Reply result = execute(command);
        Pending p = pending.remove(index);
        if (p != null) {
            p.future().complete(p.clientId() == command.clientId() && p.seq() == command.seq() ? result : unknown());
        } else if (submitting) {
            stash.put(index, new Stashed(command.clientId(), command.seq(), result));
        }
    }

    private Reply execute(CacheCommand c) {
        Session session = sessions.get(c.clientId());
        if (session != null) {
            if (c.seq() == session.lastSeq()) {
                return session.result(); // a retry of the request already executed
            }
            if (c.seq() < session.lastSeq()) {
                return new Reply(Kind.STALE, false, null, -1);
            }
        }
        if (executions != null) {
            executions.merge(c.clientId() + ":" + c.seq(), 1, Integer::sum);
        }
        Reply result = switch (c.op()) {
            case SET -> applySet(c);
            case DEL -> new Reply(Kind.OK, cache.delete(c.key()), null, -1);
            case GET -> {
                Optional<String> value = cache.get(c.key());
                yield new Reply(Kind.OK, value.isPresent(), value.orElse(null), -1);
            }
        };
        sessions.remove(c.clientId()); // re-insert so eviction order is by last use, identically on every server
        sessions.put(c.clientId(), new Session(c.seq(), result));
        while (sessions.size() > maxSessions) {
            sessions.remove(sessions.keySet().iterator().next());
        }
        return result;
    }

    private Reply applySet(CacheCommand c) {
        if (c.expiresAtMillis() == -1) {
            cache.put(c.key(), c.value());
        } else {
            long remaining = c.expiresAtMillis() - clock.getAsLong();
            if (remaining > 0) {
                cache.put(c.key(), c.value(), remaining);
            } else {
                cache.delete(c.key()); // already expired by the time it is applied: the key is simply absent
            }
        }
        return new Reply(Kind.OK, false, null, -1);
    }

    // ------------------------------------------------------------------ snapshots

    private static final int SNAPSHOT_VERSION = 1;

    /** The live entries (least recently used first, with their expiry times) and the client sessions, as bytes. */
    private volatile long snapshotDelayMillis;

    /**
     * For tests and experiments only: makes every {@link #snapshot()} take at least this long, to stand in for a state too
     * big to copy quickly. 0 (the default) turns it off.
     */
    public void setSnapshotDelayMillisForTesting(long millis) {
        this.snapshotDelayMillis = millis;
    }

    @Override
    public byte[] snapshot() {
        return captureSnapshot().serialize();
    }

    /**
     * Copies references to the live entries and sessions (cheap, on the node's thread). The strings are immutable and so are
     * the session records, so the copy stays exactly as it was while the node goes on applying commands; encoding it into
     * bytes is left to {@code serialize()}, which can run on any thread.
     */
    @Override
    public SnapshotCapture captureSnapshot() {
        List<CacheSnapshotEntry<String, String>> entries = cache.snapshotLruToMru();
        List<Map.Entry<Long, Session>> frozenSessions = new ArrayList<>(sessions.size());
        for (Map.Entry<Long, Session> e : sessions.entrySet()) {
            frozenSessions.add(Map.entry(e.getKey(), e.getValue())); // not the live map entry: that one can change
        }
        Map<String, Integer> frozenExecutions = executions == null ? null : new HashMap<>(executions);
        long delay = snapshotDelayMillis;
        return () -> serializeSnapshot(entries, frozenSessions, frozenExecutions, delay);
    }

    private static byte[] serializeSnapshot(List<CacheSnapshotEntry<String, String>> entries,
            List<Map.Entry<Long, Session>> sessions, Map<String, Integer> executions, long delayMillis) {
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream out = new java.io.DataOutputStream(bytes);
            out.writeInt(SNAPSHOT_VERSION);
            out.writeInt(entries.size());
            for (CacheSnapshotEntry<String, String> e : entries) {
                writeString(out, e.key());
                writeString(out, e.value());
                out.writeLong(e.expiresAtMillis());
            }
            out.writeInt(sessions.size());
            for (Map.Entry<Long, Session> e : sessions) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue().lastSeq());
                Reply r = e.getValue().result();
                out.writeByte(r.kind().ordinal());
                out.writeBoolean(r.found());
                out.writeBoolean(r.value() != null);
                if (r.value() != null) {
                    writeString(out, r.value());
                }
                out.writeInt(r.leaderHint());
            }
            out.writeBoolean(executions != null); // test instrumentation only: carries the execution counts along
            if (executions != null) {
                out.writeInt(executions.size());
                for (Map.Entry<String, Integer> e : executions.entrySet()) {
                    writeString(out, e.getKey());
                    out.writeInt(e.getValue());
                }
            }
            return bytes.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e); // cannot happen: writing to memory
        }
    }

    /** Replaces the cache and the sessions with those in {@code state}. Throws, changing nothing, if it is malformed. */
    @Override
    public void restore(byte[] state) {
        List<CacheSnapshotEntry<String, String>> entries = new ArrayList<>();
        Map<Long, Session> restored = new LinkedHashMap<>();
        Map<String, Integer> restoredExecutions = new HashMap<>();
        try {
            java.io.DataInputStream in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(state));
            if (in.readInt() != SNAPSHOT_VERSION) {
                throw new IllegalArgumentException("unknown snapshot version");
            }
            int count = in.readInt();
            if (count < 0 || count > state.length) {
                throw new IllegalArgumentException("bad entry count " + count);
            }
            for (int i = 0; i < count; i++) {
                String key = readString(in);
                String value = readString(in);
                entries.add(new CacheSnapshotEntry<>(key, value, in.readLong()));
            }
            int sessionCount = in.readInt();
            if (sessionCount < 0 || sessionCount > state.length) {
                throw new IllegalArgumentException("bad session count " + sessionCount);
            }
            Kind[] kinds = Kind.values();
            for (int i = 0; i < sessionCount; i++) {
                long clientId = in.readLong();
                long lastSeq = in.readLong();
                int kind = in.readByte();
                if (kind < 0 || kind >= kinds.length) {
                    throw new IllegalArgumentException("bad reply kind " + kind);
                }
                boolean found = in.readBoolean();
                String value = in.readBoolean() ? readString(in) : null;
                int hint = in.readInt();
                restored.put(clientId, new Session(lastSeq, new Reply(kinds[kind], found, value, hint)));
            }
            if (in.readBoolean()) {
                int tracked = in.readInt();
                if (tracked < 0 || tracked > state.length) {
                    throw new IllegalArgumentException("bad tracking count " + tracked);
                }
                for (int i = 0; i < tracked; i++) {
                    String key = readString(in);
                    restoredExecutions.put(key, in.readInt());
                }
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("trailing bytes after the snapshot");
            }
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("malformed snapshot: " + e.getMessage(), e);
        }
        // nothing above touched this object; from here on it is replaced
        cache.clear();
        long now = clock.getAsLong();
        for (CacheSnapshotEntry<String, String> e : entries) {
            if (e.expiresAtMillis() == -1) {
                cache.put(e.key(), e.value());
            } else if (e.expiresAtMillis() > now) {
                cache.put(e.key(), e.value(), e.expiresAtMillis() - now);
            }
        }
        if (executions != null) {
            executions.clear();
            executions.putAll(restoredExecutions);
        }
        sessions.clear();
        sessions.putAll(restored);
        while (sessions.size() > maxSessions) {
            sessions.remove(sessions.keySet().iterator().next());
        }
    }

    private static void writeString(java.io.DataOutputStream out, String s) throws java.io.IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(java.io.DataInputStream in) throws java.io.IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new IllegalArgumentException("bad string length " + length);
        }
        byte[] b = new byte[length];
        in.readFully(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ inspection

    /** The live entries of this server's copy, sorted by key. For tests and diagnostics. */
    public Map<String, String> contents() {
        Map<String, String> out = new TreeMap<>();
        for (CacheSnapshotEntry<String, String> e : cache.snapshotLruToMru()) {
            out.put(e.key(), e.value());
        }
        return out;
    }

    /** How many times (clientId, seq) was really executed on this server; only when tracking was enabled. */
    int executions(long clientId, long seq) {
        return executions.getOrDefault(clientId + ":" + seq, 0);
    }

    int sessionCount() {
        return sessions.size();
    }

    @Override
    public void close() {
        cache.close();
    }
}
