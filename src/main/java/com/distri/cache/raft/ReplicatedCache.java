package com.distri.cache.raft;

import com.distri.cache.CacheSnapshotEntry;
import com.distri.cache.LRUCache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * The cache as a Raft state machine, plus the front end a client talks to on the leader.
 *
 * <p><b>Writes and reads both go through the log.</b> A request is appended on the leader, replicated to a majority,
 * committed, applied to the cache in log order, and only then answered. Reading through the log makes every GET
 * linearizable without any lease or clock reasoning, at the price of a log round trip per read.
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
 * <p><b>Recovery.</b> The cache itself is not persisted: after a restart a node starts empty and re-applies the whole
 * persisted log as the leader re-confirms the commit index. The log is never compacted, so recovery time grows with
 * the log.
 *
 * <p><b>TTLs.</b> A SET carries an absolute expiry fixed by the leader's clock when it accepts the request. Servers
 * then compare it with their own clocks, so skewed clocks can make servers disagree about whether a TTL'd key is still
 * visible; the log order (and therefore the ordinary key/value state) does not depend on clocks.
 *
 * <p>Not thread-safe: drive it and its {@link RaftNode} from one thread.
 */
public final class ReplicatedCache implements StateMachine, AutoCloseable {

    public enum Kind { OK, NOT_LEADER, UNKNOWN, STALE }

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

    /** Connects this state machine to the Raft node that feeds it. Needed before any request is submitted. */
    public void attach(RaftNode node) {
        this.node = node;
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
