package com.distri.cache.raft;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * A deterministic simulation of a Raft group: virtual time, a network that can drop, delay, duplicate, reorder and
 * partition messages, and nodes that can crash and restart on the storage they left behind. Everything random comes
 * from one seed, so a failing run can be replayed exactly.
 *
 * <p>After <b>every</b> tick it checks Raft's safety properties and throws an AssertionError (with the seed and the
 * recent events) if one is broken:
 * <ul>
 *   <li>Election safety: at most one leader is ever seen for any term.</li>
 *   <li>Commit agreement (state machine safety): once an index is committed anywhere, every node that ever commits
 *       that index commits the same entry; and nodes only apply committed commands, in order.</li>
 *   <li>Leader completeness: a leader of term T holds every entry that was committed in a term before T.</li>
 *   <li>Log matching: if two logs hold an entry with the same index and term, they are identical up to there.</li>
 *   <li>A leader never changes its own log except by appending; terms and commit indexes never go backwards.</li>
 * </ul>
 */
final class SimCluster {

    /** One command a node's state machine has applied. */
    record Applied(long index, String command) {}

    private record Envelope(long deliverAt, long seq, int from, int to, RaftMessage message) {}

    /** Lets a test put its own state machine behind each simulated node. Called again after every restart. */
    interface NodeFactory {
        StateMachine create(int node);

        void started(int node, RaftNode raftNode);

        default void stopped(int node) {
        }

        /** Return storage to use for this (re)start, for example one reopened from disk; null keeps the default. */
        default RaftStorage openStorage(int node) {
            return null;
        }
    }

    private static final class RecordingStateMachine implements StateMachine {
        final List<Applied> applied = new ArrayList<>();
        final StateMachine delegate;
        long lastIndex;

        RecordingStateMachine(StateMachine delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] snapshot() {
            byte[] inner = delegate == null ? new byte[0] : delegate.snapshot();
            return java.nio.ByteBuffer.allocate(8 + inner.length).putLong(lastIndex).put(inner).array();
        }

        /** Freezes the index and the delegate's state now; the bytes are produced later, as the real node's worker does. */
        @Override
        public SnapshotCapture captureSnapshot() {
            long frozenIndex = lastIndex;
            SnapshotCapture inner = delegate == null ? null : delegate.captureSnapshot();
            return () -> {
                byte[] bytes = inner == null ? new byte[0] : inner.serialize();
                return java.nio.ByteBuffer.allocate(8 + bytes.length).putLong(frozenIndex).put(bytes).array();
            };
        }

        @Override
        public void restore(byte[] state) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(state);
            long index = buf.getLong();
            byte[] inner = new byte[buf.remaining()];
            buf.get(inner);
            if (delegate != null) {
                delegate.restore(inner);
            }
            lastIndex = index;
        }

        @Override
        public void apply(long index, byte[] command) {
            if (index <= lastIndex) {
                throw new AssertionError("state machine applied index " + index + " after " + lastIndex);
            }
            lastIndex = index;
            applied.add(new Applied(index, new String(command, StandardCharsets.UTF_8)));
            if (delegate != null) {
                delegate.apply(index, command);
            }
        }
    }

    final int n;
    final long seed;
    final RaftConfig config;
    final Random random;                // network and chaos randomness (separate from the nodes' timers)
    long now;

    private final AuditStorage[] storages;
    private final RaftNode[] nodes;
    private final RecordingStateMachine[] machines;
    private final boolean[][] cut;
    private final PriorityQueue<Envelope> inFlight = new PriorityQueue<>(
            java.util.Comparator.comparingLong(Envelope::deliverAt).thenComparingLong(Envelope::seq));
    private long nextSeq;

    // network behaviour
    double dropRate;
    double duplicateRate;
    int minDelay = 1;
    int maxDelay = 3;

    // invariant bookkeeping
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final Map<Long, LogEntry> committed = new HashMap<>();
    private final Map<Long, Long> committedInTerm = new HashMap<>();
    private final long[] checkedCommit;
    private final long[] lastTerm;
    private final long[] lastCommitIndex;
    private final List<List<LogEntry>> leaderPrefix;
    private final long[] leaderPrefixTerm;
    private final long[] leaderPrefixBase;
    private final Map<Long, String> appliedAt = new HashMap<>();
    private final Deque<String> events = new ArrayDeque<>();
    private int stepsSinceDeepCheck;
    long messagesSent;
    long messagesDelivered;

    private final NodeFactory factory;

    /** A snapshot a node has handed to its (simulated) background worker; it finishes {@code dueAt} steps from the start. */
    private record SnapshotJob(long dueAt, java.util.function.Supplier<RaftNode.SnapshotOutcome> work,
            java.util.function.Consumer<RaftNode.SnapshotOutcome> done) {}

    private final int snapshotDelaySteps;               // negative: snapshots are taken inline on the node's thread
    private final Random snapshotRandom;                // its own stream, so turning this on does not change other runs
    private final List<List<SnapshotJob>> snapshotJobs = new ArrayList<>();

    SimCluster(int n, long seed, RaftConfig config) {
        this(n, seed, config, null);
    }

    SimCluster(int n, long seed, RaftConfig config, NodeFactory factory) {
        this(n, seed, config, factory, -1);
    }

    /**
     * @param snapshotDelaySteps negative: a node encodes and writes its snapshots itself, at once. Zero or more: it hands
     *     them to a background worker that finishes each after a random number of steps from 0 to this value, while the
     *     node goes on; a crash loses whatever was in progress.
     */
    SimCluster(int n, long seed, RaftConfig config, NodeFactory factory, int snapshotDelaySteps) {
        this.factory = factory;
        this.snapshotDelaySteps = snapshotDelaySteps;
        this.snapshotRandom = new Random(seed * 7919 + 13);
        for (int i = 0; i < n; i++) {
            snapshotJobs.add(new ArrayList<>());
        }
        this.n = n;
        this.seed = seed;
        this.config = config;
        this.random = new Random(seed);
        this.storages = new AuditStorage[n];
        this.nodes = new RaftNode[n];
        this.machines = new RecordingStateMachine[n];
        this.cut = new boolean[n][n];
        this.checkedCommit = new long[n];
        this.lastTerm = new long[n];
        this.lastCommitIndex = new long[n];
        this.leaderPrefix = new ArrayList<>();
        this.leaderPrefixTerm = new long[n];
        this.leaderPrefixBase = new long[n];
        for (int i = 0; i < n; i++) {
            storages[i] = new AuditStorage(new InMemoryRaftStorage());
            leaderPrefix.add(null);
            start(i);
        }
    }

    // ------------------------------------------------------------------ nodes

    private void start(int i) {
        List<Integer> peers = new ArrayList<>();
        for (int p = 0; p < n; p++) {
            if (p != i) {
                peers.add(p);
            }
        }
        RaftStorage opened = factory == null ? null : factory.openStorage(i);
        if (opened != null) {
            storages[i] = new AuditStorage(opened);
        }
        machines[i] = new RecordingStateMachine(factory == null ? null : factory.create(i));
        nodes[i] = new RaftNode(i, peers, storages[i], (to, message) -> send(i, to, message), machines[i], config,
                new Random(random.nextLong()));
        if (snapshotDelaySteps >= 0) {
            snapshotJobs.get(i).clear();
            nodes[i].setSnapshotWorker((work, done) -> snapshotJobs.get(i).add(
                    new SnapshotJob(now + snapshotRandom.nextInt(snapshotDelaySteps + 1), work, done)));
        }
        if (factory != null) {
            factory.started(i, nodes[i]);
        }
        checkedCommit[i] = nodes[i].commitIndex(); // a restart from a snapshot begins at its index
        lastCommitIndex[i] = nodes[i].commitIndex();
        leaderPrefix.set(i, null);
    }

    void crash(int i) {
        if (nodes[i] != null) {
            event("crash " + i);
            nodes[i] = null;
            snapshotJobs.get(i).clear(); // whatever a background snapshot had done so far is lost with the process
            if (factory != null) {
                factory.stopped(i);
            }
        }
    }

    void restart(int i) {
        if (nodes[i] == null) {
            event("restart " + i);
            start(i);
        }
    }

    boolean isUp(int i) {
        return nodes[i] != null;
    }

    RaftNode node(int i) {
        return nodes[i];
    }

    RaftStorage storage(int i) {
        return storages[i];
    }

    List<Applied> applied(int i) {
        return machines[i].applied;
    }

    int liveCount() {
        int c = 0;
        for (RaftNode node : nodes) {
            if (node != null) {
                c++;
            }
        }
        return c;
    }

    /** The live leader with the highest term, or -1. */
    int leader() {
        int best = -1;
        for (int i = 0; i < n; i++) {
            if (nodes[i] != null && nodes[i].role() == RaftNode.Role.LEADER
                    && (best == -1 || nodes[i].term() > nodes[best].term())) {
                best = i;
            }
        }
        return best;
    }

    List<Integer> leaders() {
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (nodes[i] != null && nodes[i].role() == RaftNode.Role.LEADER) {
                out.add(i);
            }
        }
        return out;
    }

    RaftNode.ProposeResult propose(int i, String command) {
        if (nodes[i] == null) {
            return new RaftNode.ProposeResult(false, 0, 0, -1);
        }
        RaftNode.ProposeResult r = nodes[i].propose(command.getBytes(StandardCharsets.UTF_8));
        observe(i);
        if (r.accepted()) {
            event("propose " + command + " at node " + i + " -> index " + r.index() + " term " + r.term());
        }
        return r;
    }

    // ------------------------------------------------------------------ network

    private void send(int from, int to, RaftMessage message) {
        messagesSent++;
        if (cut[from][to] || random.nextDouble() < dropRate) {
            return;
        }
        enqueue(from, to, message);
        if (random.nextDouble() < duplicateRate) {
            enqueue(from, to, message);
        }
    }

    private void enqueue(int from, int to, RaftMessage message) {
        int delay = minDelay + (maxDelay > minDelay ? random.nextInt(maxDelay - minDelay + 1) : 0);
        inFlight.add(new Envelope(now + delay, nextSeq++, from, to, message));
    }

    /** Cuts every link between the nodes in {@code group} and the others, in both directions. */
    void partition(Set<Integer> group) {
        event("partition " + group);
        for (int a = 0; a < n; a++) {
            for (int b = 0; b < n; b++) {
                if (group.contains(a) != group.contains(b)) {
                    cut[a][b] = true;
                }
            }
        }
    }

    void isolate(int node) {
        partition(Set.of(node));
    }

    void heal() {
        event("heal");
        for (boolean[] row : cut) {
            java.util.Arrays.fill(row, false);
        }
    }

    // ------------------------------------------------------------------ time

    void step() {
        now++;
        if (snapshotDelaySteps >= 0) {
            for (int i = 0; i < n; i++) {
                List<SnapshotJob> jobs = snapshotJobs.get(i);
                for (int j = 0; j < jobs.size() && nodes[i] != null; ) {
                    SnapshotJob job = jobs.get(j);
                    if (job.dueAt() > now) {
                        j++;
                        continue;
                    }
                    jobs.remove(j);
                    try {
                        job.done().accept(job.work().get());
                    } catch (RuntimeException ex) {
                        throw violation("node " + i + " threw " + ex);
                    }
                    observe(i);
                }
            }
        }
        while (!inFlight.isEmpty() && inFlight.peek().deliverAt() <= now) {
            Envelope e = inFlight.poll();
            if (nodes[e.to()] == null || cut[e.from()][e.to()]) {
                continue; // destination is down, or the link was cut while the message was in flight
            }
            messagesDelivered++;
            try {
                nodes[e.to()].receive(e.from(), e.message());
            } catch (RuntimeException ex) {
                throw violation("node " + e.to() + " threw " + ex);
            }
            observe(e.to());
        }
        for (int i = 0; i < n; i++) {
            if (nodes[i] != null) {
                nodes[i].tick();
                observe(i);
            }
        }
        checkInvariants();
    }

    void run(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
    }

    /** Steps until {@code condition} holds; returns false if it still does not after {@code maxSteps}. */
    boolean runUntil(java.util.function.BooleanSupplier condition, int maxSteps) {
        for (int i = 0; i < maxSteps; i++) {
            if (condition.getAsBoolean()) {
                return true;
            }
            step();
        }
        return condition.getAsBoolean();
    }

    // ------------------------------------------------------------------ invariants

    private void event(String text) {
        events.addLast("t=" + now + " " + text);
        if (events.size() > 60) {
            events.removeFirst();
        }
    }

    AssertionError violation(String what) {
        return new AssertionError("RAFT SAFETY VIOLATION: " + what + "\n  seed=" + seed + " n=" + n + " step=" + now
                + "\n  recent events:\n    " + String.join("\n    ", events));
    }

    /**
     * Checks one node right after it handled a message or a tick. Doing it per call (not per step) matters: a node
     * can commit an entry and be deposed a moment later in the same step, and the commit must be attributed to the
     * term it happened in.
     */
    private void observe(int i) {
        long term = storages[i].currentTerm();
        if (term < lastTerm[i]) {
            throw violation("node " + i + " term went from " + lastTerm[i] + " to " + term);
        }
        lastTerm[i] = term;
        RaftNode node = nodes[i];
        if (node == null) {
            return;
        }
        if (node.commitIndex() < lastCommitIndex[i]) {
            throw violation("node " + i + " commit index went backwards");
        }
        lastCommitIndex[i] = node.commitIndex();

        if (node.role() == RaftNode.Role.LEADER) {
            Integer previous = leaderOfTerm.putIfAbsent(node.term(), i);
            if (previous != null && previous != i) {
                throw violation("two leaders in term " + node.term() + ": nodes " + previous + " and " + i);
            }
            if (previous == null) {
                event("node " + i + " became leader of term " + node.term());
                checkLeaderCompleteness(i);
            }
        }

        // commit agreement: record what each node commits, and compare
        for (long idx = checkedCommit[i] + 1; idx <= node.commitIndex(); idx++) {
            LogEntry entry = storages[i].historic(idx);
            if (entry == null) {
                continue; // covered by a snapshot installed from a leader: this node never held the entry
            }
            LogEntry known = committed.get(idx);
            if (known == null) {
                committed.put(idx, entry);
                // Only a leader's own call can commit an entry for the first time, so this node's term is the
                // term the entry was committed in.
                if (node.role() != RaftNode.Role.LEADER) {
                    throw violation("entry " + idx + " first seen committed on non-leader " + i);
                }
                committedInTerm.put(idx, node.term());
            } else if (!known.equals(entry)) {
                throw violation("node " + i + " committed " + entry + " at index " + idx + " but " + known
                        + " was committed there before");
            }
        }
        checkedCommit[i] = Math.max(checkedCommit[i], node.commitIndex());

        // applied commands are committed ones, and every node applies the same command at an index
        RecordingStateMachine sm = machines[i];
        for (int a = sm.applied.size() - 1; a >= 0; a--) {
            Applied ap = sm.applied.get(a);
            String existing = appliedAt.putIfAbsent(ap.index(), ap.command());
            if (existing != null && !existing.equals(ap.command())) {
                throw violation("index " + ap.index() + " applied as '" + existing + "' and '" + ap.command() + "'");
            }
            if (existing != null) {
                break; // the rest of this node's list was checked earlier
            }
        }
    }

    /** A leader of term T must hold every entry that was committed in a term before T. */
    private void checkLeaderCompleteness(int i) {
        RaftNode node = nodes[i];
        for (Map.Entry<Long, LogEntry> e : committed.entrySet()) {
            long idx = e.getKey();
            LogEntry mine = idx <= storages[i].lastIndex() ? storages[i].historic(idx) : null;
            boolean unknownToMe = idx <= storages[i].lastIndex() && mine == null; // inside a snapshot it was sent
            if (committedInTerm.get(idx) < node.term() && !unknownToMe
                    && (idx > storages[i].lastIndex() || !mine.equals(e.getValue()))) {
                throw violation("leader " + i + " of term " + node.term() + " lacks entry " + idx + " "
                        + e.getValue() + " committed in term " + committedInTerm.get(idx));
            }
        }
    }

    private void checkInvariants() {
        for (int i = 0; i < n; i++) {
            observe(i);
        }
        if (now % 5 == 0) {
            for (int i = 0; i < n; i++) {
                if (nodes[i] != null && nodes[i].role() == RaftNode.Role.LEADER) {
                    checkLeaderCompleteness(i);
                }
            }
        }
        if (++stepsSinceDeepCheck >= 20) {
            stepsSinceDeepCheck = 0;
            deepCheck();
        }
    }

    /** The slower checks: log matching between every pair of logs, and leaders only appending. */
    void deepCheck() {
        for (int a = 0; a < n; a++) {
            for (int b = a + 1; b < n; b++) {
                long common = Math.min(storages[a].lastIndex(), storages[b].lastIndex());
                long matchAt = 0;
                for (long idx = common; idx >= 1; idx--) {
                    LogEntry ea = storages[a].historic(idx);
                    LogEntry eb = storages[b].historic(idx);
                    if (ea != null && eb != null && ea.term() == eb.term()) {
                        matchAt = idx;
                        break;
                    }
                }
                for (long idx = 1; idx <= matchAt; idx++) {
                    LogEntry ea = storages[a].historic(idx);
                    LogEntry eb = storages[b].historic(idx);
                    if (ea != null && eb != null && !ea.equals(eb)) {
                        throw violation("log matching broken between nodes " + a + " and " + b + " at index " + idx
                                + " (they agree at " + matchAt + ")");
                    }
                }
            }
        }
        for (int i = 0; i < n; i++) {
            RaftNode node = nodes[i];
            if (node == null || node.role() != RaftNode.Role.LEADER) {
                leaderPrefix.set(i, null);
                continue;
            }
            List<LogEntry> log = node.logCopy();
            long logBase = storages[i].snapshotIndex();
            List<LogEntry> before = leaderPrefix.get(i);
            if (before != null && leaderPrefixTerm[i] == node.term()) {
                long beforeLast = leaderPrefixBase[i] + before.size();
                if (logBase + log.size() < beforeLast) {
                    throw violation("leader " + i + " shortened its log within term " + node.term());
                }
                // the leader may have compacted its log meanwhile: compare the indexes both versions still hold
                for (long idx = Math.max(logBase, leaderPrefixBase[i]) + 1; idx <= beforeLast; idx++) {
                    if (!log.get((int) (idx - logBase - 1)).equals(before.get((int) (idx - leaderPrefixBase[i] - 1)))) {
                        throw violation("leader " + i + " changed its log within term " + node.term());
                    }
                }
            }
            leaderPrefix.set(i, log);
            leaderPrefixBase[i] = logBase;
            leaderPrefixTerm[i] = node.term();
        }
    }

    /** The highest term any node has reached. */
    long maxTermSeen() {
        long max = 0;
        for (RaftStorage st : storages) {
            max = Math.max(max, st.currentTerm());
        }
        return max;
    }

    /** Everything committed so far, by index (for tests). */
    Map<Long, LogEntry> committedEntries() {
        return committed;
    }

    /** A fingerprint of the whole run so far, to check that the same seed reproduces the same history. */
    long fingerprint() {
        long h = 1125899906842597L;
        for (int i = 0; i < n; i++) {
            h = 31 * h + storages[i].currentTerm();
            h = 31 * h + storages[i].lastIndex();
            for (long idx = storages[i].snapshotIndex() + 1; idx <= storages[i].lastIndex(); idx++) {
                h = 31 * h + storages[i].entry(idx).hashCode();
            }
        }
        h = 31 * h + messagesSent;
        h = 31 * h + messagesDelivered;
        return h;
    }
}
