package com.distri.cache.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * One server of a Raft group: leader election, log replication and commit, as in "In Search of an Understandable
 * Consensus Algorithm" (Ongaro and Ousterhout), including log compaction by snapshots (section 7) but without
 * membership changes.
 *
 * <p>The node is a plain state machine. It owns no threads, sockets or clocks: the caller supplies time as calls to
 * {@link #tick()} and incoming messages as calls to {@link #receive}, and gets outgoing messages through the
 * {@link Transport}. That makes it deterministic, which is what lets a simulator replay a failure exactly from a
 * random seed. <b>It is not thread-safe</b>; the caller must serialise all calls.
 *
 * <p>Durability: the term, the vote and the log live in the {@link RaftStorage}. They are written before any message
 * that depends on them is sent, so a node that crashes and is rebuilt on the same storage keeps its promises.
 * Everything else (role, commit index, what has been applied, leader state) is rebuilt after a restart.
 *
 * <p>Also included: a new leader appends a no-op entry so that entries of earlier terms become committed; a leader
 * that cannot hear from a majority for an election timeout steps down (check-quorum). Not included: pre-vote,
 * membership changes.
 * A snapshot travels in chunks (one in flight per follower, acknowledged one by one), so a large snapshot never
 * blocks the heartbeats queued behind it.
 *
 * <p>Read-index reads (Raft thesis section 6.4): {@link #beginRead} answers a read without writing to the log. The
 * leader notes its commit index (at least the index of its own first entry of the term), confirms with a round of
 * heartbeats that a majority still follows it, waits until it has applied up to the noted index, and only then calls
 * {@link ReadListener#ready}. No clocks or leases are involved.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** Result of {@link #propose}: either the position the entry was given, or who to ask instead. */
    public record ProposeResult(boolean accepted, long index, long term, int leaderHint) {}

    /** Result of {@link #beginRead}: accepted, or who to ask instead. */
    public record ReadStart(boolean accepted, int leaderHint) {}

    /** Told, on the node's own thread, when a read-index read may be answered or must be abandoned. */
    public interface ReadListener {
        /** Leadership was confirmed and everything the read must see is applied: answer from the state machine now. */
        void ready(Object token);

        /** Leadership was lost before the read could be confirmed; it was not answered and may be retried elsewhere. */
        void failed(Object token);
    }

    private record PendingRead(Object token, long round, long index) {}

    private final int id;
    private final List<Integer> peers;
    private final int majority;
    private final RaftStorage storage;
    private final Transport transport;
    private final StateMachine stateMachine;
    private final RaftConfig config;
    private final Random random;

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private int votedFor;
    private int leaderId = -1;
    private long commitIndex;
    private long lastApplied;

    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    private final Set<Integer> votesGranted = new HashSet<>();

    // leader state
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Set<Integer> heardFrom = new HashSet<>();
    private int quorumElapsed;
    private long ticks;
    /** The snapshot this leader is sending to a follower, and how far the follower has got. */
    private static final class SnapshotSend {
        final long index;
        final long term;
        final byte[] state;
        long offset;            // start of the next chunk to send: everything before it is acknowledged
        long followerOffset;    // what the follower's latest reply says it holds
        long sentAt = -1;       // tick of the last chunk sent, -1 for none

        SnapshotSend(RaftStorage.Snapshot snapshot) {
            this.index = snapshot.index();
            this.term = snapshot.term();
            this.state = snapshot.state();
        }
    }

    /** A snapshot this follower is assembling from chunks. Identified by who sends it and which snapshot it is. */
    private static final class IncomingSnapshot {
        final long term;
        final int leader;
        final long index;
        final long lastTerm;
        byte[] buffer = new byte[0];
        int received;

        IncomingSnapshot(long term, int leader, long index, long lastTerm) {
            this.term = term;
            this.leader = leader;
            this.index = index;
            this.lastTerm = lastTerm;
        }

        void append(byte[] data) {
            if (received + data.length > buffer.length) {
                long wanted = Math.max((long) received + data.length, Math.min(2L * buffer.length, RaftStorage.MAX_SNAPSHOT_BYTES));
                buffer = java.util.Arrays.copyOf(buffer, (int) wanted);
            }
            System.arraycopy(data, 0, buffer, received, data.length);
            received += data.length;
        }
    }

    private final Map<Integer, SnapshotSend> snapshotSends = new HashMap<>();
    /** What this node accepts as one snapshot. A field only so a test can lower it; production code never changes it. */
    int maxSnapshotBytes = RaftStorage.MAX_SNAPSHOT_BYTES;

    /** How many snapshots this leader is holding for transfers still in progress (each keeps its bytes in memory). */
    int snapshotTransfersInProgress() {
        return snapshotSends.size();
    }
    private IncomingSnapshot incoming;
    private long snapshotChunksSent;
    private long snapshotChunksReceived;
    private long snapshotsTaken;
    private long snapshotsInstalled;
    private long snapshotsSkipped;
    private ReadListener readListener;
    private long readRound;                              // number of the latest read started by this leader
    private long termStartIndex;                         // index of this leader's first entry (its no-op)
    private final Map<Integer, Long> ackedRound = new HashMap<>();
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private long readsServed;

    /**
     * @param peers the ids of the OTHER servers in the group (the group has {@code peers.size() + 1} servers)
     * @param random the source of election-timeout randomness (seed it to make runs repeatable)
     */
    public RaftNode(int id, List<Integer> peers, RaftStorage storage, Transport transport, StateMachine stateMachine,
            RaftConfig config, Random random) {
        if (peers.contains(id)) {
            throw new IllegalArgumentException("peers must not contain this node's own id");
        }
        this.id = id;
        this.peers = List.copyOf(peers);
        this.majority = (this.peers.size() + 1) / 2 + 1;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.config = config;
        this.random = random;
        this.currentTerm = storage.currentTerm();
        this.votedFor = storage.votedFor();
        RaftStorage.Snapshot snapshot = storage.snapshot();
        if (snapshot != null) {
            stateMachine.restore(snapshot.state()); // the log before this point is gone; the snapshot is the state
            commitIndex = snapshot.index();
            lastApplied = snapshot.index();
        }
        resetElectionTimer();
    }

    // ------------------------------------------------------------------ observers

    public int id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long term() {
        return currentTerm;
    }

    /** The server this node believes is the current leader, or -1. */
    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return storage.lastIndex();
    }

    /** Index of the last log entry covered by this node's snapshot, 0 if it has none. */
    public long snapshotIndex() {
        return storage.snapshotIndex();
    }

    /** Snapshots this node took itself since it started. */
    public long snapshotsTaken() {
        return snapshotsTaken;
    }

    /** Snapshots this node received from a leader and installed since it started. */
    public long snapshotsInstalled() {
        return snapshotsInstalled;
    }

    /** Read-index reads that were answered since this node started. */
    public long readsServed() {
        return readsServed;
    }

    /** Read-index reads waiting for confirmation or for the state machine to catch up. */
    public int pendingReads() {
        return pendingReads.size();
    }

    /** Snapshot chunks this node (as leader) has sent, retransmissions included. */
    public long snapshotChunksSent() {
        return snapshotChunksSent;
    }

    /** Snapshot chunks this node (as follower) has accepted as the next piece of a snapshot. */
    public long snapshotChunksReceived() {
        return snapshotChunksReceived;
    }

    /** Times a snapshot was due but the state was too large to snapshot. */
    public long snapshotsSkipped() {
        return snapshotsSkipped;
    }

    // ------------------------------------------------------------------ inputs

    /** One unit of time has passed. */
    public void tick() {
        ticks++;
        if (role == Role.LEADER) {
            heartbeatElapsed++;
            if (heartbeatElapsed >= config.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastAppend();
            }
            if (config.checkQuorum()) {
                quorumElapsed++;
                if (quorumElapsed >= config.electionTimeoutMinTicks()) {
                    boolean hearsMajority = heardFrom.size() + 1 >= majority;
                    heardFrom.clear();
                    quorumElapsed = 0;
                    if (!hearsMajority) {
                        becomeFollower(currentTerm, -1);
                    }
                }
            }
        } else {
            electionElapsed++;
            if (electionElapsed >= electionTimeout) {
                startElection();
            }
        }
    }

    /**
     * Asks to add {@code command} to the replicated log. Only a leader accepts. Acceptance means the entry is in
     * the leader's log, <b>not</b> that it is committed; it commits when a majority has it (see
     * {@link #commitIndex()}), and it can still be lost if this leader loses leadership first.
     */
    public ProposeResult propose(byte[] command) {
        return proposeBatch(List.of(command));
    }

    /**
     * Group commit: adds several commands to the log with <b>one</b> storage write (one fsync), one broadcast and one
     * commit check. The entries get consecutive indexes starting at {@link ProposeResult#index()}, the same safety
     * rules as {@link #propose} apply to each of them: they are durable on this node before any follower is told
     * about them, and none counts as committed until a majority holds it.
     *
     * @return {@code index} is the index of the FIRST command; the command at position {@code i} has index
     *     {@code index + i}. Not accepted (and nothing written) if this node is not the leader.
     */
    public ProposeResult proposeBatch(List<byte[]> commands) {
        if (commands.isEmpty()) {
            throw new IllegalArgumentException("a batch cannot be empty");
        }
        for (byte[] command : commands) {
            if (command.length == 0) {
                throw new IllegalArgumentException("a command cannot be empty (empty is reserved for no-op entries)");
            }
        }
        if (role != Role.LEADER) {
            return new ProposeResult(false, 0, currentTerm, leaderId);
        }
        List<LogEntry> entries = new ArrayList<>(commands.size());
        for (byte[] command : commands) {
            entries.add(new LogEntry(currentTerm, command));
        }
        long first = storage.lastIndex() + 1;
        storage.append(entries);
        broadcastAppend();
        advanceCommit(); // a group of one commits at once
        return new ProposeResult(true, first, currentTerm, id);
    }

    public void setReadListener(ReadListener listener) {
        this.readListener = listener;
    }

    /**
     * Starts a linearizable read that does not use the log. Only a leader accepts. {@code token} is handed back to the
     * {@link ReadListener}, possibly before this method returns (a one-server group has nothing to confirm).
     * The read is answered once (1) the leader's first entry of this term is committed, so its commit index is known
     * to be current, (2) a majority has acknowledged a heartbeat sent after this call, so no newer leader exists, and
     * (3) the state machine has applied everything up to the commit index noted now.
     */
    public ReadStart beginRead(Object token) {
        if (role != Role.LEADER) {
            return new ReadStart(false, leaderId);
        }
        if (readListener == null) {
            throw new IllegalStateException("no ReadListener set");
        }
        readRound++;
        long index = config.faults().contains(RaftConfig.Fault.READ_IGNORE_TERM_START)
                ? commitIndex : Math.max(commitIndex, termStartIndex);
        pendingReads.add(new PendingRead(token, readRound, index));
        broadcastAppend(); // stamped with the new round: the replies prove this leader was still followed after now
        serveReads();
        return new ReadStart(true, id);
    }

    /** Answers every pending read that is confirmed and caught up. Leader only. */
    private void serveReads() {
        if (role != Role.LEADER || pendingReads.isEmpty()) {
            return;
        }
        List<PendingRead> ready = new ArrayList<>();
        for (PendingRead read : pendingReads) {
            if (lastApplied < read.index()) {
                continue;
            }
            boolean confirmed;
            if (config.faults().contains(RaftConfig.Fault.READ_SKIP_LEADERSHIP_CHECK)) {
                confirmed = true;
            } else {
                int acks = 1;
                for (int peer : peers) {
                    long acked = ackedRound.getOrDefault(peer, -1L);
                    boolean counts = config.faults().contains(RaftConfig.Fault.READ_COUNT_OLD_ACKS)
                            ? acked >= 0 : acked >= read.round();
                    if (counts) {
                        acks++;
                    }
                }
                confirmed = acks >= majority;
            }
            if (confirmed) {
                ready.add(read);
            }
        }
        pendingReads.removeAll(ready);
        for (PendingRead read : ready) {
            readsServed++;
            readListener.ready(read.token());
        }
    }

    private void failPendingReads() {
        if (pendingReads.isEmpty()) {
            return;
        }
        List<PendingRead> failed = new ArrayList<>(pendingReads);
        pendingReads.clear();
        for (PendingRead read : failed) {
            readListener.failed(read.token());
        }
    }

    /** A message from server {@code from} arrived. */
    public void receive(int from, RaftMessage message) {
        if (message.term() > currentTerm) {
            // Someone is in a newer term: whatever this node was, it is now a follower in that term.
            becomeFollower(message.term(), -1);
        }
        if (message instanceof RaftMessage.RequestVote m) {
            onRequestVote(from, m);
        } else if (message instanceof RaftMessage.RequestVoteReply m) {
            onRequestVoteReply(from, m);
        } else if (message instanceof RaftMessage.AppendEntries m) {
            onAppendEntries(from, m);
        } else if (message instanceof RaftMessage.AppendEntriesReply m) {
            onAppendEntriesReply(from, m);
        } else if (message instanceof RaftMessage.InstallSnapshot m) {
            onInstallSnapshot(from, m);
        } else if (message instanceof RaftMessage.SnapshotChunkReply m) {
            onSnapshotChunkReply(from, m);
        }
    }

    // ------------------------------------------------------------------ elections

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        storage.saveTermAndVote(currentTerm, votedFor);
        leaderId = -1;
        votesGranted.clear();
        votesGranted.add(id);
        resetElectionTimer();
        if (votesGranted.size() >= majority) {
            becomeLeader();
            return;
        }
        long lastIndex = storage.lastIndex();
        RaftMessage.RequestVote request =
                new RaftMessage.RequestVote(currentTerm, id, lastIndex, storage.termAt(lastIndex));
        for (int peer : peers) {
            transport.send(peer, request);
        }
    }

    private void onRequestVote(int from, RaftMessage.RequestVote m) {
        boolean grant = false;
        if (m.term() == currentTerm && (votedFor == -1 || votedFor == m.candidateId())) {
            long myLastIndex = storage.lastIndex();
            long myLastTerm = storage.termAt(myLastIndex);
            boolean candidateUpToDate = m.lastLogTerm() > myLastTerm
                    || (m.lastLogTerm() == myLastTerm && m.lastLogIndex() >= myLastIndex);
            if (candidateUpToDate || config.faults().contains(RaftConfig.Fault.SKIP_LOG_UP_TO_DATE_CHECK)) {
                grant = true;
                votedFor = m.candidateId();
                storage.saveTermAndVote(currentTerm, votedFor); // durable before the vote leaves this node
                resetElectionTimer();
            }
        }
        transport.send(from, new RaftMessage.RequestVoteReply(currentTerm, grant));
    }

    private void onRequestVoteReply(int from, RaftMessage.RequestVoteReply m) {
        if (role != Role.CANDIDATE || m.term() != currentTerm || !m.voteGranted()) {
            return;
        }
        votesGranted.add(from);
        if (votesGranted.size() >= majority) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        heartbeatElapsed = 0;
        quorumElapsed = 0;
        heardFrom.clear();
        nextIndex.clear();
        matchIndex.clear();
        long next = storage.lastIndex() + 1;
        for (int peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        // Entries from earlier terms only become committed once an entry of THIS term is committed after them.
        storage.append(List.of(LogEntry.noop(currentTerm)));
        termStartIndex = storage.lastIndex();
        ackedRound.clear();
        snapshotSends.clear();
        broadcastAppend();
        advanceCommit();
    }

    private void becomeFollower(long term, int leader) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = -1;
            storage.saveTermAndVote(currentTerm, votedFor);
            incoming = null; // a half-received snapshot belongs to a leader of an older term
        }
        boolean wasLeader = role == Role.LEADER;
        role = Role.FOLLOWER;
        leaderId = leader;
        resetElectionTimer();
        if (wasLeader) {
            failPendingReads();
            snapshotSends.clear();
        }
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        int min = config.electionTimeoutMinTicks();
        int max = config.electionTimeoutMaxTicks();
        electionTimeout = min + (max > min ? random.nextInt(max - min + 1) : 0);
    }

    // ------------------------------------------------------------------ replication: follower side

    private void onAppendEntries(int from, RaftMessage.AppendEntries m) {
        if (m.term() < currentTerm) {
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, 0, 0));
            return;
        }
        if (role == Role.LEADER) {
            // Two leaders in one term would break Raft's central guarantee.
            throw new IllegalStateException("node " + id + " is leader of term " + currentTerm
                    + " and received AppendEntries from " + from + " for the same term");
        }
        role = Role.FOLLOWER; // a candidate that hears from a leader of its own term gives up
        leaderId = m.leaderId();
        electionElapsed = 0;

        long prevIndex = m.prevLogIndex();
        long prevTerm = m.prevLogTerm();
        List<LogEntry> entries = m.entries();
        long base = storage.snapshotIndex();
        if (prevIndex < base) {
            // The leader starts before this node's snapshot. Everything up to the snapshot is committed, so the leader
            // holds it too and it matches: skip those entries and continue from the snapshot point.
            long gap = base - prevIndex;
            entries = entries.size() >= gap ? entries.subList((int) gap, entries.size()) : List.of();
            prevIndex = base;
            prevTerm = storage.snapshotTerm();
        }
        long lastIndex = storage.lastIndex();
        if (prevIndex > lastIndex) {
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, lastIndex + 1, 0, m.readRound()));
            return;
        }
        if (prevIndex > 0 && storage.termAt(prevIndex) != prevTerm) {
            long conflictTerm = storage.termAt(prevIndex);
            long conflictIndex = prevIndex;
            while (conflictIndex > base + 1 && storage.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, conflictIndex, conflictTerm, m.readRound()));
            return;
        }

        // The entry before the new ones matches. Add the new ones, deleting only what truly conflicts.
        if (config.faults().contains(RaftConfig.Fault.TRUNCATE_ON_EVERY_APPEND)) {
            if (storage.lastIndex() > prevIndex) {
                storage.truncateFrom(prevIndex + 1);
            }
            storage.append(entries);
        } else {
            long index = prevIndex;
            for (int i = 0; i < entries.size(); i++) {
                index++;
                if (index <= storage.lastIndex()) {
                    if (storage.termAt(index) == entries.get(i).term()) {
                        continue; // already have this entry (a duplicate or reordered message)
                    }
                    storage.truncateFrom(index); // a real conflict: the leader's log wins
                }
                storage.append(entries.subList(i, entries.size()));
                break;
            }
        }
        long lastNew = prevIndex + entries.size();
        long newCommit = config.faults().contains(RaftConfig.Fault.COMMIT_PAST_VERIFIED_ENTRIES)
                ? Math.min(m.leaderCommit(), storage.lastIndex())
                : Math.min(m.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, true, lastNew, 0, 0, m.readRound()));
    }

    // ------------------------------------------------------------------ replication: leader side

    private void broadcastAppend() {
        for (int peer : peers) {
            sendAppend(peer);
        }
    }

    private void sendAppend(int peer) {
        long next = nextIndex.get(peer);
        if (next <= storage.snapshotIndex()) {
            sendSnapshot(peer); // the entries this follower needs were compacted away
            return;
        }
        long prev = next - 1;
        long last = Math.min(storage.lastIndex(), next + config.maxEntriesPerMessage() - 1);
        List<LogEntry> entries = next <= last ? storage.slice(next, last) : List.of();
        transport.send(peer, new RaftMessage.AppendEntries(currentTerm, id, prev, storage.termAt(prev), entries, commitIndex, readRound));
    }

    private void sendSnapshot(int peer) {
        SnapshotSend send = snapshotSends.get(peer);
        if (send == null || send.index != storage.snapshotIndex()) {
            send = new SnapshotSend(storage.snapshot()); // a new snapshot replaces one in progress: start over
            snapshotSends.put(peer, send);
        }
        if (send.sentAt >= 0 && ticks - send.sentAt < Math.max(1, config.electionTimeoutMinTicks() / 2)) {
            return; // a chunk is already on its way; do not flood a slow follower
        }
        if (send.sentAt >= 0) {
            send.offset = send.followerOffset; // no progress for a while: continue from what the follower says it holds
        }
        sendChunk(peer, send);
    }

    private void sendChunk(int peer, SnapshotSend send) {
        int total = send.state.length;
        int from = (int) send.offset;
        int to = (int) Math.min((long) total, (long) from + config.snapshotChunkBytes());
        send.sentAt = ticks;
        snapshotChunksSent++;
        transport.send(peer, new RaftMessage.InstallSnapshot(currentTerm, id, send.index, send.term, from,
                java.util.Arrays.copyOfRange(send.state, from, to), to == total));
    }

    private void onSnapshotChunkReply(int from, RaftMessage.SnapshotChunkReply m) {
        if (role != Role.LEADER || m.term() != currentTerm) {
            return;
        }
        SnapshotSend send = snapshotSends.get(from);
        if (send == null || send.index != m.lastIncludedIndex() || m.nextOffset() >= send.state.length) {
            return; // not the transfer in progress (a late reply), or nonsense: a reply to a non-final chunk is below the total
        }
        heardFrom.add(from);
        send.followerOffset = m.nextOffset();
        if (m.nextOffset() > send.offset) {
            send.offset = m.nextOffset(); // progress: send the next chunk now
            sendChunk(from, send);
        }
        // a reply that is not ahead of what is already acknowledged is a duplicate, a late reply or a request to go back:
        // the timer in sendSnapshot resends from what the follower says it holds
    }

    private void onInstallSnapshot(int from, RaftMessage.InstallSnapshot m) {
        if (m.term() < currentTerm) {
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, 0, 0));
            return;
        }
        if (role == Role.LEADER) {
            throw new IllegalStateException("node " + id + " is leader of term " + currentTerm
                    + " and received InstallSnapshot from " + from + " for the same term");
        }
        role = Role.FOLLOWER;
        leaderId = m.leaderId();
        electionElapsed = 0;

        long index = m.lastIncludedIndex();
        if (index <= commitIndex && !config.faults().contains(RaftConfig.Fault.INSTALL_STALE_SNAPSHOT)) {
            // Everything the snapshot covers is already committed here, so it matches the leader's log. Installing it
            // would roll the state machine back.
            incoming = null;
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, true, index, 0, 0));
            return;
        }
        boolean ignoreOffset = config.faults().contains(RaftConfig.Fault.SNAPSHOT_CHUNK_IGNORE_OFFSET);
        boolean sameTransfer = incoming != null && incoming.term == m.term() && incoming.leader == m.leaderId()
                && incoming.index == index && incoming.lastTerm == m.lastIncludedTerm();
        if (!sameTransfer) {
            // Chunks of snapshots taken by different servers can differ byte for byte even at the same log index, so
            // pieces of two transfers must never be mixed: a new transfer has to start at the beginning.
            if (m.offset() != 0 && !ignoreOffset) {
                transport.send(from, new RaftMessage.SnapshotChunkReply(currentTerm, index, 0));
                return;
            }
            incoming = new IncomingSnapshot(m.term(), m.leaderId(), index, m.lastIncludedTerm());
        }
        if (!ignoreOffset && m.offset() != incoming.received) {
            // a duplicate (offset below what we hold) or a gap (above it): say where the next byte belongs
            transport.send(from, new RaftMessage.SnapshotChunkReply(currentTerm, index, incoming.received));
            return;
        }
        if ((long) incoming.received + m.data().length > maxSnapshotBytes) {
            incoming = null; // a snapshot larger than this node accepts: refuse it, the leader will not make progress
            return;
        }
        incoming.append(m.data());
        snapshotChunksReceived++;
        if (!m.done()) {
            transport.send(from, new RaftMessage.SnapshotChunkReply(currentTerm, index, incoming.received));
            return;
        }
        byte[] state = java.util.Arrays.copyOf(incoming.buffer, incoming.received);
        long snapshotTerm = incoming.lastTerm;
        incoming = null;
        stateMachine.restore(state);     // throws before changing anything if the state is unusable
        storage.installSnapshot(index, snapshotTerm, state); // durable before we acknowledge
        commitIndex = index;
        lastApplied = index;
        snapshotsInstalled++;
        transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, true, index, 0, 0));
    }

    private void onAppendEntriesReply(int from, RaftMessage.AppendEntriesReply m) {
        if (role != Role.LEADER || m.term() != currentTerm) {
            return; // stale, or this node is no longer leading
        }
        ackedRound.merge(from, m.readRound(), Math::max); // max(): a delayed reply never lowers what is proven
        handleAppendReply(from, m);
        serveReads();
    }

    private void handleAppendReply(int from, RaftMessage.AppendEntriesReply m) {
        heardFrom.add(from);
        SnapshotSend transfer = snapshotSends.get(from);
        if (transfer != null && m.success() && m.matchIndex() >= transfer.index) {
            snapshotSends.remove(from); // the follower holds the snapshot: done
        }
        if (m.success()) {
            // max(): a delayed or duplicated reply must never move a peer's progress backwards
            long before = matchIndex.get(from);
            matchIndex.merge(from, m.matchIndex(), Math::max);
            long match = matchIndex.get(from);
            if (nextIndex.get(from) < match + 1) {
                nextIndex.put(from, match + 1);
            }
            advanceCommit();
            // Keep a lagging follower catching up without waiting for the next heartbeat, but only when this reply
            // made progress. Answering every (possibly duplicated) reply with another message would let duplicates
            // multiply; the heartbeat retries anything else.
            if (match > before && nextIndex.get(from) <= storage.lastIndex()) {
                sendAppend(from);
            }
            return;
        }
        long next = m.conflictIndex();
        if (m.conflictTerm() > 0) {
            // If this leader has entries of the follower's conflicting term, resume right after the last of them.
            long lastOfTerm = 0;
            for (long i = storage.lastIndex(); i > storage.snapshotIndex(); i--) {
                long t = storage.termAt(i);
                if (t == m.conflictTerm()) {
                    lastOfTerm = i;
                    break;
                }
                if (t < m.conflictTerm()) {
                    break;
                }
            }
            if (lastOfTerm > 0) {
                next = lastOfTerm + 1;
            }
        }
        next = Math.max(next, matchIndex.get(from) + 1);
        if (next < nextIndex.get(from)) {
            nextIndex.put(from, next);
            sendAppend(from); // only when the probe point moved back; a duplicate or stale failure changes nothing
        }
    }

    private void advanceCommit() {
        for (long n = storage.lastIndex(); n > commitIndex; n--) {
            if (storage.termAt(n) != currentTerm
                    && !config.faults().contains(RaftConfig.Fault.COMMIT_OLD_TERMS_BY_COUNTING)) {
                break; // only entries of the current term are committed by counting replicas (Raft section 5.4.2)
            }
            int replicas = 1;
            for (int peer : peers) {
                if (matchIndex.get(peer) >= n) {
                    replicas++;
                }
            }
            if (replicas >= majority) {
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = storage.entry(lastApplied);
            boolean snapshotDue = config.snapshotEvery() > 0
                    && lastApplied - storage.snapshotIndex() >= config.snapshotEvery();
            byte[] early = snapshotDue && config.faults().contains(RaftConfig.Fault.SNAPSHOT_BEFORE_APPLY)
                    ? snapshotState() : null;
            if (!entry.isNoop()) {
                stateMachine.apply(lastApplied, entry.command());
            }
            if (snapshotDue) {
                takeSnapshot(early);
            }
        }
        serveReads();
    }

    private byte[] snapshotState() {
        byte[] state = stateMachine.snapshot();
        return state.length > RaftStorage.MAX_SNAPSHOT_BYTES ? null : state;
    }

    /** Records the state as of {@code lastApplied} and drops the log entries it covers. */
    private void takeSnapshot(byte[] precomputed) {
        byte[] state = precomputed != null ? precomputed : snapshotState();
        if (state == null) {
            snapshotsSkipped++; // too large to send in one message: keep the log instead
            return;
        }
        storage.installSnapshot(lastApplied, storage.termAt(lastApplied), state);
        snapshotsTaken++;
    }

    /** For diagnostics and tests. */
    List<LogEntry> logCopy() {
        List<LogEntry> out = new ArrayList<>();
        for (long i = storage.snapshotIndex() + 1; i <= storage.lastIndex(); i++) {
            out.add(storage.entry(i));
        }
        return out;
    }
}
