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
 * Consensus Algorithm" (Ongaro and Ousterhout), without log compaction and without membership changes.
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
 * snapshots, membership changes, and read-only queries that avoid the log.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** Result of {@link #propose}: either the position the entry was given, or who to ask instead. */
    public record ProposeResult(boolean accepted, long index, long term, int leaderHint) {}

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

    // ------------------------------------------------------------------ inputs

    /** One unit of time has passed. */
    public void tick() {
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
        if (command.length == 0) {
            throw new IllegalArgumentException("a command cannot be empty (empty is reserved for no-op entries)");
        }
        if (role != Role.LEADER) {
            return new ProposeResult(false, 0, currentTerm, leaderId);
        }
        storage.append(List.of(new LogEntry(currentTerm, command)));
        long index = storage.lastIndex();
        broadcastAppend();
        advanceCommit(); // a group of one commits at once
        return new ProposeResult(true, index, currentTerm, id);
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
        broadcastAppend();
        advanceCommit();
    }

    private void becomeFollower(long term, int leader) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = -1;
            storage.saveTermAndVote(currentTerm, votedFor);
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        resetElectionTimer();
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

        long lastIndex = storage.lastIndex();
        if (m.prevLogIndex() > lastIndex) {
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, lastIndex + 1, 0));
            return;
        }
        if (m.prevLogIndex() > 0 && storage.termAt(m.prevLogIndex()) != m.prevLogTerm()) {
            long conflictTerm = storage.termAt(m.prevLogIndex());
            long conflictIndex = m.prevLogIndex();
            while (conflictIndex > 1 && storage.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, false, 0, conflictIndex, conflictTerm));
            return;
        }

        // The entry before the new ones matches. Add the new ones, deleting only what truly conflicts.
        List<LogEntry> entries = m.entries();
        if (config.faults().contains(RaftConfig.Fault.TRUNCATE_ON_EVERY_APPEND)) {
            if (storage.lastIndex() > m.prevLogIndex()) {
                storage.truncateFrom(m.prevLogIndex() + 1);
            }
            storage.append(entries);
        } else {
            long index = m.prevLogIndex();
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
        long lastNew = m.prevLogIndex() + entries.size();
        long newCommit = config.faults().contains(RaftConfig.Fault.COMMIT_PAST_VERIFIED_ENTRIES)
                ? Math.min(m.leaderCommit(), storage.lastIndex())
                : Math.min(m.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        transport.send(from, new RaftMessage.AppendEntriesReply(currentTerm, true, lastNew, 0, 0));
    }

    // ------------------------------------------------------------------ replication: leader side

    private void broadcastAppend() {
        for (int peer : peers) {
            sendAppend(peer);
        }
    }

    private void sendAppend(int peer) {
        long next = nextIndex.get(peer);
        long prev = next - 1;
        long last = Math.min(storage.lastIndex(), next + config.maxEntriesPerMessage() - 1);
        List<LogEntry> entries = next <= last ? storage.slice(next, last) : List.of();
        transport.send(peer, new RaftMessage.AppendEntries(currentTerm, id, prev, storage.termAt(prev), entries, commitIndex));
    }

    private void onAppendEntriesReply(int from, RaftMessage.AppendEntriesReply m) {
        if (role != Role.LEADER || m.term() != currentTerm) {
            return; // stale, or this node is no longer leading
        }
        heardFrom.add(from);
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
            for (long i = storage.lastIndex(); i >= 1; i--) {
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
            if (!entry.isNoop()) {
                stateMachine.apply(lastApplied, entry.command());
            }
        }
    }

    /** For diagnostics and tests. */
    List<LogEntry> logCopy() {
        List<LogEntry> out = new ArrayList<>();
        for (long i = 1; i <= storage.lastIndex(); i++) {
            out.add(storage.entry(i));
        }
        return out;
    }
}
