package com.distri.cache.raft;

import java.util.List;

/** The messages of the Raft protocol (the five of the paper, plus the reply to a snapshot chunk). Every message carries the sender's current term. */
public sealed interface RaftMessage
        permits RaftMessage.RequestVote, RaftMessage.RequestVoteReply, RaftMessage.AppendEntries,
        RaftMessage.AppendEntriesReply, RaftMessage.InstallSnapshot, RaftMessage.SnapshotChunkReply {

    long term();

    /** A candidate asks for a vote; {@code lastLogIndex/lastLogTerm} describe its log so voters can compare. */
    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) implements RaftMessage {}

    record RequestVoteReply(long term, boolean voteGranted) implements RaftMessage {}

    /**
     * The leader replicates entries (empty for a heartbeat). {@code prevLogIndex/prevLogTerm} identify the entry
     * just before {@code entries}, which the follower must already have.
     *
     * <p>{@code readRound} is used only by read-index reads: the leader stamps every message with the number of the
     * latest read it has started, and the follower echoes it in its reply. A reply that carries round {@code r}
     * therefore proves the follower recognised this leader at a moment after read {@code r} began. 0 means no read.
     */
    record AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
            long leaderCommit, long readRound) implements RaftMessage {
        public AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                long leaderCommit) {
            this(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, 0);
        }
    }

    /**
     * On success {@code matchIndex} is the last index known to match the leader's log. On failure
     * {@code conflictIndex/conflictTerm} let the leader jump back instead of probing one entry at a time.
     * {@code readRound} echoes the round of the AppendEntries being answered (see there).
     */
    record AppendEntriesReply(long term, boolean success, long matchIndex, long conflictIndex, long conflictTerm,
            long readRound) implements RaftMessage {
        public AppendEntriesReply(long term, boolean success, long matchIndex, long conflictIndex, long conflictTerm) {
            this(term, success, matchIndex, conflictIndex, conflictTerm, 0);
        }
    }

    /**
     * One chunk of the leader's state as of log index {@code lastIncludedIndex} (whose entry had term
     * {@code lastIncludedTerm}), sent to a follower that is so far behind that the entries it needs are no longer in the
     * leader's log. {@code offset} is where {@code data} belongs in the whole state; {@code done} marks the last chunk.
     * The follower answers each chunk but the last with a {@link SnapshotChunkReply}, and the last with an
     * {@link AppendEntriesReply} whose {@code matchIndex} is {@code lastIncludedIndex}.
     */
    record InstallSnapshot(long term, int leaderId, long lastIncludedIndex, long lastIncludedTerm, long offset,
            byte[] data, boolean done) implements RaftMessage {

        /** The whole state in one chunk. */
        public InstallSnapshot(long term, int leaderId, long lastIncludedIndex, long lastIncludedTerm, byte[] state) {
            this(term, leaderId, lastIncludedIndex, lastIncludedTerm, 0, state, true);
        }

        /** The chunk's bytes (the whole state when {@code offset == 0 && done}). */
        public byte[] state() {
            return data;
        }
    }

    /**
     * A follower tells the leader how much of the snapshot {@code lastIncludedIndex} it holds, contiguously from the
     * start: the leader continues from {@code nextOffset}. A value lower than what the leader believes means the
     * follower lost or never got some bytes.
     */
    record SnapshotChunkReply(long term, long lastIncludedIndex, long nextOffset) implements RaftMessage {}
}
