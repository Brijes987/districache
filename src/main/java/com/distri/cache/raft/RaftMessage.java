package com.distri.cache.raft;

import java.util.List;

/** The four messages of the Raft protocol. Every message carries the sender's current term. */
public sealed interface RaftMessage
        permits RaftMessage.RequestVote, RaftMessage.RequestVoteReply, RaftMessage.AppendEntries,
        RaftMessage.AppendEntriesReply {

    long term();

    /** A candidate asks for a vote; {@code lastLogIndex/lastLogTerm} describe its log so voters can compare. */
    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) implements RaftMessage {}

    record RequestVoteReply(long term, boolean voteGranted) implements RaftMessage {}

    /**
     * The leader replicates entries (empty for a heartbeat). {@code prevLogIndex/prevLogTerm} identify the entry
     * just before {@code entries}, which the follower must already have.
     */
    record AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
            long leaderCommit) implements RaftMessage {}

    /**
     * On success {@code matchIndex} is the last index known to match the leader's log. On failure
     * {@code conflictIndex/conflictTerm} let the leader jump back instead of probing one entry at a time.
     */
    record AppendEntriesReply(long term, boolean success, long matchIndex, long conflictIndex, long conflictTerm)
            implements RaftMessage {}
}
