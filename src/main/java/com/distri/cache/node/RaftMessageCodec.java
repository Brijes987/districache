package com.distri.cache.node;

import com.distri.cache.raft.LogEntry;
import com.distri.cache.raft.RaftMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Binary form of the five Raft messages. Decoding validates everything it reads: input is untrusted. */
final class RaftMessageCodec {

    private static final byte REQUEST_VOTE = 1;
    private static final byte REQUEST_VOTE_REPLY = 2;
    private static final byte APPEND_ENTRIES = 3;
    private static final byte APPEND_ENTRIES_REPLY = 4;
    private static final byte INSTALL_SNAPSHOT = 5;
    private static final int MAX_ENTRIES = 100_000;

    private RaftMessageCodec() {}

    static byte[] encode(RaftMessage message) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            if (message instanceof RaftMessage.RequestVote m) {
                out.writeByte(REQUEST_VOTE);
                out.writeLong(m.term());
                out.writeInt(m.candidateId());
                out.writeLong(m.lastLogIndex());
                out.writeLong(m.lastLogTerm());
            } else if (message instanceof RaftMessage.RequestVoteReply m) {
                out.writeByte(REQUEST_VOTE_REPLY);
                out.writeLong(m.term());
                out.writeBoolean(m.voteGranted());
            } else if (message instanceof RaftMessage.AppendEntries m) {
                out.writeByte(APPEND_ENTRIES);
                out.writeLong(m.term());
                out.writeInt(m.leaderId());
                out.writeLong(m.prevLogIndex());
                out.writeLong(m.prevLogTerm());
                out.writeLong(m.leaderCommit());
                out.writeInt(m.entries().size());
                for (LogEntry e : m.entries()) {
                    out.writeLong(e.term());
                    byte[] command = e.command();
                    out.writeInt(command.length);
                    out.write(command);
                }
            } else if (message instanceof RaftMessage.AppendEntriesReply m) {
                out.writeByte(APPEND_ENTRIES_REPLY);
                out.writeLong(m.term());
                out.writeBoolean(m.success());
                out.writeLong(m.matchIndex());
                out.writeLong(m.conflictIndex());
                out.writeLong(m.conflictTerm());
            } else if (message instanceof RaftMessage.InstallSnapshot m) {
                out.writeByte(INSTALL_SNAPSHOT);
                out.writeLong(m.term());
                out.writeInt(m.leaderId());
                out.writeLong(m.lastIncludedIndex());
                out.writeLong(m.lastIncludedTerm());
                out.writeInt(m.state().length);
                out.write(m.state());
            } else {
                throw new IllegalArgumentException("unknown message type " + message);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e); // cannot happen: writing to memory
        }
    }

    /** @throws IOException if the bytes are not a well-formed message */
    static RaftMessage decode(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        byte type = in.readByte();
        RaftMessage message;
        switch (type) {
            case REQUEST_VOTE -> message = new RaftMessage.RequestVote(positive(in.readLong()), in.readInt(),
                    nonNegative(in.readLong()), nonNegative(in.readLong()));
            case REQUEST_VOTE_REPLY -> message = new RaftMessage.RequestVoteReply(positive(in.readLong()),
                    in.readBoolean());
            case APPEND_ENTRIES -> {
                long term = positive(in.readLong());
                int leaderId = in.readInt();
                long prevIndex = nonNegative(in.readLong());
                long prevTerm = nonNegative(in.readLong());
                long commit = nonNegative(in.readLong());
                int count = in.readInt();
                if (count < 0 || count > MAX_ENTRIES) {
                    throw new IOException("bad entry count " + count);
                }
                List<LogEntry> entries = new ArrayList<>(Math.min(count, 1024));
                for (int i = 0; i < count; i++) {
                    long entryTerm = in.readLong();
                    if (entryTerm <= 0) {
                        throw new IOException("bad entry term " + entryTerm);
                    }
                    int length = in.readInt();
                    if (length < 0 || length > in.available()) {
                        throw new IOException("bad command length " + length);
                    }
                    byte[] command = new byte[length];
                    in.readFully(command);
                    entries.add(new LogEntry(entryTerm, command));
                }
                message = new RaftMessage.AppendEntries(term, leaderId, prevIndex, prevTerm, entries, commit);
            }
            case APPEND_ENTRIES_REPLY -> message = new RaftMessage.AppendEntriesReply(positive(in.readLong()),
                    in.readBoolean(), nonNegative(in.readLong()), nonNegative(in.readLong()),
                    nonNegative(in.readLong()));
            case INSTALL_SNAPSHOT -> {
                long term = positive(in.readLong());
                int leaderId = in.readInt();
                long index = positive(in.readLong());
                long snapshotTerm = positive(in.readLong());
                int length = in.readInt();
                if (length < 0 || length > in.available() || length > com.distri.cache.raft.RaftStorage.MAX_SNAPSHOT_BYTES) {
                    throw new IOException("bad snapshot length " + length);
                }
                byte[] state = new byte[length];
                in.readFully(state);
                message = new RaftMessage.InstallSnapshot(term, leaderId, index, snapshotTerm, state);
            }
            default -> throw new IOException("unknown message type " + type);
        }
        if (in.available() != 0) {
            throw new IOException("trailing bytes after a message");
        }
        return message;
    }

    private static long positive(long v) throws IOException {
        if (v <= 0) {
            throw new IOException("term must be positive, got " + v);
        }
        return v;
    }

    private static long nonNegative(long v) throws IOException {
        if (v < 0) {
            throw new IOException("negative index or term " + v);
        }
        return v;
    }
}
