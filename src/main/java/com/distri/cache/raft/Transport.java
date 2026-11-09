package com.distri.cache.raft;

/** Sends a message to another node. May drop, delay, duplicate or reorder it; Raft is correct under all four. */
public interface Transport {
    void send(int to, RaftMessage message);
}
