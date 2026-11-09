package com.distri.cache.raft;

/** What a committed command is applied to. Called in log order, once per committed non-empty entry. */
public interface StateMachine {
    void apply(long index, byte[] command);
}
