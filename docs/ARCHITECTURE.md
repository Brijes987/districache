# Architecture

How the pieces fit together. Diagrams are [Mermaid](https://mermaid.js.org/), which GitHub renders directly. Class
names are the real ones in `src/main/java/com/distri/cache/`. Details and invariants live in the linked docs.

## 1. The whole system

DistriCache has two server modes that share the same cache core. They are separate runtimes, not one cluster.

```mermaid
flowchart TB
    subgraph clients["Clients"]
        CC["CacheClient / ClusterClient<br/>ConsistentHashRing routing"]
        RC["ReplicatedClient<br/>session id + sequence number"]
        RCLI["Any Redis-protocol client"]
    end

    subgraph single["Mode 1: single server (package server)"]
        CS["CacheServer<br/>RESP protocol, optional TLS + password"]
        SH["ShardedLRUCache"]
        AOF["AppendOnlyLog<br/>persistence"]
        REPL["ReplicationHub / ReplicaLink<br/>async primary to replica"]
        CS --> SH
        CS --> AOF
        CS --> REPL
    end

    subgraph cluster["Mode 2: Raft cluster (packages node + raft)"]
        direction TB
        RS["ReplicatedServer x N<br/>client port + peer port + metrics port"]
        RN["RaftNode<br/>election, replication, commit,<br/>snapshots"]
        RCACHE["ReplicatedCache<br/>state machine + group commit"]
        RS --> RN --> RCACHE
    end

    CC -- RESP --> CS
    RCLI -- RESP --> CS
    RC -- "client protocol" --> RS

    PROM["Prometheus<br/>/metrics"]
    RS -. "NodeMetrics" .-> PROM
    CS -. "ServerMetrics" .-> PROM
```

Related: [REPLICATION.md](REPLICATION.md) (mode 1), [RAFT.md](RAFT.md) (mode 2), [PERSISTENCE.md](PERSISTENCE.md),
[METRICS.md](METRICS.md), [SECURITY.md](SECURITY.md).

## 2. Inside one Raft server

One owner thread (`raft-loop-<id>`) runs the `RaftNode`; network threads only hand it work through a queue. That is what
keeps Raft's state single-threaded.

```mermaid
flowchart LR
    CLI["Client connections<br/>(client port)"] --> Q
    PEERIN["Peer connections<br/>(peer port, challenge-response handshake)"] --> Q

    subgraph rt["NodeRuntime (single owner thread)"]
        Q["Task queue"] --> DRAIN["Drain up to maxBatch tasks<br/>then flush()"]
        DRAIN --> RCACHE["ReplicatedCache<br/>batching queue, pending futures,<br/>exactly-once session table"]
        RCACHE -- "proposeBatch(commands)" --> RN["RaftNode"]
        RN -- "apply committed entries" --> RCACHE
        RCACHE --> LRU["LRUCache<br/>values + per-entry TTL"]
    end

    RN -- "append / truncate / fsync" --> FS["FileRaftStorage"]
    FS --- LOG[("raft.log<br/>checksummed entries")]
    FS --- SNAP[("raft.snapshot<br/>cache + sessions")]
    RN -- "Transport" --> PT["PeerTransport<br/>RaftMessageCodec + Frames"]
    PT <--> OTHERS["Other Raft servers"]
```

- **Group commit** ([GROUP_COMMIT.md](GROUP_COMMIT.md)): one log append and one fsync per batch of already-queued
  requests. There is no artificial delay; load builds the next batch while the previous one is being fsynced.
- **Replies are sent only after the entry is committed and applied**, and the log is durable before any message that
  depends on it leaves the server.
- **Snapshots** ([SNAPSHOTS.md](SNAPSHOTS.md)): after applying the entry that crosses `--snapshot-every`, the state
  machine is serialised, `raft.snapshot` is written first, then `raft.log` is rewritten without the covered prefix.

## 3. Life of a write

```mermaid
sequenceDiagram
    participant C as ReplicatedClient
    participant L as Leader (ReplicatedServer)
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: SET k v (clientId, seq)
    Note over L: queued, then batched with other pending requests
    L->>L: append batch to raft.log + fsync
    par replicate
        L->>F1: AppendEntries(batch)
        L->>F2: AppendEntries(batch)
    end
    F1->>F1: append + fsync
    F1-->>L: success
    Note over L: majority has the entry, current term:<br/>commitIndex advances
    L->>L: apply to LRUCache (session table dedups retries)
    L-->>C: OK
    F2-->>L: success (later)
```

Reads take the same path (through the log), which is why they are linearizable and cost about as much as writes. A
retry with the same `(clientId, seq)` after a lost reply or a leader change returns the stored reply instead of
executing twice.

## 4. Catching up a lagging follower

```mermaid
flowchart TD
    A["Leader needs to send entries<br/>from index i to a follower"] --> B{"Is i still in the leader's log?"}
    B -- yes --> C["AppendEntries"]
    B -- "no: compacted into the snapshot" --> D["InstallSnapshot<br/>(chunks, one at a time, up to 128 MiB)"]
    D --> E{"Follower already past<br/>the snapshot?"}
    E -- yes --> F["Acknowledge, do not install"]
    E -- no --> G["restore state machine,<br/>then storage.installSnapshot"]
    G --> C
```

## 5. Where the evidence comes from

```mermaid
flowchart LR
    SIM["Deterministic simulator<br/>(drop, delay, duplicate, partition, crash)"] --> INV["Raft invariant checks"]
    SIM --> LIN["LinearizabilityChecker<br/>(Wing-Gong / Lowe)"]
    PROC["Real multi-process cluster<br/>kill and restart servers"] --> LIN
    MUT["Hand-made mutations and<br/>injected faults"] --> SIM
    BENCH["ClusterBenchmark<br/>RecoveryBenchmark"] --> NUM["Numbers in README / docs"]
```

See the Testing section of the [README](../README.md#testing).
