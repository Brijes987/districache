# Snapshots and log compaction (replicated mode)

## The problem
The Raft log only grows. Before this change every server kept every entry ever committed, in memory and in `raft.log`,
and replayed all of them on every restart. Disk use and restart time grew with the number of writes, even when the cache
itself stayed the same size (for example a million overwrites of ten thousand keys).

## What it does
- **Taking a snapshot.** After a server applies an entry, and at least `--snapshot-every N` entries (default 10,000) have
  been applied since its last snapshot, it saves the state machine as of that exact entry and drops the log entries the
  snapshot covers. Every server does this on its own; they need not agree on when.
- **What a snapshot holds.** The live cache entries from least to most recently used with their expiry times (so the same
  entry is evicted next), and the client sessions (last sequence number and reply per client), so a retry after a
  snapshot is still executed at most once.
- **Restart.** The server loads its snapshot, rebuilds the cache from it and continues with the log entries after it.
- **Catching up.** If a follower needs entries the leader has already dropped, the leader sends its snapshot in one
  `InstallSnapshot` message (retried at most every half election timeout, not at every heartbeat). The follower restores
  its state machine from it, stores it durably, and then continues with normal replication.

## Files
- `raft.snapshot`: `magic | index | term | length | state | CRC-32`, replaced by write-temp, force, atomic rename.
- `raft.log` after a compaction starts with a 16-byte header holding the index the log now starts after. A log without
  the header (written by an earlier version) starts at entry 1, so existing data directories still open.
- Order: the snapshot is made durable first, then the log is rewritten (temp, force, atomic rename). A crash between the
  two leaves a snapshot and a longer log; opening finishes the compaction. A corrupt snapshot, or a compacted log without
  its snapshot, makes the server refuse to start rather than guess.

## Safety rules it keeps
- Only applied (so committed) entries are ever compacted, and a snapshot is taken only after the entry it names was
  applied.
- A follower never installs a snapshot older than what it has already committed (that would roll its state back); it
  acknowledges it instead. A snapshot from an older term is refused.
- If the follower's own log holds the snapshot's last entry with the same term, the entries after it are kept; otherwise
  its whole log is dropped (its leftovers belong to a different history).
- AppendEntries that start before a follower's snapshot are trimmed to the part it does not have; elections compare
  logs by the snapshot's index and term when the log is empty after it.
- `restore` either completes or throws before changing anything, including at startup.

## Flags and metrics
- `--snapshot-every N` on `ReplicatedServerMain` (default 10,000; 0 = never, the log then grows without bound).
- Gauges: `districache_raft_snapshot_index`, `districache_raft_log_entries`, `districache_raft_snapshots_taken`,
  `districache_raft_snapshots_installed`, `districache_raft_snapshots_skipped` (see METRICS.md).

## How it is tested
- `SnapshotStorageTest` (15): file and in-memory storage behave the same; suffix kept or dropped by term; reopen after
  compaction; a torn tail after compaction; a crash between writing the snapshot and rewriting the log; a crash in the
  middle of installing a leader's snapshot; stray temp files; corrupt snapshot, missing snapshot and corrupt header are
  refused.
- `SnapshotNodeTest` (19): when snapshots are taken and what they contain; restart from a snapshot; leader sends a
  snapshot only when needed and not every heartbeat; follower installs, ignores stale or old-term snapshots, keeps a
  matching suffix, trims overlapping AppendEntries; a state machine that rejects a snapshot changes nothing; voting after
  compaction.
- `SnapshotCacheTest` (6): the snapshot round trip including sessions, LRU order and expiry; every truncation, trailing
  bytes and a wrong version are rejected without touching the cache; 2,000 random byte strings.
- Simulator chaos (the invariant checkers now understand compacted logs): Raft safety over 60 runs with snapshots every 8
  and every 1 entries; replica agreement and exactly-once over 40 runs; torn-tail file recovery over 16 runs; a
  linearizability check of 80 faulty runs. These assert that snapshots were really taken and really installed.
- `SnapshotClusterTest` (5): real servers and files: the log stays bounded; restart from snapshot plus tail; a server that
  was down while the log was compacted is caught up by a snapshot; the whole group restarts from its snapshots.
- Two deliberately broken versions (installing a stale snapshot; snapshotting before applying the last entry) are caught
  by the chaos tests, and 13 hand-made code mutations were each caught by at least one test.

## Measuring
```
java -cp target/classes com.distri.cache.node.RecoveryBenchmark
```
It runs, for several write counts, the same workload with snapshots off and on, then times a restart from the files
alone and reports bytes on disk. One server with real files, fsync off (neither number depends on fsync), 10,000
distinct keys overwritten over and over.

## Results
Windows 10, JDK 23.0.1, 2 CPUs; one server, real files, fsync off, 100-byte values, 10,000 distinct keys overwritten
repeatedly; one run per row.

| writes | snapshots | disk bytes | restart ms | log entries kept |
|---:|---|---:|---:|---:|
| 100,000 | off | 15,588,932 | 239 | 100,002 |
| 100,000 | every 10,000 | 1,229,139 | 27 | 2 |
| 400,000 | off | 62,355,632 | 812 | 400,002 |
| 400,000 | every 10,000 | 1,229,139 | 22 | 2 |
| 1,000,000 | off | 155,889,032 | 2,602 | 1,000,002 |
| 1,000,000 | every 10,000 | 1,229,139 | 140 | 2 |

- Without snapshots the files and the restart time grow with the number of writes: 156 MB and 2.6 s after a million
  writes. With snapshots every 10,000 entries the disk use stays at 1.2 MB (the size of the 10,000-key state) at every
  write count, about 127x smaller after a million writes.
- Restart was about 9x, 37x and 19x faster at 100k, 400k and 1M writes. The 1M restart (140 ms) is slower than the 400k
  one (22 ms) even though both load the same snapshot and a two-entry tail; that is one run in one JVM and I did not
  investigate it, so read the restart times as "tens to low hundreds of milliseconds against seconds", not as exact
  ratios.
- Restart here is the time to open the files and rebuild the cache in one process. It does not include starting the
  JVM or reconnecting to other servers, and a server that was far behind still needs a snapshot or log catch-up after it.
- The measurements say nothing about the pause while a snapshot is being taken; that is not measured.

## Limits
- A snapshot is one message, at most 32 MiB. A larger state is not snapshotted (the log keeps growing, counted in
  `snapshots_skipped`); chunked transfer is not implemented.
- A follower only slightly behind when a snapshot is taken is sent the whole snapshot rather than the few entries it
  misses (no trailing entries are kept).
- Taking a snapshot copies the whole state while the node's thread is busy with it, so very large caches will pause that
  server for the duration. This pause has not been measured.
- Snapshots are written for the replicated mode only; the single server has its own append-only-log compaction.
