# Group commit for the Raft log

## The problem
With fsync on, a replicated write costs one disk sync on the leader and one on each follower that must acknowledge it.
Before this change every client request was proposed alone: one log append, one `force()`, one broadcast. The node's
single thread did one request at a time, so extra clients queued behind the disk instead of sharing it. The benchmark
in the README (measured before this change) showed 207 durable SET/s with one client and only 354 with 32.

## What it does
Requests that are **already waiting** are written together.

1. Network threads still post each request to the node's task queue.
2. The node loop runs a queued task, then keeps taking tasks that are already in the queue (up to `--max-batch`,
   default 256) without waiting. In batching mode a client request does not propose anything: it joins
   `ReplicatedCache`'s queue.
3. `ReplicatedCache.flush()` then calls `RaftNode.proposeBatch(...)`: **one** `storage.append(list)` (one fsync), one
   broadcast, one commit check.
4. A follower that receives the batch in one AppendEntries (up to 64 entries per message) appends it with one fsync too.

There is no timer and no artificial delay. While the previous batch is being synced, new requests pile up, so the load
itself forms the batches. A single client therefore still pays one sync per write (nothing is waiting alongside it);
the gain is for concurrent clients.

## What did not change (the safety rules)
- An entry is in the leader's own durable log before any message carrying it is sent (append, then broadcast).
- No request is answered before its entry is committed (a majority holds it) and applied. Each request still has its own
  future, completed by `apply()` for its own log index.
- Exactly-once sessions are untouched: a retry that lands in the same batch as the original is executed once.
- Leadership lost between queueing and flushing gives NOT_LEADER (nothing written); lost after flushing gives UNKNOWN
  via the existing `poll()`, exactly as for single requests.
- Reads still go through the log, in order with the writes of the same batch.

## Flags
- `--max-batch N` on `ReplicatedServerMain` (default 256). `1` switches group commit off: every request is proposed
  and synced alone, the behaviour measured in the README table.
- `ClusterBenchmark --max-batch N` passes it to the three server processes.
- Metrics: `districache_raft_proposal_batches` and `districache_raft_proposed_entries` (mean batch = entries / batches).

## How it is tested
- `GroupCommitNodeTest` (9): one append and one AppendEntries per follower for a whole batch, consecutive indexes, no
  commit before a majority holds the entries, partial acknowledgement commits only a prefix, entries stored before any
  message carries them, batch equals one-by-one proposals, non-leader writes nothing, bad input writes nothing, a
  follower appends a received batch once, a single server commits inside the call.
- `GroupCommitCacheTest` (8): nothing answered before flush; reads see earlier writes of the same batch; nothing
  answered before commit in a group; NOT_LEADER for followers and for a leader demoted before flush; UNKNOWN for a leader
  cut off after flush; a duplicate (client, seq) in one batch runs once; shutdown fails queued requests.
- Chaos with batching on: replica agreement and exactly-once over 30 seeds, with torn-tail file recovery over 10, and a
  linearizability check of 120 faulty runs. The tests assert that batches of 2 or more really occurred.
- `GroupCommitClusterTest` (3): real processes' worth of sockets and real fsync, 24 concurrent clients, every
  acknowledged write readable afterwards, mean batch above 1; `--max-batch 1` leaves batching off; killing the leader
  under load loses nothing.
- Mutations that were caught: broadcasting before the append, acknowledging at flush time, wrong index mapping, dropping
  the last entry of a batch. One mutation was not caught because it is equivalent: removing the "demoted before flush"
  check in `flush()` still ends in NOT_LEADER, since `proposeBatch` itself refuses when not leader.

## Measuring
Run from the project root after `mvn clean package -DskipTests`:

```
java -cp target/classes com.distri.cache.node.ClusterBenchmark --seconds 5 --max-batch 1
java -cp target/classes com.distri.cache.node.ClusterBenchmark --seconds 5
```

The first is the old behaviour, the second is group commit. Same machine, same session, back to back.

## Results
Windows 10, JDK 23.0.1, 2 CPUs, 5 s per cell after a 1 s warm-up, three server processes and the clients on one
machine, one run of each setting back to back.

| fsync | op | clients | group commit OFF ops/s | ON ops/s | p50 ms off -> on | p99 ms off -> on |
|---|---|---:|---:|---:|---|---|
| on | SET | 1 | 203 | 201 | 4.65 -> 4.58 | 10.61 -> 14.39 |
| on | SET | 8 | 246 | 694 | 22.15 -> 7.92 | 311.51 -> 72.33 |
| on | SET | 32 | 310 | 3982 | 83.57 -> 7.10 | 459.34 -> 20.60 |
| on | GET | 1 | 251 | 241 | 3.92 -> 4.11 | 7.57 -> 5.64 |
| on | GET | 8 | 417 | 897 | 17.44 -> 7.62 | 42.15 -> 21.87 |
| on | GET | 32 | 411 | 3620 | 78.05 -> 7.70 | 156.20 -> 24.86 |
| off | SET | 1 | 1008 | 768 | 0.49 -> 0.55 | 4.64 -> 10.56 |
| off | SET | 8 | 2202 | 4373 | 2.75 -> 1.01 | 23.58 -> 9.19 |
| off | SET | 32 | 1857 | 8901 | 15.30 -> 3.27 | 103.47 -> 11.49 |
| off | GET | 1 | 2553 | 2297 | 0.33 -> 0.35 | 0.60 -> 0.65 |
| off | GET | 8 | 2650 | 7059 | 3.41 -> 0.94 | 9.30 -> 4.20 |
| off | GET | 32 | 2382 | 9813 | 14.40 -> 3.01 | 73.38 -> 5.88 |

What it shows:
- With fsync on and 32 clients, SET went from 310 to 3,982 ops/s (about 13x) and p50 from 83.6 ms to 7.1 ms. With 8 clients
  it is about 2.8x. GET, which also goes through the log, gained about 2.2x (8 clients) and 8.8x (32 clients).
- With one client there is no gain (203 vs 201 SET/s), as designed: nothing else is waiting to share the sync.
- With fsync off, 32 clients gained about 4.8x, so part of the gain is fewer messages and less per-request work, not only
  fewer disk syncs.
- Single-client fsync-off SET was lower with group commit (1,008 -> 768) and its p99 higher. I did not investigate this:
  it is one run, and the old-mode runs of the same setting have varied (the earlier README table shows 1,010). Treat it as
  possible small overhead for a lone client, not as a measured fact.
- Caveats: one run per setting on one machine with everything sharing 2 CPUs; the old-mode SET numbers differ somewhat
  from the earlier README table (207/336/354 against 203/246/310 here), which is the run-to-run spread to expect.

## Limits
- Gains depend on concurrent clients and on how slow the disk sync is; with one client there is no gain.
- The batch size is capped at 256 requests and, on the wire, 64 entries per AppendEntries.
- Only the leader's request path and the follower append path batch; there is no pipelining of the next batch while the
  previous one is still waiting for acknowledgements beyond what the existing heartbeat and reply flow already does.
