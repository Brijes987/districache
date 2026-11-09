# Replicated mode: Raft, durable storage, linearizability checking and a real TCP cluster (Phases 6 to 9)

DistriCache has a second, separate mode in which several servers keep one cache consistent using the Raft consensus
algorithm. It is built in layers, each tested before the next: the algorithm (Phase 6), durable storage and the cache as a
replicated state machine (Phase 7), a linearizability checker (Phase 8), and a real TCP cluster with a client that follows
leader changes (Phase 9). The original single-server and primary/replica modes described in the other documents are
unchanged and do not use any of this.

The replicated mode talks its own small framed binary protocol, not the Redis protocol, because a request must carry the
client id and sequence number that make retries safe.

## What is implemented

Package `com.distri.cache.raft`:

- Leader election with randomized timeouts, one vote per term, votes persisted before the reply leaves the node.
- Log replication with the `prevLogIndex` / `prevLogTerm` consistency check, conflict hints so a leader can skip back
  over a divergent suffix, and truncation only on a real conflict (a delayed shorter message never deletes matching entries).
- Commit rule: a leader counts replicas only for entries of its **own term** (Raft paper section 5.4.2); older entries
  commit indirectly. A new leader appends a no-op of its term so earlier entries can commit promptly.
- Followers limit `commitIndex` to the entries verified against the current leader.
- Check-quorum: a leader that has not heard from a majority for an election timeout steps down.
- Deterministic design: the node has no threads, clocks or sockets. It is driven by `tick()` and `receive(from, msg)` and
  talks through a `Transport` and a `RaftStorage` interface, so the same code can later run over TCP and files.

## How it is tested

1. **Scripted node tests** (`RaftNodeTest`): exact messages fed to one node, including the Figure 8 situation and the
   follower commit limit.
2. **Seeded chaos simulation** (`SimCluster`, `Chaos`, `RaftChaosTest`): 3 or 5 nodes on a simulated network that drops,
   duplicates, delays and reorders messages, with partitions and crashes/restarts that keep stable storage. Invariants
   are checked after every message and tick: election safety, leader append-only, log matching, leader completeness,
   state machine safety (no two nodes apply different commands at one index), and monotonic commit. After the faults
   stop, the cluster must elect a leader and commit new commands (liveness). Every run is determined by its seed; a
   failure prints the seed and recent events, and a test checks that the same seed reproduces the same history.
3. **Tests of the tests** (`RaftBugInjectionTest`, `RaftNodeTest`): four known Raft mistakes can be switched on
   (`RaftConfig.Fault`, testing only). Measured detection:

| Mistake | How it is caught |
|---|---|
| Vote without the log up-to-date check | chaos: 393 of 400 seeds (normal profile) |
| Truncate the log on every AppendEntries | chaos: 280 of 400 seeds (normal profile) |
| Commit an old-term entry by counting replicas | scripted node test only; 0 of 800 chaos seeds |
| Follower commits past the verified prefix | scripted node test only; 0 of 800 chaos seeds |

The last two are honest gaps in random testing: they need a precise interleaving that the chaos run does not reach, which
is why they have scripted tests instead.

A bug found by this harness while building it: a leader that answered every (possibly duplicated) success reply with
another append let duplicates multiply and exhausted memory under duplication faults. It now resends only when the
reply made progress.

## Durable storage (`FileRaftStorage`)

Term and vote live in `raft.meta` (replaced by write-temp, force, atomic rename); the log lives in `raft.log` as
`length | CRC-32 | term | command` records, forced to disk before each call returns.

On open, a record cut off by a crash, a last record that fails its checksum, or a zero-filled tail is dropped (the file
is truncated and the repair written back). A bad record that is **followed by more data** cannot be a torn write, so
opening fails rather than silently discarding the rest of the log. Unlike the earlier append-only file for the cache,
records carry checksums, so corruption that still parses is detected.

Tested: the log file cut at every byte offset (always a whole prefix, always still appendable), damaged last vs. middle
record, zero-filled tail, corrupt or stale meta file, a random-operation comparison against the in-memory storage across
reopens, and a vote that must survive a restart. In the simulator, servers run on real files and each crash may leave a
half-written record behind.

Limits: an I/O error throws and the node must stop; the log is held in memory and never compacted; whether `force` really
reaches the platters depends on the disk. Tests use `fsync=false` to stay fast, so **power-loss durability is not
tested**, only the recovery logic.

## The cache as a replicated state machine (`ReplicatedCache`)

- Writes **and reads** go through the log, are applied in log order and answered after commit. Reads therefore need no
  lease or clock reasoning, at the cost of a log round trip per GET.
- A follower answers `NOT_LEADER` with a hint; a deposed leader answers `UNKNOWN` (the request may or may not have
  been executed).
- **Retries are safe.** Each client numbers its requests; the state machine remembers each client's last sequence number
  and result, returns the stored result for a repeat, and refuses an older number (`STALE`). So a client that gets no
  answer retries the same number anywhere and the operation takes effect at most once. One request in flight per client
  is assumed, and the session table is bounded, so evicting a client's session could let a very late retry apply twice.
- A SET carries an absolute expiry set by the leader's clock. Log order does not depend on clocks, but servers with
  skewed clocks can disagree about whether a TTL'd key is still visible.
- After a restart a node starts with an empty cache and re-applies its whole persisted log.

Tested (13 scenario tests, 3 chaos tests): redirect, exactly-once retry (including a retry whose first attempt had
already committed), a deposed leader's `UNKNOWN`, acknowledged writes surviving a leader crash, a restarted server
rebuilding its data, bounded sessions, TTL. Chaos: 4 clients retrying through crashes, partitions, loss and duplication;
in the 30 default seeds that is about 18,800 acknowledged requests, about 79,000 retries and about 740 duplicate requests
inside the logs, and afterwards every server holds identical data and no request was executed twice on any server.
Disabling the deduplication makes this test fail ("request 1:11 was executed 2 times").

This check does **not** verify that the answers clients received were correct. The linearizability checker (below) does.

## Linearizability checking (`com.distri.cache.linearizability`)

**What it decides.** Given a recorded history of SET / GET / DEL operations (when each started, when its answer arrived,
what the answer was), is there one order of all the operations that respects real time (if A's answer arrived before B
started, A comes first) and in which every answer is what a plain key-value map would have given, executing one
operation at a time? If yes the history is *linearizable*.

**How.** Keys are independent in this model, so each key is checked alone. For one key the checker searches depth-first
for such an order (the Wing and Gong search, with Lowe's memoisation of already-explored states); an operation without an
answer may have taken effect at any time after it started, or never. A state budget makes it answer `GAVE_UP` rather than
guess. A violation report names the key, how far the search got, and the operations that could not be placed.

**Tests of the checker.** Hand-made histories (stale read, read of a never-written value, flipping values, real-time order
between clients, delete results, pending operations); 240 generated histories that are linearizable by construction and
must be accepted; 60 histories with one corrupted read that must always be rejected; a 2,000-operation history decided in
well under a second. Three deliberately broken versions of the checker (ignoring real-time order, accepting any read,
ignoring delete results) each fail between 1 and 11 of those tests.

**Applied to the replicated cache.** The chaos clients record every operation. Measured on this code:

| Servers | Result |
|---|---|
| Correct | linearizable in all 126 fault-injected runs (about 350 to 650 operations each; never `GAVE_UP`) |
| Bug: a leader answers GET from its own copy, not through the log | caught in 28 of 60 runs (13 to 43 per 100 across fault profiles) |
| Bug: a leader answers SET/DEL before the entry is applied | caught in 30 of 30 runs |

The stale-read bug is the realistic one, and it is **not** caught in every run: it needs a client to ask a deposed or
not-yet-caught-up leader at the wrong moment. That is why the correct system is judged over many seeds, not one.

**What a pass means, and does not.** No violation was found in the histories that were recorded, under these faults.
It is not a proof. Times are simulated ticks and are conservative (an operation's interval is never shorter than
reality), so the checker can miss a violation that hinges on a tight interval, but it does not invent one. Operations are
single-key SET / GET / DEL without TTLs; there are no multi-key operations or transactions. In the chaos runs every
operation eventually got an answer (clients retry until they do), so histories contain no pending operations; the
checker supports them and its own tests cover them.

## A real cluster over TCP (`com.distri.cache.node`)

- `ReplicatedServer`: one group member. It keeps its Raft state in files, talks to the other members over TCP and serves
  clients on a second port. One thread owns the Raft node; network threads only post work to it. If that thread hits an
  unexpected error (a broken Raft invariant, a disk error) the server **stops itself** instead of carrying on with
  possibly inconsistent state.
- Server to server: one outgoing connection per peer, written by its own thread, so a slow or dead peer cannot delay the
  others. Sending never blocks; a full queue drops the message, which Raft tolerates. Peers prove they know a shared
  secret with an HMAC challenge-response. Without a secret anyone who can reach the peer port can inject Raft messages.
- `ReplicatedClient`: finds the leader, follows `NOT_LEADER` redirects, and retries with the **same** request number
  after a lost reply, a crashed leader or a timeout, so each operation takes effect at most once. If its deadline passes
  without an answer, the exception says the outcome is unknown. One request in flight per client.
- Group membership is fixed at start. There is no joining or leaving while it runs.

**Tests over real sockets and real files** (three servers in one JVM): election and basic operations, TTL, redirect with the
leader's address, a repeated request number answered from memory (including a later retry that must not delete another
client's newer value), killing the leader while a client keeps writing, restarting a killed server from its files, stopping
and restarting the whole group with real fsync, no majority means no acknowledgement (reads included), secrets required and
wrong ones refused, garbage on both ports, oversized keys.

**Linearizability on the real network.** Three clients run concurrently while servers are killed and restarted and the
leader is repeatedly cut off from its peers (a test hook drops its peer traffic while its client port keeps working). Every
call and answer is recorded with real timestamps (about 55,000 to 75,000 operations per run) and checked. The correct
servers pass, with and without check-quorum. When a deliberately broken server answers reads from its own copy, the run
without check-quorum is caught in 3 of 3 attempts. The run **with** check-quorum did not catch that bug in my attempts:
check-quorum makes a cut-off leader step down within about one election timeout, which nearly closes the window in
real time. The simulator, with its message delays, does catch it with check-quorum on.

## Running it

All commands assume the project is built (`mvn compile`) and you are in the project folder; on Windows use
`target\classes` where this says `target/classes`.

- **Kill-the-leader demo** (three real server processes, real disks): `java -cp target/classes com.distri.cache.node.ClusterDemo`
  finds the leader, kills it with SIGKILL while a writer keeps going, reports how long the writer stalled, reads every
  acknowledged key back, and checks the recorded history for linearizability.
- **Benchmark**: `java -cp target/classes com.distri.cache.node.ClusterBenchmark` (add `--seconds N`). Three server
  processes, clients and disk share one machine over loopback, so it shows relative costs (durable or not, one client or
  many), not production capacity. Every read goes through the log, so a read costs about what a write costs.
- **One server by hand**: `java -cp target/classes com.distri.cache.node.ReplicatedServerMain --id 0 --members
  0=127.0.0.1:7000:7100,1=127.0.0.1:7001:7101,2=127.0.0.1:7002:7102 --data data0 --peer-secret-file peer.secret
  --client-secret-file client.secret`, with the same `--members` list on every server. Secrets are read from files, not
  the command line.

## What this does NOT claim

- Not formally proven or model-checked. The claim is: tested with randomized fault injection against stated invariants.
- Linearizability is checked on recorded histories from the simulator and from a local cluster, under the faults those tests inject. It is not a proof, and it has not been checked on separate machines or under real network faults other than the partitions and kills described above.
- No encryption of peer or client traffic, and no TLS for the replicated mode (the existing single-server mode has TLS). Use a trusted network or a tunnel.
- Reads go through the log, so they are as costly as writes; there is no read-index or lease optimisation.
- No snapshots or log compaction: disk use and restart time grow with the number of writes.
- The group is fixed; servers cannot be added or removed while it runs.

## Why not a monitor-based failover or data migration

The earlier roadmap listed failover and data migration. With Raft the group elects its own leader, so a separate
monitor process is unnecessary and was dropped. Data migration on membership change is not planned.
