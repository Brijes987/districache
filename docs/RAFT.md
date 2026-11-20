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
  others. Sending never blocks; a full queue drops the message, which Raft tolerates. The queue is limited by bytes (16 MiB per peer, checked before the message is encoded) as well as by count (4,096): it used to be limited by count alone, and a message can be several megabytes, so a leader with a dead follower queued one per heartbeat and per client write without a practical limit. Found through an `OutOfMemoryError` in `ChunkedSnapshotClusterTest` on a 4 GB laptop; `PeerTransportBackpressureTest` showed 120 MB queued for a dead peer before the change and at most about 16 MiB plus one message after. The same test needed about 500 MB of heap before and about 380 MB after (my machine; one run each, the rest is the 42 MB state held several times). Peers prove they know a shared
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

## Pre-vote (opt-in, `--pre-vote`)

**The problem.** In plain Raft a node that cannot hear the leader (its link is cut, or it was isolated and came back) times out, raises its term and starts an election. When it reconnects, its higher term makes the healthy leader step down. Pre-vote (Raft thesis section 9.6) adds a first round: before raising its term the node asks its peers "would you vote for me?" with the term it would use. Nothing changes on the receiver (term, vote, election timer). A peer says yes only if it has not heard from a live leader within the minimum election timeout, the proposed term is higher than its own, and the candidate's log is at least as up to date. Only after a majority says yes does the node start a real election.

**How to use it.** Start every server with `--pre-vote` (or `RaftConfig.withPreVote(true)`). It is off by default. It must be set the same on every node of a cluster: a node with it off refuses every pre-vote, so a mixed cluster could not elect a replacement leader if the off nodes were a majority. That mixed case is not supported and not tested.

**What was tested** (all pass in my Linux sandbox; `PreVoteTest` 34 tests, `PreVoteWireTest` 3, `PreVoteClusterTest` 1):
- Scripted single-node cases: a timeout sends pre-votes without raising the term; a majority of grants starts a real election; no majority never raises the term; stale-round grants are ignored; a refusal carrying a higher term is adopted; a voter with a live leader refuses and changes nothing; a stale log is refused; a far-ahead proposed term is not adopted.
- Simulator scenarios: with a full isolation or a one-way link failure (the node cannot hear the leader, the leader can hear it) the leader is deposed and the term rises without pre-vote; with pre-vote the leader stays and the term does not move. Liveness still holds after a leader crash (20 seeds, 5 nodes), a leader cut off, and a restart of all nodes. No pre-vote round is started under a healthy leader.
- Randomised chaos: 60 normal, 40 brutal and 20 without-check-quorum seeds, replay determinism, and 60 linearizability-checked runs with pre-vote on.
- Two hand-made breakages, each caught: ignoring the leader lease (caught in 10 of 10 one-way-cut runs) and adopting the proposed term on receipt (elections become impossible, caught as no leader within 2,000 steps in 10 of 10 seeds). This shows the tests can fail; it is not a proof the code is correct.
- Real sockets: a 3-node cluster with pre-vote elects a leader, serves 30 writes, survives killing the leader, serves 30 more and reads all 60 back (run 3 times, passed 3 of 3 in the sandbox).
- Two real bugs were found by these tests during development and fixed: a candidate whose election timed out kept its role and could never count grants (no leader after a crash), and a timed-out follower would have refused other nodes' pre-votes.

**Windows run.** On the 2-CPU Windows 10 laptop the full `mvn clean test` gave 660 tests, 0 failures, 0 errors (one run; this includes all 38 pre-vote tests, 5 min 28 s total).

**Docker, under write load (docs/DOCKER.md).** In one of two runs on a 2-CPU Docker VM, killing the leader left the two surviving servers without a leader for over 60 seconds with pre-vote on; the other run recovered after 2.9 s. I could not reproduce the failure in my Linux sandbox and do not know its cause, so pre-vote is not shown to be reliable under load in that setting.

**Not measured / not claimed.** Nothing was run on separate machines or under real network faults. The cost of the extra round (election delay) was not measured.

## Network partitions on real servers (`RealPartitionLinearizabilityTest`)

Five real servers over real TCP and real files; the partition is made by dropping the peer messages between the two
sides (the servers' client ports keep working, as when only the servers' network is split). Four clients run at once and
every call and answer is recorded with real timestamps, and the Wing-Gong checker decides whether the recorded history
could come from one correct key-value map. One client can only reach the server that was leader when the cut began, so it
is the one that would see a stale read or a wrongly acknowledged write from the cut-off leader.

| Test | Setting | What is cut |
|---|---|---|
| leader alone, read-index, no check-quorum | the old leader stays leader through the cut | the leader from the other four |
| same with pre-vote | | |
| log reads, check-quorum on, pre-vote on | | |
| leader plus one follower (2 of 5) | read-index, pre-vote | the pair from the other three |
| random cuts, 6 rounds, pre-vote off and on | read-index | 1 or 2 random servers each round |
| fault switched on: `READ_SKIP_LEADERSHIP_CHECK` | read-index, no check-quorum | the leader alone |
| one-way cut, 3 servers, pre-vote on and off | | a follower stops hearing the leader (its own messages still get out) |

What the runs showed (Linux sandbox; the class passed 6 times in a row after the test was fixed, then once more inside
the full suite of 569 tests). On the 2-CPU Windows laptop the full suite gave 669 tests, 0 failures, and the 8 partition tests took 50.8 s in one run, with the switched-on mistake caught there as well (the pinned client read `"c4-18"` after the other side had acknowledged newer writes):

- Every healthy-setting history was linearizable, and in each of those tests the majority side acknowledged at least 20
  writes during the run, so the cut was real.
- **The test can fail.** With the known mistake switched on, the cut-off leader answered reads from its own copy: the
  pinned client read `x` as absent while the other side had already acknowledged `"c2-4"` for it, and the checker
  reported NOT LINEARIZABLE.
- **One-way cut:** with pre-vote the leader kept its leadership and the term did not move while the follower could not
  hear it, and 20 writes still succeeded; without pre-vote the term rose.
- My first version of this test failed 5 of 8 runs because it wrote two starting values through a client whose writes were
  not put in the recorded history, so the checker saw a value nobody had written. That was a flaw in the test, not in the
  servers; the starting writes are now recorded.

Not claimed: partitions here are made inside the servers (messages dropped), not at the network layer; only 5-server
groups (3 for the one-way cut), fixed random seeds, short runs (a few seconds per scenario); no membership changes; the
Docker runs in docs/DOCKER.md are a separate and less clean result.

## What this does NOT claim

- Not formally proven or model-checked. The claim is: tested with randomized fault injection against stated invariants.
- Linearizability is checked on recorded histories from the simulator and from a local cluster, under the faults those tests inject. It is not a proof, and it has not been checked on separate machines or under real network faults other than the partitions and kills described above.
- TLS for the replicated mode is optional (`--tls-*` options, see SECURITY.md) and its cost is unmeasured. Without it, peer and client traffic is not encrypted.
- By default reads go through the log, so they are as costly as writes. `--read-mode read-index` avoids that (READ_INDEX.md); its gain is measured, and only clear with fsync on and few clients. There is no lease optimisation.
- Snapshots and log compaction exist (SNAPSHOTS.md); a snapshot is sent in chunks and limited to 128 MiB (SNAPSHOTS.md).
- The group is fixed; servers cannot be added or removed while it runs.

## Why not a monitor-based failover or data migration

The earlier roadmap listed failover and data migration. With Raft the group elects its own leader, so a separate
monitor process is unnecessary and was dropped. Data migration on membership change is not planned.
