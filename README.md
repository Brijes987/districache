# DistriCache

[![CI](https://github.com/Brijes987/districache/actions/workflows/ci.yml/badge.svg)](https://github.com/Brijes987/districache/actions/workflows/ci.yml)

A distributed in-memory cache in Java 17, built from first principles with no runtime dependencies. It started as a
thread-safe LRU cache with per-entry TTL (a hash map plus a **hand-written doubly-linked list**, guarded by a
`ReentrantReadWriteLock`) and grew in layers:

1. **A single server** that speaks a subset of the Redis protocol, with persistence, password and TLS, and
   asynchronous primary-replica replication.
2. **A replicated cluster** built on **Raft** (written from scratch): three or more server processes elect a leader, replicate
   writes through a durable log, survive the leader being killed, and give clients exactly-once retries.
3. **Evidence for the claims**: a deterministic fault-injecting simulator, property-based tests, a linearizability
   checker run on recorded client histories (in the simulator and against real killed processes), and a Prometheus
   metrics endpoint.

Everything stated here is backed by a test or a measurement in this repository. What is **not** done is listed in
[Known limitations](#known-limitations).

## Contents
- [Quick start](#quick-start)
- [The cache](#the-cache)
- [Single server (Redis protocol)](#single-server-redis-protocol)
- [Client and multi-server routing](#client-and-multi-server-routing)
- [Replicated cluster (Raft)](#replicated-cluster-raft)
- [Metrics](#metrics)
- [Build and test](#build-and-test)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)

## Quick start
Requires Java 17+ and Maven.
```bash
mvn clean test                                    # the full test suite
mvn compile
java -cp target/classes com.distri.cache.node.ClusterDemo   # kill-the-leader demo, three real server processes
```
(On Windows use `target\classes`.)

## The cache
```java
try (LRUCache<String, String> cache = new LRUCache<>(1000)) {
    cache.put("user:1", "Asha");
    cache.put("session:9", "token", 30_000);   // expires after 30 s
    Optional<String> v = cache.get("user:1");
}
```
`new LRUCache<>(capacity, cleanupIntervalMillis)` sets the background cleanup period; `0` turns the background task off.

- `put`, `put` with TTL, `get`, `remove`, `delete` (atomic, used by the server's `DEL`), `size`, `capacity`
- Strict LRU eviction. Operations on entries without a TTL are O(1); any operation that adds, changes or removes a TTL
  entry is O(log n) because of the expiry index
- TTL expiry three ways: lazily on `get`, eagerly on `put`, and in the background
- Expired entries never count against capacity and are always removed before a valid entry is evicted
- `ShardedLRUCache`: 16 independently locked shards for less lock contention; eviction order is per shard, not global
- Optional **buffered reads** (`bufferedReads = true`): hits are recorded in a lock-free buffer instead of taking the write
  lock, at the price of approximate recency under concurrency; 1.1x to 1.6x more throughput with 2 to 4 threads in my
  benchmark, no gain with one thread (see [docs/READ_BUFFER.md](docs/READ_BUFFER.md))
- `BaselineCache` (a `ConcurrentHashMap` wrapper) for benchmark comparison

## Single server (Redis protocol)
The cache can be served over TCP with a subset of the Redis wire protocol, so ordinary Redis clients can talk to it.
Commands: `PING`, `ECHO`, `GET`, `SET key value [EX seconds | PX millis | PXAT unix-millis]`, `DEL`, `DBSIZE`, `QUIT`,
`AUTH`, `REWRITEAOF`, and for replication `PSYNC`, `REPLICAOF`, `REPLINFO`. Keys and values are UTF-8 text.

```bash
mvn -DskipTests package
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --port 6380 --capacity 100000
python scripts/try_server.py 6380
```

| Option | What it does | Details |
|---|---|---|
| `--aof PATH` | Persistence: append-only log, replay on startup, three fsync policies, compaction | [docs/PERSISTENCE.md](docs/PERSISTENCE.md) |
| `--password-file PATH`, `--tls-keystore PATH` | Password (`AUTH`) and TLS | [docs/SECURITY.md](docs/SECURITY.md) |
| `--allow-replicas`, `--replicaof HOST:PORT` | Asynchronous primary-replica replication with full and partial resync | [docs/REPLICATION.md](docs/REPLICATION.md) |
| `--metrics-port P` | Prometheus metrics | [docs/METRICS.md](docs/METRICS.md) |

All of these are off by default, so the server listens on `127.0.0.1` only unless you pass `--host`. Connections beyond
the client limit (128 by default) receive an error and are closed.

## Client and multi-server routing
`CacheClient` talks to one server (get, set with optional TTL, delete, dbSize, ping) and reconnects after a failure.
`ClusterClient` spreads keys over several servers with a consistent-hash ring (160 virtual nodes per server), so the
same key always goes to the same server. Both accept a `ClientSecurity` (password and/or TLS trust store).

```java
try (ClusterClient cluster = ClusterClient.of("10.0.0.1:6380", "10.0.0.2:6380", "10.0.0.3:6380")) {
    cluster.set("user:1", "Asha");
    Optional<String> v = cluster.get("user:1");
}
```

`java -cp target/classes com.distri.cache.client.RingDemo` shows the ring in action. With 100,000 keys over 5 servers,
adding a sixth moved 16.4% of the keys (ideal 16.7%), and none moved between the existing servers. The same change with
`hash % n` moved 95.6% of the keys in that test.

`ClusterClient` does **not** copy data when a server joins or leaves (moved keys simply miss), does not read from
replicas, and does not fail over: a request routed to a dead server throws `CacheClientException`. For availability
use the replicated cluster below.

## Replicated cluster (Raft)
A group of servers (three in the demo) that keeps working when one of them dies. Full design, invariants and limits are
in [docs/RAFT.md](docs/RAFT.md).

- **Raft from scratch**: leader election, log replication, commitment by counting replicas only for current-term entries,
  a no-op entry when a leader takes over, and check-quorum so a cut-off leader steps down.
- **Durable**: the term, vote and log are stored with checksums and survive crashes, including a torn final write.
- **Exactly-once client retries**: clients carry a session id and sequence number, so a retry after a lost reply or a
  leader change takes effect at most once.
- **Reads go through the log**, so they are linearizable but cost about as much as writes (see the benchmark below).
- **Real TCP runtime**: each server is a process with a peer port (authenticated with a shared-secret challenge-response
  handshake) and a client port; a single owner thread runs each Raft node, and a node that hits an internal error stops
  itself rather than carry on in a state it cannot trust.

```bash
java -cp target/classes com.distri.cache.node.ClusterDemo
java -cp target/classes com.distri.cache.node.ClusterBenchmark --seconds 5
```

### Kill-the-leader demo
Three server processes, real disks, fsync on. The demo kills the leader with SIGKILL while a writer is running, then
restarts it. Output from one run on a Windows 10 laptop (JDK 23):

```
[  2.26s] cluster is up; leader is server 0
[  3.86s] KILLING the leader, server 0, with SIGKILL after 200 acknowledged writes
[  4.58s] first write after the kill acknowledged by server 2 after 713 ms
[  7.81s] 600 writes acknowledged; slowest single write took 699 ms
[ 10.71s] read back all 600 keys: 0 lost or wrong
[ 10.99s] linearizability check of 1324 recorded operations (writer and concurrent reader): LINEARIZABLE
```
This is a single run on one machine: it shows the mechanism works, not a guaranteed failover time. The time depends on
the election timeout settings.

### Benchmark
Three server processes, clients and disk all on one machine over loopback, 100-byte values, 5 s per cell after a 1 s
warm-up. Windows 10, JDK 23.0.1, 2 CPUs. Relative costs, not production capacity.

| fsync | op | clients | ops/second | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| on | SET | 1 | 207 | 4.68 | 10.15 |
| on | SET | 8 | 336 | 19.11 | 114.24 |
| on | SET | 32 | 354 | 75.24 | 380.67 |
| on | GET | 1 | 257 | 3.96 | 7.03 |
| on | GET | 8 | 465 | 16.20 | 31.36 |
| on | GET | 32 | 447 | 68.92 | 160.63 |
| off | SET | 1 | 1010 | 0.53 | 5.82 |
| off | SET | 8 | 2023 | 3.09 | 32.39 |
| off | SET | 32 | 2061 | 15.08 | 79.57 |
| off | GET | 1 | 2466 | 0.34 | 0.63 |
| off | GET | 8 | 2692 | 3.39 | 10.48 |
| off | GET | 32 | 3059 | 6.58 | 44.79 |

What it shows: with fsync on, every write waits for a disk sync on a majority, which caps throughput at a few hundred
writes per second on this machine (207 with one client, 354 with 32, about 1.7x), because each entry is synced separately
(batching several entries per sync is not implemented). Turning fsync off is about 5 to 10 times faster but gives up
durability, so `--no-fsync` is for experiments only. A read costs about the same as a write because it goes through the log.

## Metrics
Both server modes can serve Prometheus-format metrics at `http://127.0.0.1:PORT/metrics` with `--metrics-port PORT`
(off by default, no dependencies, no authentication, so keep it on localhost or behind a firewall).
Single server: command counts, hit ratio, latency histogram with estimated p50/p99/p99.9, connections, cache size, log
size, replication state. Replicated server: Raft term, role, leader, commit and apply lag, leader changes, and client
requests by outcome. Latency percentiles are bucket-edge estimates of server-side time, not client-observed latency. See
[docs/METRICS.md](docs/METRICS.md).

## Build and test
```bash
mvn clean test
```
413 tests, 0 failures at the time of writing, run on every push by GitHub Actions.

## Benchmarks
Single-node cache throughput uses JMH:
```bash
mvn -DskipTests install
cd benchmarks
mvn -q package
java -jar target/benchmarks.jar -t 1     # single thread
java -jar target/benchmarks.jar -t 4     # four threads
```
Results from one machine (Intel Core i3-7020U, 2 cores, JDK 23) are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).
Numbers depend on the machine, so run the benchmarks yourself before comparing. The baseline has no eviction or
ordering, so it is an upper bound, not a fair match. The replicated cluster benchmark is above.

## Testing
- Unit tests for LRU order, overwrite, validation, capacity edge cases and TTL, and a regression test for the case where
  capacity pressure must not evict a valid entry while an expired one exists
- Property-based tests (jqwik): random operation sequences replayed against a `LinkedHashMap` reference model
- Concurrency tests: 8 threads, small capacity and few keys, checking for exceptions, deadlock, `size <= capacity` and
  an intact internal structure
- Persistence tests: restart recovery, TTLs across restarts, every possible cut of a half-written record, corrupt-file
  handling, compaction, and a concurrent-writes test that replays the log and compares it with the live cache
- Security tests: password gating, lockout, pre-auth timeout, and TLS with trusted, untrusted and wrong-host-name certificates
- Replication tests: real servers over real sockets covering full and partial resync, backlog overflow, primary death
  and replacement, read-only replicas, promotion, password and TLS on the link
- **Raft in a deterministic simulator**: a seeded network that drops, duplicates, delays and partitions messages and
  crashes nodes. After every simulated tick the tests check the Raft invariants (election safety, log matching, leader
  completeness, state-machine safety). Raft faults injected on purpose (`RaftBugInjectionTest` and scripted node
  tests) are caught, which shows these tests can fail.
- **Linearizability**: a Wing-Gong/Lowe checker run on client histories recorded under crashes, partitions, message loss
  and duplication. The correct servers passed in all 126 runs; two deliberately broken servers (stale reads from a
  deposed leader, acknowledging before applying) were caught in 28 of 60 and 30 of 30 runs.
- **Real processes**: a test that kills real server leaders on real disks and checks the recorded history. Limits: with
  check-quorum on, the stale-read bug is mostly masked in real time (a cut-off leader steps down quickly), while the
  simulator still catches it; see docs/RAFT.md.
- Buffered-read tests: a concurrent recording test (everything reported as recorded is delivered exactly once, in order),
  identical behaviour to exact LRU with one thread over random sequences, and an 8-thread stress test; six deliberate bugs
  in the buffer code were each caught by a failing test
- Metrics tests compare every counter and gauge with what a real client or a real three-server group actually did.

The claim is **tested with randomized fault injection against stated invariants**. It is not a formal proof, and the
cluster has not been tested on separate machines or under real network faults beyond the partitions and kills described.

## Known limitations
**Single server**
- One lock guards each cache. A hit on anything other than the most recent entry reorders the list and takes the write
  lock, so read scalability is limited: this is the price of strict LRU. In the benchmarks, 4-thread throughput was lower
  than single-thread throughput (docs/BENCHMARKS.md). The opt-in buffered-reads mode removes the exclusive lock from hits
  and raised 2-to-4-thread throughput by 1.1x to 1.6x, but with approximate recency, no gain on one thread, and a hit-ratio cost
  that has not been measured (docs/READ_BUFFER.md). `ShardedLRUCache` improves 4-thread throughput by 1.2x to 2.8x
  over the single-lock cache on the test machine (2 cores) but does not exceed its own single-thread speed there.
- Cleanup removes all expired entries under the write lock, so a large batch expiring at once briefly blocks writers on
  that shard. The server uses one thread per connection and has no idle timeout once authenticated; keys and values are
  limited to 1 MiB each and 8 MiB per command.
- Persistence is a single append-only log that serialises writes while on and does not log evictions (docs/PERSISTENCE.md).
- Security is basic: one shared password (no users or permissions), no limit on guesses across connections, no client
  certificates, no certificate reloading (docs/SECURITY.md).
- Primary-replica replication is asynchronous: an acknowledged write can be lost if the primary fails before a replica
  has it, replicas can serve stale data, evictions are not replicated, and promotion is manual (`REPLICAOF NO ONE`).

**Replicated cluster (docs/RAFT.md)**
- Not formally verified or model-checked.
- No TLS or encryption for peer or client traffic in this mode (the single server has TLS). Use a trusted network or a tunnel.
- Reads go through the log, so they cost as much as writes. There is no read-index or lease optimisation.
- Each log entry is synced to disk separately; there is no group commit, which is why durable throughput is a few
  hundred writes per second in the benchmark above.
- No snapshots or log compaction: disk use and restart time grow with the number of writes.
- The group is fixed: servers cannot be added or removed while it runs. There is no data migration.
- Tested on one machine only. Failover time and throughput on separate machines or a real network are unmeasured.

**Metrics**
- Latency is server-side processing time and percentiles are bucket-edge estimates. Evictions are not counted.
  The overhead of recording has not been measured. The output has not been scraped by a real Prometheus server.

## Roadmap
- Done: Redis-protocol server, Java client, consistent-hash routing, append-only-log persistence, password authentication,
  TLS, asynchronous primary-replica replication
- Done: Raft core with a deterministic simulator and fault-injection tests; durable Raft storage; the cache as a
  replicated state machine with exactly-once client retries
- Done: linearizability checker on recorded histories, in the simulator and against real killed processes
- Done: a TCP runtime so the replicated cache runs as separate server processes, with the kill-the-leader demo and a
  measured benchmark
- Done: Prometheus metrics for both server modes
- Done: opt-in buffered reads with a before/after JMH comparison (docs/READ_BUFFER.md); not yet exposed as a server flag
- Not started (ideas, in the order I would do them): group commit for the Raft log; measuring the hit-ratio cost of buffered
  reads; log snapshots and compaction; read-index reads; TLS for the replicated mode; a test on separate machines
- Dropped: a separate failover monitor process (Raft elects its own leader) and data migration on membership changes
