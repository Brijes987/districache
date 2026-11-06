# DistriCache

[![CI](https://github.com/Brijes987/districache/actions/workflows/ci.yml/badge.svg)](https://github.com/Brijes987/districache/actions/workflows/ci.yml)

A thread-safe, in-memory LRU cache with per-entry TTL, written in Java 17 from first principles: a hash
map plus a **hand-written doubly-linked list**, guarded by a `ReentrantReadWriteLock`.

Includes a Redis-protocol server, a Java client, and client-side consistent-hash routing across several independent servers. Servers do not talk to each other: there is no replication, failover or data migration (see [Known limitations](#known-limitations) and [Roadmap](#roadmap)).

## Features
- `put`, `put` with TTL, `get`, `remove`, `delete` (atomic, used by the server's `DEL`), `size`, `capacity`
- Strict LRU eviction. Operations on entries without a TTL are O(1); any operation that adds, changes or removes a TTL entry is O(log n) because of the expiry index
- TTL expiry in three ways: lazily on `get`, eagerly on `put`, and in the background
- Expired entries never count against capacity and are always removed before a valid entry is evicted
- `BaselineCache` (a `ConcurrentHashMap` wrapper) for benchmark comparison
- `ShardedLRUCache`: 16 independently locked shards for less lock contention; eviction order is per shard, not global
- Optional persistence: an append-only log with replay on startup, three fsync policies and compaction (see [docs/PERSISTENCE.md](docs/PERSISTENCE.md))
- Optional password (`AUTH`) and TLS for the server and client, off by default (see [docs/SECURITY.md](docs/SECURITY.md))

## Usage
```java
try (LRUCache<String, String> cache = new LRUCache<>(1000)) {
    cache.put("user:1", "Asha");
    cache.put("session:9", "token", 30_000);   // expires after 30 s
    Optional<String> v = cache.get("user:1");
}
```
`new LRUCache<>(capacity, cleanupIntervalMillis)` sets the background cleanup period; `0` turns
the background task off.

## Server (Redis protocol)
The cache can be served over TCP using a subset of the Redis wire protocol, so ordinary Redis clients can talk to it.
Supported commands: `PING`, `ECHO`, `GET`, `SET key value [EX seconds | PX millis | PXAT unix-millis]`, `DEL`, `DBSIZE`, `QUIT`, `AUTH`, `REWRITEAOF`.
Keys and values are UTF-8 text.
Optional flags: `--aof PATH` (persistence), `--password-file PATH` and `--tls-keystore PATH` (security). See
[docs/PERSISTENCE.md](docs/PERSISTENCE.md) and [docs/SECURITY.md](docs/SECURITY.md).

## Client and multi-server routing
`CacheClient` talks to one server (get, set with optional TTL, delete, dbSize, ping) and reconnects after a failure.
`ClusterClient` spreads keys over several servers with a consistent-hash ring (160 virtual nodes per server), so the
same key always goes to the same server.
Both accept a `ClientSecurity` (password and/or TLS trust store) for servers that require it.

```java
try (ClusterClient cluster = ClusterClient.of("10.0.0.1:6380", "10.0.0.2:6380", "10.0.0.3:6380")) {
    cluster.set("user:1", "Asha");
    Optional<String> v = cluster.get("user:1");
}
```

`java -cp target/classes com.distri.cache.client.RingDemo` shows the ring in action. With 100,000 keys over 5 servers,
adding a sixth moved 16.4% of the keys (ideal 16.7%), and none moved between the existing servers. The same change
with `hash % n` moved 95.6% of the keys in that test.

What it does **not** do: it does not copy data when a server joins or leaves (moved keys simply miss), it does not
replicate, and it does not fail over: a request routed to a dead server throws `CacheClientException`.

```bash
mvn -DskipTests package
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --port 6380 --capacity 100000
python scripts/try_server.py 6380
```

Password and TLS are optional and off by default (see [docs/SECURITY.md](docs/SECURITY.md)), so the server listens on
`127.0.0.1` only unless you pass `--host`. Connections beyond the client limit (128 by default) receive an error and
are closed; there is no idle timeout once a connection is authenticated.

## Build and test
Requires Java 17+ and Maven.
```bash
mvn clean test
```

## Benchmarks (JMH)
```bash
mvn -DskipTests install
cd benchmarks
mvn -q package
java -jar target/benchmarks.jar -t 1     # single thread
java -jar target/benchmarks.jar -t 4     # four threads
```
Results from one machine (Intel Core i3-7020U, 2 cores, JDK 23) are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).
Numbers depend on the machine, so run the benchmarks yourself before comparing. The baseline has no eviction
or ordering, so it is an upper bound, not a fair match.

## Testing
- Unit tests for LRU order, overwrite, validation, capacity edge cases and TTL
- A regression test for the case where capacity pressure must not evict a valid entry while an expired one exists
- Property-based tests (jqwik): random operation sequences are replayed against a `LinkedHashMap` reference model
- Concurrency tests: 8 threads, small capacity and few keys, checking for exceptions, deadlock,
  `size <= capacity` and an intact internal structure
- Persistence tests: restart recovery, TTLs across restarts, every possible cut of a half-written record, corrupt-file handling, compaction, and a concurrent-writes test that replays the log and compares it with the live cache
- Security tests: password gating, lockout, pre-auth timeout, and TLS with trusted, untrusted and wrong-host-name certificates (throw-away test certificates)

Linearizability is **not** claimed, and these tests do not prove it.

## Known limitations
- One lock guards the cache. A hit on anything other than the most recent entry reorders the list and takes the
  write lock, so read scalability is limited. This is the price of strict LRU. In the benchmarks, 4-thread
  throughput was lower than single-thread throughput (see docs/BENCHMARKS.md).
- `ShardedLRUCache` improves 4-thread throughput by 1.2x to 2.8x over the single-lock cache on the test machine
  (2 cores), but does not exceed its own single-thread speed there. See docs/BENCHMARKS.md.
- Cleanup removes all expired entries under the write lock, so a large batch expiring at once briefly blocks writers
  on that shard. The server uses one thread per connection and has no idle timeout; keys and values are limited to 1 MiB
  each and 8 MiB per command.
- Persistence is a single append-only log. It has no checksums, serialises writes while on, and does not log evictions. Details in docs/PERSISTENCE.md.
- Security is basic: one shared password (no users or permissions), no limit on guesses across connections, no
  client certificates, no certificate reloading. See docs/SECURITY.md.


## Roadmap
- Done: Redis-protocol server, Java client, consistent-hash routing over several servers, append-only-log persistence, password authentication, TLS
- Planned next: server-to-server link, asynchronous primary-replica replication, failover, data migration on membership changes
