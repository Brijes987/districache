# DistriCache

[![CI](https://github.com/Brijes987/DistriCache/actions/workflows/ci.yml/badge.svg)](https://github.com/Brijes987/DistriCache/actions/workflows/ci.yml)

A thread-safe, in-memory LRU cache with per-entry TTL, written in Java 17 from first principles: a hash
map plus a **hand-written doubly-linked list**, guarded by a `ReentrantReadWriteLock`.

This is Phase 1 (a single node). Distribution is not built yet; see [Roadmap](#roadmap).

## Features
- `put`, `put` with TTL, `get`, `remove`, `size`, `capacity` (the `Cache` interface)
- Strict LRU eviction. Operations on entries without a TTL are O(1); any operation that adds, changes or removes a TTL entry is O(log n) because of the expiry index
- TTL expiry in three ways: lazily on `get`, eagerly on `put`, and in the background
- Expired entries never count against capacity and are always removed before a valid entry is evicted
- `BaselineCache` (a `ConcurrentHashMap` wrapper) for benchmark comparison

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

Linearizability is **not** claimed, and these tests do not prove it.

## Known limitations
- One lock guards the cache. A hit on anything other than the most recent entry reorders the list and takes the
  write lock, so read scalability is limited. This is the price of strict LRU. In the benchmarks, 4-thread
  throughput was lower than single-thread throughput (see docs/BENCHMARKS.md).

## Roadmap
Phase 2 (not started): sharding and replication across nodes.

Design notes: [docs/DESIGN.md](docs/DESIGN.md)
