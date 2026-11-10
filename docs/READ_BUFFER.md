# Buffered reads (optional LRU mode)

By default a hit on anything other than the most recent entry reorders the LRU list, which needs the cache's write
lock. Under many threads most hits take that exclusive lock, which limits read scalability (see
[BENCHMARKS.md](BENCHMARKS.md)). Buffered reads is an **opt-in** mode that avoids it, using the same idea as
Caffeine's read buffers.

```java
LRUCache<String, String> cache = new LRUCache<>(10_000, 0, true);                 // single lock
ShardedLRUCache<String, String> sharded = new ShardedLRUCache<>(10_000, 16, 0, true); // 16 shards
```
The default (`false`, or the constructors without the flag) is unchanged: exact LRU.

## How it works
- A `get` takes only the **read** lock, returns the value, and records the entry in a `ReadBuffer`.
- `ReadBuffer` is a set of small ring buffers (one stripe per core, 64 slots each). Producers claim a slot with one
  compare-and-set and publish with an ordered store: no lock, no blocking, no allocation. A thread always uses the same stripe.
- The recorded hits are applied to the list in a batch under the write lock. That happens when a stripe is half full and the
  thread wins `tryLock` (it never waits), **before every `put` evicts anything**, and before a snapshot
  (`snapshotLruToMru`, used by log compaction and full resync).
- When a stripe is full and the write lock is busy, the hit is **dropped**: that entry's recency is simply not refreshed.

## What is guaranteed, and what is not
- Values, TTL behaviour and the capacity limit are the same as in the default mode. A returned value is always what was
  stored, because it is read under the read lock.
- Recency is **approximate under concurrency**: a hit's effect can be applied late, or dropped. The only visible effect
  is which entry a later `put` evicts. It can evict an entry that was read recently if that read was dropped.
- With **one thread**, nothing is ever dropped and the eviction order is **identical** to the default mode. This is
  tested by replaying random operation sequences against both modes and comparing every answer and the final order.
- A `get` of an expired entry returns empty but does not remove it (the next `put`, the background cleanup or
  `cleanupExpiredEntries()` does).
- Reads still take the read lock, whose own state is shared between readers, so this does not make reads fully
  lock-free. It removes the exclusive lock, not all contention.

## Tests
- `ReadBufferTest`: order, full-ring dropping, drain advice, stripe independence, and a concurrency test with six
  producers and a draining thread showing that every element reported as recorded is delivered exactly once, in
  order per producer, with plenty of drops and wrap-arounds.
- `BufferedReadsTest`: a hit protects an entry from eviction; snapshots see recent hits; entries removed, cleared or
  replaced after a hit are never brought back; single-thread equivalence with exact LRU (40 random sequences, plus
  the sharded cache); and an 8-thread stress test that checks values, capacity and the internal structure.
- Each of six deliberate bugs in this code (no drain before eviction, no identity check when draining, no drain before
  a snapshot, forgetting to publish the read position, ignoring a full ring, not clearing slots) made at least one test fail.

## Benchmark
`benchmarks/` has `lru-buffered` and `sharded-buffered` next to `lru` and `sharded`. All four were run in the same
invocation, per thread count, so they share the machine's conditions:
```
java -jar target/benchmarks.jar "CacheBenchmark.(get|mixed90_10)" -p impl=lru,lru-buffered,sharded,sharded-buffered -p capacityRatio=1.0 -t 4
```

### Results
One laptop (Windows 10, 2 CPUs, JDK 23.0.1, JMH 1.37), 10,000 keys, capacity equal to the key space (so every `get` is
a hit, and almost none is on the most recent entry), 2 forks x 5 measured iterations, 99.9% confidence interval
after the plus-minus sign. Throughput in operations per microsecond; higher is better. `mixed90_10` is 90% gets and 10% puts.

| Threads | Benchmark | lru | lru-buffered | sharded | sharded-buffered |
|---:|---|---:|---:|---:|---:|
| 1 | get | 6.09 ± 0.28 | 6.03 ± 0.19 | 5.74 ± 1.49 | 4.64 ± 0.26 |
| 1 | mixed90_10 | 4.50 ± 0.90 | 4.26 ± 0.71 | 4.14 ± 0.18 | 4.05 ± 0.77 |
| 2 | get | 2.42 ± 0.47 | 3.47 ± 0.49 | 4.84 ± 0.17 | 5.88 ± 0.18 |
| 2 | mixed90_10 | 2.21 ± 0.29 | 2.84 ± 0.38 | 4.99 ± 0.46 | 5.55 ± 0.20 |
| 4 | get | 2.86 ± 1.32 | 4.61 ± 0.23 | 5.11 ± 0.58 | 6.80 ± 0.29 |
| 4 | mixed90_10 | 1.83 ± 0.26 | 2.43 ± 0.33 | 5.10 ± 0.33 | 5.99 ± 0.25 |

Buffered over plain, same run (ratio of the averages):

| Threads | Benchmark | single lock | 16 shards |
|---:|---|---:|---:|
| 2 | get | 1.43x | 1.21x |
| 2 | mixed90_10 | 1.29x | 1.11x |
| 4 | get | 1.61x | 1.33x |
| 4 | mixed90_10 | 1.33x | 1.18x |

### What this shows
- With several threads, buffered reads are faster in every cell, by about 1.1x to 1.6x. The gain is larger for the
  single-lock cache (whose write lock is the bottleneck) and smaller for the sharded cache (already less contended).
- It does **not** make reads scale freely. On this 2-CPU machine the best 4-thread figure (sharded-buffered, 6.8) is
  about the same as the best single-thread figure (6.1).
- **With one thread there is no gain**, and for `get` on the sharded cache buffering looks slower (4.6 against 5.7, but
  the plain sharded result has a wide interval because one of its two runs was disturbed, so the size of the loss is
  uncertain). Single-thread use pays for recording and draining and gets nothing back. That is why the mode is opt-in.

### What was not measured
- Only `capacityRatio=1.0` (every get a hit), which is the case buffering helps most. Workloads with misses and evictions
  were not run.
- **The cost of approximate recency was not measured**: how many hits get dropped, and how much the hit ratio of a
  small cache changes under concurrent skewed access. Until that is measured, the benefit above is a throughput result only.
- Only 2 CPUs, one run per cell group. The intervals of a few neighbouring cells overlap (for example `lru` against
  `lru-buffered` for `mixed90_10` at 2 threads), so small differences should not be read as real.
- Not wired into the server: `ServerMain` has no flag for it yet.
