# Benchmark results

Measured with JMH 1.37 using `benchmarks/src/main/java/com/distri/cache/bench/CacheBenchmark.java`
(`@Fork(2)`, 3 warmup and 5 measurement iterations of 1 s each). Raw output is in
[`benchmark-results/`](benchmark-results/).

**Machine:** Intel Core i3-7020U @ 2.30 GHz, 2 cores / 2 hardware threads, Windows 10, Oracle JDK 23.0.1.
A laptop, not a quiet server, so treat small differences as noise.

The key space is 10,000 integer keys. `capacityRatio 1.0` means every key fits (all `get`s hit);
`0.1` means the cache holds 1,000 keys (most `get`s miss and most `put`s evict).
`BaselineCache` is a bare `ConcurrentHashMap` with no eviction and no ordering, so it is an upper
bound and not a like-for-like competitor.

## Throughput, millions of operations per second

| Benchmark | capacityRatio | Threads | LRUCache | Baseline |
|---|---|---|---|---|
| get | 1.0 | 1 | 4.66 | 42.69 |
| get | 0.1 | 1 | 16.77 | 43.06 |
| put | 1.0 | 1 | 7.53 | 14.46 |
| put | 0.1 | 1 | 7.40 | 14.44 |
| mixed 90/10 | 1.0 | 1 | 5.76 | 29.37 |
| mixed 90/10 | 0.1 | 1 | 12.97 | 28.47 |
| get | 1.0 | 4 | 1.71 | 79.06 |
| get | 0.1 | 4 | 6.69 | 78.75 |
| put | 1.0 | 4 | 3.96 | 16.51 |
| put | 0.1 | 4 | 4.14 | 17.85 |
| mixed 90/10 | 1.0 | 4 | 1.75 | 49.49 |
| mixed 90/10 | 0.1 | 4 | 3.98 | 43.64 |

## What this shows

- **Eviction cost does not grow with the cache.** `put` is about as fast when most puts evict (0.1) as
  when none do (1.0).
- **Hits are the expensive path.** A hit must move the entry to the front of the list, which needs the
  write lock. Misses use only the read lock.
- **A single lock does not scale.** With 4 threads, LRUCache throughput falls to roughly 0.3 to 0.55 of
  its single-thread rate, while the baseline gains. Note that 4 threads on this 2-core machine are
  oversubscribed, so the exact ratios are noisy, but the direction is clear.

## Possible next step

Split the cache into independently locked shards so threads touching different keys do not contend.
This would trade strict global LRU order for per-shard LRU order.

# Scaling with cache size: current implementation vs the earlier one

`SizeScalingBenchmark` fills a cache to capacity, then measures one `put` of a key that is not present
(so every call evicts) and one `size()` call. `legacy` is `LegacyLRUCache`, the earlier implementation of
this project, kept in the benchmarks module only for this comparison. It counted valid entries by scanning
the whole map. Single thread; JMH 1.37, 1 fork, 2 warmup and 3 measurement iterations of 1 s each. Same
machine as above. Raw output: [`benchmark-results/bench-size.txt`](benchmark-results/bench-size.txt).

Average time per operation in microseconds (lower is better):

| Operation | Entries | Current | Earlier version |
|---|---|---|---|
| put of a new key (evicts) | 1,000 | 0.148 | 23.1 |
| put of a new key (evicts) | 10,000 | 0.142 | 238.6 |
| put of a new key (evicts) | 100,000 | 0.154 | 3,594.8 |
| size() | 1,000 | 0.033 | 8.6 |
| size() | 10,000 | 0.036 | 90.4 |
| size() | 100,000 | 0.038 | 1,268.7 |

The earlier version's cost grows roughly in proportion to the number of entries (O(n)); the current
version's cost does not change with size. Only 3 measurement iterations were run, and the current version's
timings are so small that their error bars are relatively wide, so treat the exact ratio as approximate; the
trend is unambiguous.
