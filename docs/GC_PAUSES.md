# Garbage-collection pauses and snapshot pauses

**Status: one run of each tool, on one machine (below). Not repeated, so run-to-run variation is unknown.**

## What can pause a cache server
1. The JVM's garbage collector, because a cache holds many small objects (a String key, a String value and a map node per
   entry). A collection that stops application threads is a pause every request waits through.
2. Taking a snapshot of the replicated cache copies the whole state on the Raft node's own thread
   (`ReplicatedCache.snapshot()`), so that server answers nothing, sends no heartbeats and grants no votes meanwhile.
   (docs/SNAPSHOTS.md has said "not measured" for this.)

## Tool 1: `GcPauseBenchmark` (in-process cache, one JVM)
Fills a `ShardedLRUCache` with `--entries` entries, then runs `--threads` threads (90% get, 10% put, random keys) and
reports, from three angles:
- request latency of the operations (p50, p99, p99.9, max);
- stalls seen by the application: a thread sleeps 1 ms in a loop and records how much longer than 1 ms each sleep took.
  Any pause that stops Java threads shows up, whatever the cause. An idle baseline is measured first, so the noise of the
  machine (timer, scheduler, other programs) can be told apart from GC;
- every collection the JVM reports, with the duration it gives. For a concurrent collector a "cycle" includes time the
  application kept running, so it is not a pause. For real stop-the-world pauses the JVM's own log is summarised:
  `--summarize-gc-log FILE` adds up every `Pause ...` line.

Run all three collectors (G1, Parallel, ZGC) with the script, from the project folder after `mvn -q compile`:

    scripts\gc-pause.bat 2g 2000000        (Windows)
    scripts/gc-pause.sh 2g 2000000         (Linux, macOS)

Arguments are heap size and number of entries. It takes about four minutes and writes `gc-results\run-*.txt` and
`gc-results\pauses-*.txt`. Close other programs first, do not run it on battery, and run it twice to see how much the
numbers move. To try another heap, change the first argument; the heap has to be larger than the data
(about 2 million entries of 100 bytes need well over 1 GiB once object overhead is counted).

Limits: this is the cache in one JVM, with no network, disk or Raft. It says nothing about collectors, heap sizes or
workloads you did not run. Percentiles are the upper edge of 10-microsecond bins (the maximum is exact). The GC log covers
the whole JVM life, including the fill and the idle baseline, so its pause counts are larger than the "collections during
the measured period" line; compare the two only knowing that.

## Tool 2: `SnapshotPauseBenchmark` (how long `snapshot()` and `restore()` take)
    java -Xmx2g -cp target/classes com.distri.cache.raft.SnapshotPauseBenchmark 10000 100000 1000000

For each cache size (entries of 100 bytes from 1000 different clients) it takes the snapshot 7 times, drops the first 2 as
warm-up and prints the median and worst time of `snapshot()`, and of `restore()` into a fresh cache, with the snapshot
size. It includes the GC work the copy causes; it does not include writing the file or sending it. The size cap is 128 MiB
(docs/SNAPSHOTS.md), so very large sizes are not snapshotted at all; stop where the printed snapshot size passes 128.

## Results (one run each)

Machine: Windows 10, **2 CPUs**, Java 23.0.1, 2 GiB heap (`-Xms2g -Xmx2g`), 2,000,000 entries of 100-byte values,
**4 worker threads** (more threads than CPUs), 60 s measured, 5 s idle baseline. Run with `scripts\gc-pause.bat 2g 2000000`.

| Collector | p99 request ms | max request ms | stall p99 under load ms | max stall under load ms | idle-baseline max stall ms | longest stop-the-world pause in the JVM log ms |
|---|---|---|---|---|---|---|
| G1 | 0.01 | 2199 | 153 | 2198 | 4.0 | 369 (Young, Normal) |
| Parallel | 0.01 | 8741 | 32 | 8741 | 3.9 | 8740 (one Full GC) |
| ZGC | 0.02 | 590 | 60 | 557 | 3.1 | 1.09 |

Stop-the-world pauses added up over the whole JVM life (fill, baseline and load): G1 35 pauses, 4.8 s in total (median
119 ms); Parallel 23 pauses, 15.2 s (median 236 ms; 22 young pauses up to 804 ms and the one 8.7 s Full GC); ZGC 58 pauses,
4.2 ms in total.

What the numbers show:
- Parallel: the single Full GC (reason "Ergonomics") stopped everything for 8.7 s, and the application saw exactly that stall.
- ZGC: its own pauses are tiny (at most 1.09 ms), as designed.
- G1 and ZGC: the application nevertheless saw stalls of 2.2 s (G1) and 0.56 s (ZGC), far above the idle baseline (about
  4 ms). For ZGC those stalls are not stop-the-world pauses. For G1 the largest logged pause is 369 ms, so most of the
  2.2 s is not explained by the log. I did not find the cause. Likely contributors, not verified: 4 busy threads on 2 CPUs
  competing with the collector's own threads, and the heap being small for this much data (ZGC major cycles took up to 6.2 s).
- So on this machine, with this setup, ZGC had by far the shortest GC pauses but not the best end-to-end stalls, and the
  setup (2 CPUs, 4 workers, 2 GiB) is not a fair test of a concurrent collector. Do not read this as a ranking of collectors.

Not done, and needed before drawing conclusions: a repeat run (variation), a run with `--threads 1` so the collector has
a CPU (`scripts\gc-pause.bat 2g 2000000 1`), a larger heap, and a machine with more cores.

## Snapshot pause (`SnapshotPauseBenchmark`, same machine, `-Xmx2g`)

| Entries | snapshot MiB | snapshot() median ms | snapshot() worst ms | restore() median ms | restore() worst ms |
|---|---|---|---|---|---|
| 10,000 | 1.2 | 10.1 | 28.1 | 6.9 | 27.8 |
| 100,000 | 12.4 | 119.8 | 161.3 | 91.7 | 433.0 |
| 1,000,000 | 125.7 | 2,473 | 32,891 | 2,119 | 7,029 |

- Five timed runs per size (two warm-up runs dropped), so "worst" is the worst of five.
- 1,000,000 entries of 100 bytes is already 125.7 MiB: just under the 128 MiB snapshot cap, so about a million such entries is the
  most that can be snapshotted at all.
- The 32.9 s worst case at 1,000,000 entries is far from its 2.5 s median. With a 2 GiB heap the copy needs much of the
  heap, so garbage collection probably dominates, but I did not check. Rerun with `-Xmx4g` to see whether it goes away.
- What it means for Raft (follows from the design, not tested): the node's thread does the copy, so it sends no heartbeats
  meanwhile. With the defaults an election timeout is 300 to 600 ms. At 100,000 entries the pause (about 0.12 to 0.16 s) is
  below that; at 1,000,000 entries (2.5 s median) it is far above it, so followers would start an election while the
  leader is taking a snapshot. This has not been run.
