# Garbage-collection pauses and snapshot pauses

**Status: three GC runs and three snapshot runs, on one laptop with 4.16 GB of RAM (below). The first two GC runs were probably distorted by memory shortage; the third is the most trustworthy. Treat every number as "what happened once".**

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

Second run, same machine and settings but **1 worker thread** (`scripts\gc-pause.bat 2g 2000000 1`):

| Collector | p99 request ms | max request ms | stall p99 under load ms | max stall under load ms | idle-baseline max stall ms | longest stop-the-world pause in the JVM log ms |
|---|---|---|---|---|---|---|
| G1 | 0.01 | 410 | 51 | 574 | 6.3 | 465 (Young, Normal) |
| Parallel | 0.01 | 1070 | 8.4 | 1934 | 6.5 | 1782 (Young) |
| ZGC | 0.03 | 1498 | 38 | 1611 | 16.8 | 1.57 |

Stop-the-world pauses added up over the JVM life: G1 29 pauses, 4.9 s (median 124 ms); Parallel 13 pauses, 8.2 s (median 392 ms);
ZGC 44 pauses, 7.2 ms in total.

What the two runs show:
- ZGC's own stop-the-world pauses were tiny both times (at most 1.09 and 1.57 ms), as designed.
- G1's longest logged pause was 369 ms and 465 ms; its young-generation pauses had a median of about 120 ms in both runs.
- Parallel is the least consistent: the first run had one 8.7 s Full GC, the second had none but young pauses up to 1.8 s. One run
  cannot say which is typical.
- For every collector the application saw stalls of 0.4 to 8.7 s, far above the idle baseline (4 to 17 ms). For ZGC those stalls
  are not stop-the-world pauses, and for G1 most of them are not explained by the logged pauses. **I do not know the cause.**
  My first guess (4 workers competing with the collector on 2 CPUs) is weakened: with 1 worker ZGC still stalled 1.6 s. What
  remains is untested: a heap too small for this much live data (ZGC major cycles took up to 16 s), or the machine itself
  (the baseline max stall was 16.8 ms in the second ZGC run, so it is noisy). Look for "Allocation Stall" in `gc-ZGC.log`.
- So these runs do not rank the collectors, and they do not show that any of them is good for this cache on this machine.

Third run: **500,000 entries, 1 GiB heap, 1 worker thread** (`scripts\gc-pause.bat 1g 500000 1`), chosen because the laptop has
only 4.16 GB of physical RAM (`wmic computersystem get TotalPhysicalMemory` = 4,157,419,520 bytes) and a 2 GiB heap with 2M entries
plus Windows and other programs may not have fitted:

| Collector | p99 request ms | max request ms | stall p99 under load ms | max stall under load ms | idle-baseline max stall ms | longest stop-the-world pause in the JVM log ms |
|---|---|---|---|---|---|---|
| G1 | 0.01 | 397 | 32 | 391 | 21.8 | 153 (Young, Normal) |
| Parallel | 0.01 | 482 | 2.1 | 482 | 5.1 | 481 (one Full GC) |
| ZGC | 0.01 | 267 | 9.2 | 318 | 31.9 | 0.06 |

Pauses added up over the JVM life: G1 38 pauses, 2.9 s (median 86 ms); Parallel 42 pauses, 3.7 s (median 73 ms); ZGC 59 pauses, 1.5 ms.

What the three runs show:
- The multi-second stalls of the first two runs (up to 8.7 s) are gone: the worst stall of any collector is now 0.48 s. That fits the
  explanation that the first two runs were short of physical memory. It is not proof: two things changed at once (heap 2 GiB to 1 GiB, and
  2,000,000 to 500,000 entries), so which one mattered is not known.
- In this run, each collector's worst stall is close to its worst logged pause: Parallel 482 ms vs a 481 ms Full GC, G1 391 ms vs 153 ms
  (not fully explained), ZGC 318 ms vs 0.06 ms (not explained: ZGC's pauses are not the cause).
- ZGC's stop-the-world pauses were at most 1.57 ms in all three runs. The remaining ZGC stalls of a few hundred milliseconds are
  unexplained. The idle baseline on this machine itself reached 32 ms in this run, so it is noisy.
- G1's young pauses had a median of 86 to 124 ms in all three runs, and its longest pause was 153 to 465 ms.
- Parallel's single Full GC cost 0.48 s here and 8.7 s in the first run; the first run's heap was far larger and probably paging.
- These runs do not rank the collectors. One laptop, one workload, no repeats of the third run.

Not done: repeating the third run, a machine with more RAM and cores, and larger data sets on such a machine (the question this page
set out to answer for a big cache is still open).

## Snapshot pause (`SnapshotPauseBenchmark`)

Same machine (2 CPUs, Windows 10, Java 23), `-Xmx2g`, five timed runs per size (two warm-up runs dropped), so "worst" is the worst of five:

| Entries | snapshot MiB | snapshot() median ms | snapshot() worst ms | restore() median ms | restore() worst ms |
|---|---|---|---|---|---|
| 10,000 | 1.2 | 10.1 | 28.1 | 6.9 | 27.8 |
| 100,000 | 12.4 | 119.8 | 161.3 | 91.7 | 433.0 |
| 1,000,000 | 125.7 | 2,473 | 32,891 | 2,119 | 7,029 |

The same 1,000,000 entries with `-Xmx4g` on that machine: snapshot() median **48,886 ms**, worst 228,723; restore() median 28,506,
worst 214,176. Larger heap, 20 times slower. The machine has 4.16 GB of RAM, so a 4 GiB heap leaves almost nothing for Windows;
paging is the likely cause, though I did not watch memory use.

With a smaller heap (`-Xmx1g`, third run): 100,000 entries: snapshot() median 71 ms (worst 598), restore() median 78 ms (worst 177);
500,000 entries (62.8 MiB): snapshot() median **863 ms** (worst 962), restore() median 313 ms (worst 580). The 500,000-entry time is
about 12 times the 100,000-entry time for 5 times the data, so cost is not proportional to size on this laptop.

Cross-check, run by me, a different machine (Linux, 2 CPUs, 8 GB, Java 21): 1,000,000 entries took a snapshot() median of 581 ms
at `-Xmx2g` and 513 ms at `-Xmx4g` (worst 628 and 766 ms), restore() about 550 and 515 ms, and 500,000 entries about 260 ms. There
the cost grew roughly in proportion to size and did not depend on the heap size, so the code itself shows no sign of the
blow-up; it points at the Windows machine's memory. From the Windows machine I would trust the 10,000, 100,000 and (1 GiB heap) 500,000 rows, not the 1,000,000 row.

- 1,000,000 entries of 100 bytes is already 125.7 MiB: just under the 128 MiB snapshot cap, so about a million such entries is the
  most that can be snapshotted at all.
- What it means for Raft (follows from the design, not tested): the node's thread does the copy, so it sends no heartbeats meanwhile.
  With the defaults an election timeout is 300 to 600 ms. At 100,000 entries the pause (about 0.07 to 0.16 s here) is below that.
  At 500,000 entries (0.86 s here) and at 1,000,000 entries (0.5 s on the Linux machine, more on the laptop) it is above it, so
  followers would start an election while the leader takes a snapshot. That mechanism was shown in a real three-server cluster with a
  simulated slow snapshot (`SnapshotLeadershipTest`, docs/SNAPSHOTS.md); it has not been shown with a real state of that size.
