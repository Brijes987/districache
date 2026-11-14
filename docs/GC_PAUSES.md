# Garbage-collection pauses and snapshot pauses

**Status: tools written and tested; results NOT measured yet.** Nothing in this file is a result until the tables at the
bottom are filled in from a real run. The tools were run only to check that they work.

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

## Results

Fill these in from your runs (machine, Java version and the printed header line go with them).

| Collector | heap | p99 request ms | max request ms | max stall under load ms | idle-baseline max stall ms | longest stop-the-world pause (log) ms |
|---|---|---|---|---|---|---|
| G1 | | not measured | not measured | not measured | not measured | not measured |
| Parallel | | not measured | not measured | not measured | not measured | not measured |
| ZGC | | not measured | not measured | not measured | not measured | not measured |

| Entries | snapshot MiB | snapshot() median ms | snapshot() worst ms | restore() median ms |
|---|---|---|---|---|
| 10,000 | not measured | not measured | not measured | not measured |
| 100,000 | not measured | not measured | not measured | not measured |
| 1,000,000 | not measured | not measured | not measured | not measured |
