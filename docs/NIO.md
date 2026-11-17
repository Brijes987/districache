# Event-loop server (`--io nio`)

`NioCacheServer` serves the same Redis-protocol commands as `CacheServer`, but with a few event-loop threads instead of
one thread per connection. It is opt-in: `ServerMain --io nio [--io-threads N]`. The default is still the
thread-per-connection server, which is the only one that supports TLS and replication.

## How it works
- One accept thread. `ioThreads` loop threads (default: CPU count, at most 4), each with its own `Selector`; accepted
  connections are handed out round-robin.
- A loop reads what has arrived, parses every whole command out of it (`RespNioParser`, an incremental version of
  `RespReader` with the same limits and error messages), runs each command with the **same `CommandExecutor`** as the
  blocking server, and writes the replies.
- Commands run on the loop thread, so they must not block. The cache operations are in memory and short. Two places that
  could block are handled: a failed `AUTH` is delayed by holding that connection's reply (the blocking server sleeps
  250 ms, which would stall every other connection of the loop), and a client that does not read its replies stops being
  read from once 256 KiB of replies are queued, so memory per connection stays bounded.

## What it does not do
- **No TLS and no replication** (`ServerMain` refuses the combination with a clear message). A replica link takes over a
  blocking socket; TLS needs `SSLEngine` handling that is not written.
- **Persistence works, but `--fsync always` blocks the loop:** a write waits for the disk on the loop thread, delaying
  every other connection on that loop. With `everysec` or `no` this does not apply.
- The pre-authentication timeout is measured from connect to a successful `AUTH` (the blocking server uses a per-read
  timeout). There is still no idle timeout once authenticated.
- Not exposed as part of the Raft cluster's client port; this is the standalone server only.

## Tests (all in the Maven suite)
- `RespNioParserTest` (13): one command, pipelined, fed one byte at a time, **every possible split point** of a
  three-command stream, blank/empty commands, every protocol error with the blocking reader's message, limits firing
  from the header before data arrives, buffer released after a large command.
- `NioCacheServerTest` (22): the basic checks of `CacheServerTest` against real sockets; a 1 MiB value; connection limit
  and slot release; close; 500 idle connections with at most 10 extra threads; several loops under concurrent clients;
  a client that never reads 30 MB of replies is bounded and does not delay another client on the same single loop; failed
  `AUTH` delayed without blocking another connection; 5 failures close; pre-auth timeout; writes logged and recovered
  from the append-only log; an error reply is delivered although the client keeps sending; a connection the server decided to close is dropped even if the client never closes.
- **Differential test:** 35 raw byte streams (valid pipelines, inline commands, every malformed-input case, oversized
  values, streams cut off mid-command, a 4,000-command pipeline) are sent to both servers and the complete reply bytes
  must be identical. The first run found one difference, and it was in the *blocking* server: when a client closed in the
  middle of a command, replies to earlier pipelined commands that were still buffered were dropped. The blocking server now
  flushes them (`CacheServerTest.repliesToCompletedCommandsAreSentWhenTheClientClosesInTheMiddleOfTheNextOne`).

## Closing without losing the last reply
Found by your first full run on Windows: the differential test failed on one input (a command of nine 1 MiB arguments,
over the 8 MiB limit). The event-loop server sent `-ERR Protocol error: command too large`; the **blocking server sent
nothing**. The cause is the same as the earlier "max number of clients" failure: closing a socket that still has unread
input from the client makes the operating system send a reset, and on Windows the reset can destroy the reply before
the client reads it. I could not reproduce this on Linux (both servers delivered the error 10 of 10 times in a test
that finishes sending before it reads), so the fix is the standard one. The failing test passed on Windows afterwards (one full run, 622 tests, 0 failures), which is evidence but not proof, since the failure was intermittent in kind:
after the last reply both servers signal end-of-output (`shutdownOutput`) and then discard what the client is still
sending for at most 1 second before closing (a "lingering close"). It applies when the server closes on a protocol error,
on `QUIT` and after too many failed `AUTH` attempts, not to TLS connections. Side effect: a client that sends `QUIT` and
never closes its own end keeps its connection slot for up to 1 second.

## Measurements
Tool: `ServerIoBenchmark` (in the test sources). It starts each server as its own process (`-Xmx256m`, capacity 100,000),
drives it from another process on the same machine, and reports the server's resident memory (after a forced GC) and
thread count (from `jcmd Thread.print`, which includes the JVM's own threads), then closed-loop `GET` throughput and
per-request latency. The load generator shares the machine's CPUs, so only compare the two servers' numbers with each
other, from the same session.

### My Linux sandbox (2 CPUs, 8 GB, JDK 21.0.12). Not your laptop.
Idle connections, each having sent one `PING` (server process only):

| Idle connections | Server | Threads | Extra memory | Per connection |
|---|---|---|---|---|
| 2,000 | thread per connection | 22 to 2,021 | +223.0 MiB | 114 KiB |
| 2,000 | event loop | 23 to 23 | +11.8 MiB | 6.1 KiB |
| 8,000 | thread per connection | 22 to 8,021 | +577.3 MiB | 74 KiB |
| 8,000 | event loop | 23 to 23 | +45.8 MiB | 5.9 KiB |

`GET` of one small key, requests per second (median latency / p99 latency in ms):

| Connections x pipeline depth | Thread per connection | Event loop |
|---|---|---|
| 1 x 1 | 39,475 (0.03 / 0.05) | 39,058 (0.03 / 0.05) |
| 16 x 1 | 113,705 (0.13 / 0.34) | 141,376 (0.12 / 0.33) |
| 64 x 1, three runs | 117,878 / 113,583 / 114,707 (0.51-0.53 / 1.17-1.30) | 151,803 / 153,896 / 152,688 (0.38-0.45 / 2.87-3.45) |
| 256 x 1 | 103,049 (2.37 / 5.55) | 149,089 (1.61 / 5.96) |
| 16 x 16 (pipelined) | 318,253 (0.64 / 2.31) | 286,583 (0.82 / 3.43) |

Reading it honestly:
- The clear result is **idle connections**: no extra threads and about 6 KiB each instead of 74-114 KiB and a thread each.
- Throughput at 16 to 256 connections was 20-45% higher for the event loop in these runs; the 64-connection result repeated
  three times with little spread. With one connection there is no difference, and **with deep pipelining the
  thread-per-connection server was faster** (318k vs 287k, one run each).
- **The event loop's 99th-percentile latency was worse at 64 connections** (about 3.2 ms against 1.2 ms in all three
  runs) and at pipeline depth 16. I did not investigate why; a plausible but untested reason is that connections on one
  loop are served in turn.
- Each cell except the 64 x 1 row is a single run, and the load generator competes with the server for 2 CPUs. A different
  machine, more CPUs or a different load could change the ordering.

### Your laptop (Windows 10, 2 CPUs, JDK 23.0.1), one run of each
Memory here is the working set from `tasklist`, not Linux's resident size, so do not compare it with the table above.

| | Thread per connection | Event loop |
|---|---|---|
| Threads, 0 connections to 2,000 idle | 22 to 2,021 (+1,999) | 23 to 23 (+0) |
| Memory, 0 to 2,000 idle | 40.0 to 218.1 MiB (+178.1, 91 KiB each) | 43.7 to 54.6 MiB (+10.9, 5.6 KiB each) |
| Time to open the 2,000 connections | 2,420 ms | 1,726 ms |
| `GET`, 64 connections x 1 | 19,039 requests/s (median 3.10 ms, p99 7.11 ms) | 17,645 requests/s (median 3.01 ms, p99 9.93 ms) |

Reading it honestly:
- The idle-connection result repeats the Linux one: no extra threads and about 5.6 KiB per connection against 91 KiB.
- **On this laptop the event loop did not show a throughput advantage:** 17,645 against 19,039 requests/s, one run each,
  with 2-second warm-ups of 7,165 and 12,049 requests/s that show how unsteady the machine was. I cannot say from one
  run whether that 7% difference is real or noise, and I did not repeat it. Its p99 latency was again worse
  (9.9 ms against 7.1 ms), as it was on Linux.
- Throughput here is about 8 times lower than in my Linux sandbox for both servers (Windows loopback, a shared 2-CPU
  machine, and a load generator competing for the same CPUs), so the Linux throughput table says nothing about this laptop.
- So the claim this project can make is about **idle connections** (threads and memory). A throughput or latency gain was
  seen on Linux and not on Windows, and the event loop's tail latency was worse on both.

To repeat: `mvn -q clean package -DskipTests`, then
`java -cp target\classes;target\test-classes com.distri.cache.server.ServerIoBenchmark --io nio --idle 2000`
(and `--io blocking`).
