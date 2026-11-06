# Persistence (append-only log)

Start the server with `--aof PATH` to keep data across restarts. Without it, nothing is written to disk.

```bash
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --port 6380 --aof data/cache.aof --fsync everysec
```

## What is written
Every write command is appended to the file in the same wire format the server reads:
`SET key value`, `SET key value PXAT <absolute epoch ms>` (for entries with a TTL) and `DEL key ...`
(only keys that were really removed). On startup the file is replayed into the cache before the
server accepts connections. TTLs are stored as absolute times, so a restart does not extend them, and
entries that expired while the server was down are not restored.

## Options
| Flag | Meaning |
|---|---|
| `--aof PATH` | turn persistence on and use this file |
| `--fsync always` | force every write to disk before replying |
| `--fsync everysec` (default) | force once per second; a crash can lose up to about a second of writes |
| `--fsync no` | leave flushing to the operating system |
| `--aof-compact-bytes N` | compact automatically once the log is N bytes and has doubled since the last compaction (default 64 MiB, `0` = never) |

`REWRITEAOF` compacts on demand. It writes one record per live entry (least recently used first, so
the relative LRU order survives a restart) to a temporary file, forces it to disk, then renames it over the
old log. A crash at any point leaves either the whole old log or the whole new one.

## Behaviour on damage
- A last record cut off by a crash is dropped and the file is truncated to the last whole record. The
  server prints how many bytes were dropped.
- A malformed record anywhere else makes startup fail with the byte offset. The file is left untouched,
  because dropping everything after it would silently lose data.
- There are no checksums, so corruption that still parses as a valid command is not detected.

## Trade-offs and limits
- One lock covers "apply to the cache + append to the log", so the log order is the order the writes took
  effect. With persistence on, writes are serialised; reads are not. Writes also wait while a compaction runs.
- With `--fsync always` each write waits for a disk flush (no group commit).
- Evictions are not logged, and read recency is not logged. After a restart the cache holds the replayed
  writes, subject to capacity, and the order in which entries are evicted may differ from before the restart
  (compaction preserves it).
- If appending fails (disk full, I/O error) the client gets an error, but the change has already been applied
  in memory. The cache and the log can then disagree until the problem is fixed.
- No snapshot format other than the log itself, no background (non-blocking) compaction, and no log shipping.
