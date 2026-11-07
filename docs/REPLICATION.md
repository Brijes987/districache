# Replication (asynchronous, primary to replicas)

A **primary** accepts writes and streams them to one or more read-only **replicas**. It is asynchronous: the primary
answers a client before any replica has the write. Replication here is a way to have extra copies for reading and a
copy to promote by hand. It is **not** a durability guarantee and there is **no automatic failover** yet.

```bash
# primary
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --port 6380 --allow-replicas
# replica
java -cp target/cache-1.0-SNAPSHOT.jar com.distri.cache.server.ServerMain --port 6381 --replicaof 127.0.0.1:6380
```
Roles can also be changed at run time (the server must have been started with `--allow-replicas` or `--replicaof`):
`REPLICAOF host port` makes the server a replica of that primary (its own data is replaced by a full copy),
`REPLICAOF NO ONE` promotes a replica to a primary. `REPLINFO` prints the role, replication id, stream offset,
and, on a primary, each replica's acknowledged offset and lag in bytes.

If the primary needs a password or TLS, give the replica `--primary-password-file` (or `DISTRICACHE_PRIMARY_PASSWORD`)
and `--primary-truststore` with `--primary-truststore-password-file` (or `DISTRICACHE_TRUSTSTORE_PASSWORD`). For TLS the
replica must connect by the host name in the primary's certificate. The link uses the same checks as any client.

## How it works
- Every write the primary applies is also appended to a numbered stream: the same `SET`/`DEL` records as the
  append-only log, with `PING` as a heartbeat. The stream **offset** is the number of bytes published so far.
- A replica connects like a client (authenticating if needed) and sends `PSYNC <replication id> <offset>`.
- If the id is the primary's current one and the offset is still inside the primary's **backlog** (the most recent
  16 MiB by default, `--repl-backlog-bytes`), the primary answers `+CONTINUE` and sends everything after that offset.
- Otherwise it answers `+FULLRESYNC`: a copy of the cache taken while writes are paused, together with the stream
  offset of that instant, followed by every write after it. The replica clears its cache, loads the copy, then
  applies the stream.
- Replicas acknowledge their offset once a second. A replica that falls out of the backlog is disconnected and does a
  full resync when it returns. A primary drops a replica that sends no acknowledgement for 60 seconds; a replica
  treats 10 seconds without any data (heartbeats arrive every second) as a dead primary and reconnects with a growing
  delay, up to 10 seconds. While disconnected a replica keeps serving what it has.
- Expiry times travel as absolute times (`PXAT`), so a TTL is not stretched by replication delay.

## What you can rely on
- A replica applies the primary's writes in the order the primary applied them (they are serialised by one lock).
- After a full resync the replica's contents equal the primary's at one instant, then follow it.
- Replicas refuse client writes (`-READONLY`). `PSYNC` needs authentication like any other command.
- Promotion starts a new history (a new replication id), so other replicas must do a full resync from the new primary.

## What you cannot rely on
- **Lost writes.** A write acknowledged to a client can be missing on every replica if the primary dies first. When
  an old primary rejoins as a replica, writes it never replicated are discarded.
- **Stale reads.** A replica can lag; reads from it can return old values or miss recent writes.
- **No fencing, no election.** Nothing stops two servers both acting as primary (for example a promoted replica and an
  old primary that was only unreachable). Choosing the new primary is up to you or to Phase 6.
- **Eviction is not replicated.** Reads do not reorder a replica's LRU list, and a replica evicts on its own when full,
  so when the cache is at capacity a replica can hold different entries than the primary (including entries the
  primary has already evicted).
- **Clocks.** Expiry uses wall-clock times on both servers; clock skew makes replica entries expire early or late.
- **Full resync is not atomic for readers.** The replica clears its cache before loading, so reads during a large
  resync see incomplete data. The primary copies the whole cache into memory for each full resync, and writes pause
  for the duration of that copy.
- **Writes are serialised** while replication (or persistence) is on, which limits write throughput. No numbers have
  been measured for this.
- A restarted replica does not remember its offset, so it always does a full resync; a restarted primary has a new
  replication id, so all its replicas do too.
- No replica chaining (a replica refuses `PSYNC`), at most 16 replicas per primary, and no `WAIT`-style
  acknowledgement of writes by replicas.
- This is not Redis's replication protocol; ordinary Redis replicas cannot attach to it.

## Tests
`ReplicationTest` runs real servers on real sockets: full and partial resync, backlog overflow, writes during a
resync, primary death and a new primary on the same address, read-only replicas, promotion, password and TLS on the
link, and a replica with its own append-only log.
