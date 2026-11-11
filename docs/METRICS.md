# Metrics

Both server modes can publish their numbers in the Prometheus text format at `http://HOST:PORT/metrics`.
There are no new dependencies: the registry, the histogram and the HTTP endpoint are small classes in
`com.distri.cache.metrics`, and the endpoint uses the JDK's built-in `HttpServer`.

Metrics are **off by default**. Without `--metrics-port` a server records nothing.

## Turning it on

```
java -cp target/classes com.distri.cache.server.ServerMain --port 6380 --metrics-port 9380
java -cp target/classes com.distri.cache.node.ReplicatedServerMain --id 0 --members ... --data ... --metrics-port 9380
curl http://127.0.0.1:9380/metrics
curl http://127.0.0.1:9380/healthz
```

The endpoint binds to `127.0.0.1` unless `--metrics-host` says otherwise. It has **no authentication and no TLS**,
and it shows operational detail, so put it behind a firewall or a reverse proxy before exposing it.
Only `GET` and `HEAD` are accepted.

## What is published

### Single server (`ServerMain`)

| Metric | Type | Meaning |
|---|---|---|
| `districache_commands_total{command}` | counter | Commands processed. Unknown commands are counted as `OTHER`, so a client cannot create new series. |
| `districache_cache_hits_total`, `..._misses_total` | counter | `GET` results. |
| `districache_cache_hit_ratio` | gauge | hits / (hits + misses) since start; `NaN` before the first `GET`. |
| `districache_command_duration_seconds` | histogram | Server-side time to execute a command (see below). |
| `districache_command_duration_estimated_seconds{quantile}` | gauge | Estimated p50, p99 and p99.9 (see below). |
| `districache_cache_entries`, `districache_cache_capacity` | gauge | Live entries and configured capacity. |
| `districache_connections` | gauge | Open client connections. |
| `districache_connections_accepted_total`, `..._rejected_total` | counter | Accepted, and refused because `--max-clients` was reached. |
| `districache_auth_failures_total` | counter | `AUTH` with a wrong password. |
| `districache_aof_size_bytes` | gauge | Append-only log size (only with `--aof`). |
| `districache_replication_*` | gauge | Role, stream offset, attached replicas, link state, full and partial syncs (only with replication on). |

### Replicated server (`ReplicatedServerMain`)

| Metric | Type | Meaning |
|---|---|---|
| `districache_raft_term` | gauge | Current term. |
| `districache_raft_is_leader`, `districache_raft_role{role}` | gauge | Whether this server believes it leads. |
| `districache_raft_leader_id` | gauge | Leader this server knows of, or -1. |
| `districache_raft_commit_index`, `..._last_log_index`, `..._last_applied_index` | gauge | Log positions. |
| `districache_raft_commit_lag_entries` | gauge | `last_log_index - commit_index`: entries in the log not yet known to be committed. |
| `districache_raft_apply_lag_entries` | gauge | `commit_index - last_applied_index`. |
| `districache_raft_proposal_batches`, `..._proposed_entries` | gauge | Group commit on the leader: log appends made for client requests, and the requests they carried (monotonic; mean batch = entries / batches). Both stay 0 with `--max-batch 1`. |
| `districache_raft_leader_changes`, `..._times_became_leader` | gauge | Counts since this process started (gauges, because they are read from the node's published snapshot). |
| `districache_raft_failed` | gauge | 1 if the node stopped itself after an internal error (fail-stop). |
| `districache_raft_client_requests_total{op,outcome}` | counter | Client requests by `set`/`del`/`get` and `ok`/`not_leader`/`unknown`/`stale`/`error`. |
| `districache_raft_client_request_duration_seconds` | histogram | Time to answer a client request, including the Raft commit on the leader. |
| `districache_cache_hits_total`, `..._misses_total`, `..._hit_ratio` | counter/gauge | `GET` results (reads go through the log). |

The Raft gauges read a snapshot that the node's own thread publishes after every step, so a scrape never touches
the Raft node (which is not thread-safe). They can be a few milliseconds behind.

## What the numbers do and do not mean

- **Latency is server-side processing time.** On the single server it runs from "command parsed" to "reply
  buffered": it includes waiting for the write lock and, with `--fsync always`, the disk sync, but **not** reading
  the request from the socket or flushing the reply. It is not what a client sees. On the replicated server it covers
  a whole request including the Raft round trip.
- **Percentiles are estimates.** The histogram keeps only bucket counts (edges at 5, 10, 25, 50, 100, 250, 500
  microseconds, then 1, 2.5, 5, 10, 25, 50, 100, 250, 500 milliseconds, then 1, 2.5, 5 and 10 seconds). The
  `..._estimated_seconds` gauges report the **upper edge** of the bucket holding the requested rank: never below the
  true value, at most one bucket above it. A result of `+Inf` means the rank fell beyond 10 seconds. With Prometheus,
  `histogram_quantile(0.99, rate(districache_command_duration_seconds_bucket[1m]))` is the usual way.
- **Scrapes are not atomic.** A scrape that overlaps heavy traffic can be off by the few samples recorded during it.
- **Not measured:** the overhead of recording. It is one `System.nanoTime()` pair and one lock-free counter update
  per command, but no benchmark has been run on this, so no figure is claimed here.
- **Not published:** evictions (the cache does not count them), and Raft log compaction (the Raft log is never
  compacted yet, so there is nothing to report).
- The output format was checked by this project's tests (a line-by-line format check) and by `curl`; it has not been
  scraped by a real Prometheus server.

## Prometheus example

```yaml
scrape_configs:
  - job_name: districache
    static_configs:
      - targets: ['127.0.0.1:9380']
```

Useful queries:

```
districache_cache_hit_ratio
histogram_quantile(0.99, sum(rate(districache_command_duration_seconds_bucket[1m])) by (le))
sum(rate(districache_commands_total[1m])) by (command)
max(districache_raft_term)                        # a term that keeps rising means repeated elections
sum(districache_raft_is_leader)                   # should be 1 in a healthy group
districache_raft_commit_lag_entries > 0           # sustained non-zero: the group is not committing
```

## Tests

`LatencyHistogramTest`, `MetricsRegistryTest`, `MetricsServerTest` (unit and HTTP), `ServerMetricsTest` (counts
compared with what a real client sent over a real socket) and `NodeMetricsTest` (a real three-server group: exactly
one leader, request counts, a higher term and a new leader after the leader is killed, and a positive commit lag
while a cut-off leader holds an entry it cannot commit).
