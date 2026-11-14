package com.distri.cache.node;

import com.distri.cache.metrics.LatencyHistogram;
import com.distri.cache.metrics.MetricsRegistry;
import com.distri.cache.raft.RaftNode;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics of one replicated cache server: its Raft state and the client requests it handled.
 *
 * <p>Request latency here is the time from a decoded request to its reply being ready, so for a write on the leader
 * it includes the whole Raft round trip (disk sync on this server, replication to a majority, apply). Reads go
 * through the Raft log too, so they cost the same as writes. {@code outcome} is {@code ok}, {@code not_leader},
 * {@code unknown} (the request may or may not have taken effect), {@code stale} or {@code error}.
 */
final class NodeMetrics {

    private final LongAdder[][] requests = new LongAdder[4][5]; // [op][status]
    private final LongAdder hits;
    private final LongAdder misses;
    private final LatencyHistogram latency;

    private static final String[] OPS = {"unknown", "set", "del", "get"};
    private static final String[] OUTCOMES = {"ok", "not_leader", "unknown", "stale", "error"};

    NodeMetrics(MetricsRegistry registry, ReplicatedServer server) {
        for (int op = 1; op < OPS.length; op++) {
            for (int status = 0; status < OUTCOMES.length; status++) {
                requests[op][status] = registry.counter("districache_raft_client_requests_total",
                        "Client requests handled by this server, by operation and outcome.",
                        "op", OPS[op], "outcome", OUTCOMES[status]);
            }
        }
        hits = registry.counter("districache_cache_hits_total", "GET requests that found a live value.");
        misses = registry.counter("districache_cache_misses_total", "GET requests that found nothing.");
        latency = registry.histogram("districache_raft_client_request_duration_seconds",
                "Time to answer a client request on this server, including the Raft commit for the leader.");
        registry.quantileGauges("districache_raft_client_request_duration", "Estimated request latency percentile (bucket upper edge).",
                latency, 0.5, 0.99, 0.999);
        registry.gauge("districache_cache_hit_ratio", "hits / (hits + misses) since start; NaN before the first GET.", () -> {
            double h = hits.sum();
            double m = misses.sum();
            return h + m == 0 ? Double.NaN : h / (h + m);
        });

        registry.gauge("districache_raft_node_id", "This server's id.", server::id);
        registry.gauge("districache_raft_term", "Current Raft term.", () -> with(server, NodeRuntime::term));
        registry.gauge("districache_raft_is_leader", "1 when this server believes it is the leader.",
                () -> with(server, rt -> rt.role() == RaftNode.Role.LEADER ? 1 : 0));
        for (RaftNode.Role role : RaftNode.Role.values()) {
            registry.gauge("districache_raft_role", "1 for this server's current role, 0 for the others.",
                    () -> with(server, rt -> rt.role() == role ? 1 : 0), "role", role.name().toLowerCase());
        }
        registry.gauge("districache_raft_leader_id", "Id of the leader this server knows of, or -1.",
                () -> with(server, NodeRuntime::leaderId));
        registry.gauge("districache_raft_commit_index", "Highest log index known to be committed.",
                () -> with(server, NodeRuntime::commitIndex));
        registry.gauge("districache_raft_last_log_index", "Index of the last entry in this server's log.",
                () -> with(server, NodeRuntime::lastLogIndex));
        registry.gauge("districache_raft_last_applied_index", "Highest log index applied to the cache.",
                () -> with(server, NodeRuntime::lastApplied));
        registry.gauge("districache_raft_commit_lag_entries",
                "last_log_index - commit_index: entries in the log not yet known to be committed.",
                () -> with(server, rt -> rt.lastLogIndex() - rt.commitIndex()));
        registry.gauge("districache_raft_apply_lag_entries",
                "commit_index - last_applied_index: committed entries not yet applied to the cache.",
                () -> with(server, rt -> rt.commitIndex() - rt.lastApplied()));
        registry.gauge("districache_raft_leader_changes", "Times this server saw its known leader change since start.",
                () -> with(server, NodeRuntime::leaderChanges));
        registry.gauge("districache_raft_times_became_leader", "Times this server became leader since start.",
                () -> with(server, NodeRuntime::timesLeader));
        registry.gauge("districache_raft_snapshot_index",
                "Log index covered by this server's latest snapshot (entries up to it were dropped from the log); 0 for none.",
                () -> with(server, NodeRuntime::snapshotIndex));
        registry.gauge("districache_raft_log_entries",
                "Entries held in this server's log: last_log_index - snapshot_index. Stays bounded when snapshots are on.",
                () -> with(server, rt -> rt.lastLogIndex() - rt.snapshotIndex()));
        registry.gauge("districache_raft_snapshots_taken", "Snapshots this server took itself since start; only grows.",
                () -> with(server, NodeRuntime::snapshotsTaken));
        registry.gauge("districache_raft_snapshots_installed",
                "Snapshots this server received from a leader and installed since start; only grows.",
                () -> with(server, NodeRuntime::snapshotsInstalled));
        registry.gauge("districache_raft_snapshots_skipped",
                "Times a snapshot was due but the state was too large (over 128 MiB) to take; only grows.",
                () -> with(server, NodeRuntime::snapshotsSkipped));
        registry.gauge("districache_raft_snapshot_chunks_sent",
                "Snapshot chunks this server sent as leader, retransmissions included.",
                () -> with(server, NodeRuntime::snapshotChunksSent));
        registry.gauge("districache_raft_snapshot_chunks_received",
                "Snapshot chunks this server accepted as the next piece of a snapshot it is receiving.",
                () -> with(server, NodeRuntime::snapshotChunksReceived));
        registry.gauge("districache_raft_reads_served",
                "Read-index reads answered by this server since it started (0 unless --read-mode read-index).",
                () -> with(server, NodeRuntime::readsServed));
        registry.gauge("districache_raft_reads_pending",
                "Read-index reads waiting for a heartbeat round or for the state machine to catch up.",
                () -> with(server, NodeRuntime::pendingReads));
        registry.gauge("districache_raft_proposal_batches",
                "Log appends made by this server as leader for client requests (each is one fsync); only grows. "
                        + "Needs group commit (--max-batch above 1), otherwise stays 0.",
                () -> with(server, NodeRuntime::proposalBatches));
        registry.gauge("districache_raft_proposed_entries",
                "Client requests written to the log through those batches; proposed_entries / proposal_batches is the "
                        + "mean batch size.",
                () -> with(server, NodeRuntime::proposedEntries));
        registry.gauge("districache_raft_failed", "1 if the node stopped itself after an internal error (fail-stop).",
                () -> server.failed() ? 1 : 0);
    }

    private static double with(ReplicatedServer server, java.util.function.ToDoubleFunction<NodeRuntime> read) {
        NodeRuntime rt = server.runtimeForMetrics();
        return rt == null ? Double.NaN : read.applyAsDouble(rt);
    }

    void record(ClientProtocol.Request request, ClientProtocol.Response response, long nanos) {
        int op = request.op() >= 1 && request.op() <= 3 ? request.op() : 0;
        int status = response.status() >= 0 && response.status() < OUTCOMES.length ? response.status() : 4;
        if (op != 0) {
            requests[op][status].increment();
        }
        if (op == ClientProtocol.GET && response.status() == ClientProtocol.OK) {
            (response.found() ? hits : misses).increment();
        }
        latency.recordNanos(nanos);
    }
}
