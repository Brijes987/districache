package com.distri.cache.server;

import com.distri.cache.metrics.LatencyHistogram;
import com.distri.cache.metrics.MetricsRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * The counters and the latency histogram a {@link CacheServer} updates while it serves commands.
 *
 * <p><b>What the latency covers:</b> from the moment a command has been parsed until {@code execute} returns. That
 * includes waiting for the write coordinator's lock and, for writes with {@code --fsync always}, the disk sync.
 * It does <i>not</i> include reading the request from the socket or flushing the reply, so it is the server's
 * processing time, not what a client sees. Pipelined commands are timed one by one.
 */
final class ServerMetrics {

    /** Command names that get their own label; anything else is counted as OTHER so clients cannot create series. */
    private static final String[] KNOWN = {
        "PING", "ECHO", "GET", "SET", "DEL", "DBSIZE", "QUIT", "AUTH", "REWRITEAOF", "REPLICAOF", "REPLINFO"
    };

    final MetricsRegistry registry;
    final LatencyHistogram latency;
    final LongAdder hits;
    final LongAdder misses;
    final LongAdder authFailures;
    final LongAdder connectionsAccepted;
    final LongAdder connectionsRejected;
    private final Map<String, LongAdder> commands = new HashMap<>(); // read-only after construction
    private final LongAdder otherCommands;

    ServerMetrics(MetricsRegistry registry) {
        this.registry = registry;
        for (String name : KNOWN) {
            commands.put(name, registry.counter("districache_commands_total", "Commands processed, by command name.",
                    "command", name));
        }
        otherCommands = registry.counter("districache_commands_total", "Commands processed, by command name.",
                "command", "OTHER");
        hits = registry.counter("districache_cache_hits_total", "GET commands that found a live value.");
        misses = registry.counter("districache_cache_misses_total", "GET commands that found nothing (absent or expired).");
        authFailures = registry.counter("districache_auth_failures_total", "AUTH attempts with a wrong password.");
        connectionsAccepted = registry.counter("districache_connections_accepted_total", "Client connections accepted.");
        connectionsRejected = registry.counter("districache_connections_rejected_total",
                "Client connections refused because the client limit was reached.");
        latency = registry.histogram("districache_command_duration_seconds",
                "Server-side time to execute a command (excludes socket read and reply flush).");
        registry.quantileGauges("districache_command_duration", "Estimated command latency percentile (bucket upper edge).",
                latency, 0.5, 0.99, 0.999);
        registry.gauge("districache_cache_hit_ratio",
                "hits / (hits + misses) since the server started; NaN before the first GET.",
                () -> {
                    double h = hits.sum();
                    double m = misses.sum();
                    return h + m == 0 ? Double.NaN : h / (h + m);
                });
    }

    /** Counts a finished command and records how long it took. */
    void record(String upperCaseCommand, long nanos) {
        commands.getOrDefault(upperCaseCommand, otherCommands).increment();
        latency.recordNanos(nanos);
    }
}
