package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.client.ClientSecurity;
import java.util.ArrayList;
import java.util.List;

/**
 * A server's replication role. A server starts as a primary (it can accept replicas) or as a replica of another
 * server, and can be switched at run time with {@code REPLICAOF host port} and {@code REPLICAOF NO ONE}.
 *
 * <p>A replica is read-only and does not accept replicas of its own (no chaining). Promoting a replica starts a
 * new history with a new replication id, so every other replica must do a full resync from it.
 */
final class Replication implements AutoCloseable {

    /** Tunables; the defaults suit real use, tests shorten them. */
    record Config(long backlogBytes, long heartbeatMillis, long ackTimeoutMillis, int linkConnectTimeoutMillis,
            int linkReadTimeoutMillis, long linkAckIntervalMillis, long linkMaxBackoffMillis,
            ClientSecurity linkSecurity) {

        static Config defaults() {
            return new Config(ReplicationHub.DEFAULT_BACKLOG_BYTES, ReplicationHub.DEFAULT_HEARTBEAT_MILLIS,
                    ReplicationHub.DEFAULT_ACK_TIMEOUT_MILLIS, 3_000, 10_000, 1_000, 10_000, ClientSecurity.NONE);
        }

        Config withBacklog(long bytes) {
            return new Config(bytes, heartbeatMillis, ackTimeoutMillis, linkConnectTimeoutMillis,
                    linkReadTimeoutMillis, linkAckIntervalMillis, linkMaxBackoffMillis, linkSecurity);
        }

        Config withSecurity(ClientSecurity security) {
            return new Config(backlogBytes, heartbeatMillis, ackTimeoutMillis, linkConnectTimeoutMillis,
                    linkReadTimeoutMillis, linkAckIntervalMillis, linkMaxBackoffMillis, security);
        }

        /** For tests: short timers so failures are noticed in well under a second. */
        static Config fast() {
            return new Config(ReplicationHub.DEFAULT_BACKLOG_BYTES, 100, 5_000, 1_000, 1_500, 100, 300,
                    ClientSecurity.NONE);
        }
    }

    private final ShardedLRUCache<String, String> cache;
    private final Config config;
    private final ReplicationHub hub;
    private final WriteCoordinator coordinator;
    private volatile ReplicaLink link; // non-null while this server is a replica

    Replication(ShardedLRUCache<String, String> cache, AppendOnlyLog log, Config config) {
        this.cache = cache;
        this.config = config;
        this.hub = new ReplicationHub(config.backlogBytes(), config.heartbeatMillis(), config.ackTimeoutMillis());
        this.coordinator = new WriteCoordinator(cache, log, hub);
        hub.attach(coordinator);
    }

    WriteCoordinator coordinator() {
        return coordinator;
    }

    ReplicationHub hub() {
        return hub;
    }

    boolean isReplica() {
        return link != null;
    }

    ReplicaLink link() {
        return link;
    }

    /** Becomes a replica of {@code host:port} (replacing any current primary). */
    synchronized void replicaOf(String host, int port) {
        ReplicaLink old = link;
        if (old != null) {
            old.close();
        }
        // Whatever this server was, its own stream history ends here; its replicas are disconnected.
        hub.startNewHistory(0, false);
        ReplicaLink next = new ReplicaLink(host, port, config.linkSecurity(), cache, coordinator,
                config.linkConnectTimeoutMillis(), config.linkReadTimeoutMillis(), config.linkAckIntervalMillis(),
                config.linkMaxBackoffMillis());
        link = next;
        next.start();
    }

    /** Stops replicating and starts accepting writes, with a new replication id. */
    synchronized void promote() {
        ReplicaLink old = link;
        if (old == null) {
            return; // already a primary
        }
        long applied = Math.max(old.offset(), 0);
        old.close();
        link = null;
        coordinator.lock();
        try {
            hub.startNewHistory(applied, true);
        } finally {
            coordinator.unlock();
        }
    }

    /** Text for REPLINFO: one {@code key:value} per line. */
    String info() {
        List<String> lines = new ArrayList<>();
        ReplicaLink l = link;
        if (l == null) {
            lines.add("role:primary");
            lines.add("replid:" + hub.replid());
            lines.add("offset:" + hub.offset());
            lines.add("backlog_bytes:" + hub.backlogBytes());
            lines.add("replicas:" + hub.replicaCount());
            lines.add("full_syncs:" + hub.fullSyncs());
            lines.add("partial_syncs:" + hub.partialSyncs());
            lines.addAll(hub.describeReplicas());
        } else {
            lines.add("role:replica");
            lines.add("primary:" + l.primaryAddress());
            lines.add("link:" + (l.isConnected() ? "up" : "down"));
            lines.add("replid:" + (l.replid() == null ? "none" : l.replid()));
            lines.add("offset:" + l.offset());
            lines.add("last_io_ms_ago:" + l.lastIoAgeMillis());
            lines.add("full_syncs:" + l.fullSyncs());
            lines.add("partial_syncs:" + l.partialSyncs());
            if (!l.lastError().isEmpty()) {
                lines.add("last_error:" + l.lastError().replace('\r', ' ').replace('\n', ' '));
            }
        }
        return String.join("\n", lines) + "\n";
    }

    @Override
    public synchronized void close() {
        ReplicaLink l = link;
        if (l != null) {
            l.close();
        }
        hub.close();
    }
}
