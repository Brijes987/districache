package com.distri.cache.client;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spreads keys over several cache servers with a {@link ConsistentHashRing}. Each key always goes to the
 * same server while the set of servers is unchanged.
 *
 * <p>What this does <b>not</b> do: it does not copy data when servers join or leave (keys whose owner
 * changes simply miss, which a cache tolerates), it does not replicate, and it does not fail over: a
 * request routed to a dead server throws {@link CacheClientException}. A server that rejoins may still
 * hold old values for keys that moved away and back.
 *
 * <p>Node addresses are {@code host:port} (IPv4 or a host name).
 */
public final class ClusterClient implements AutoCloseable {

    private final ConsistentHashRing ring;
    private final ConcurrentHashMap<String, CacheClient> clients = new ConcurrentHashMap<>();
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final ClientSecurity security;
    private volatile boolean closed;

    public ClusterClient(Collection<String> nodes) {
        this(nodes, ConsistentHashRing.DEFAULT_VIRTUAL_NODES,
                CacheClient.DEFAULT_CONNECT_TIMEOUT_MILLIS, CacheClient.DEFAULT_READ_TIMEOUT_MILLIS);
    }

    public ClusterClient(Collection<String> nodes, int virtualNodes, int connectTimeoutMillis, int readTimeoutMillis) {
        this(nodes, virtualNodes, connectTimeoutMillis, readTimeoutMillis, ClientSecurity.NONE);
    }

    /** Every server must use the same password and TLS trust settings. */
    public ClusterClient(Collection<String> nodes, int virtualNodes, int connectTimeoutMillis, int readTimeoutMillis,
            ClientSecurity security) {
        if (security == null) {
            throw new IllegalArgumentException("security cannot be null (use ClientSecurity.NONE)");
        }
        this.security = security;
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("at least one node is required");
        }
        this.ring = new ConsistentHashRing(virtualNodes);
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        for (String node : nodes) {
            addNode(node);
        }
    }

    public Optional<String> get(String key) {
        return clientFor(key).get(key);
    }

    public void set(String key, String value) {
        clientFor(key).set(key, value);
    }

    public void set(String key, String value, long ttlMillis) {
        clientFor(key).set(key, value, ttlMillis);
    }

    public boolean delete(String key) {
        return clientFor(key).delete(key);
    }

    /** Sum of the live entries on all servers; fails if any server cannot be reached. */
    public long dbSize() {
        long total = 0;
        for (String node : ring.nodes()) {
            total += client(node).dbSize();
        }
        return total;
    }

    /** The node that owns {@code key} right now (for diagnostics and tests). */
    public String nodeFor(String key) {
        return ring.nodeFor(key);
    }

    public Set<String> nodes() {
        return ring.nodes();
    }

    /** Adds a server; about 1/(n+1) of the keys will now be routed to it and start empty. */
    public void addNode(String address) {
        parse(address); // validates the format before anything changes
        clients.computeIfAbsent(address, a -> {
            HostPort hp = parse(a);
            return new CacheClient(hp.host, hp.port, connectTimeoutMillis, readTimeoutMillis, security);
        });
        ring.add(address);
    }

    /** Removes a server; the keys it owned are now routed to other servers and will miss until rewritten. */
    public void removeNode(String address) {
        if (ring.size() == 1 && ring.nodes().contains(address)) {
            throw new IllegalStateException("cannot remove the last node");
        }
        if (ring.remove(address)) {
            CacheClient removed = clients.remove(address);
            if (removed != null) {
                removed.close();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        for (CacheClient c : clients.values()) {
            c.close();
        }
        clients.clear();
    }

    private CacheClient clientFor(String key) {
        return client(ring.nodeFor(key));
    }

    private CacheClient client(String node) {
        if (closed) {
            throw new IllegalStateException("cluster client is closed");
        }
        CacheClient c = clients.get(node);
        if (c == null) {
            throw new CacheClientException("node " + node + " was removed while the request was being routed");
        }
        return c;
    }

    private record HostPort(String host, int port) {}

    private static HostPort parse(String address) {
        if (address == null) {
            throw new IllegalArgumentException("node address cannot be null");
        }
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("node address must look like host:port, got '" + address + "'");
        }
        try {
            int port = Integer.parseInt(address.substring(colon + 1));
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("port out of range in '" + address + "'");
            }
            return new HostPort(address.substring(0, colon), port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("node address must look like host:port, got '" + address + "'");
        }
    }

    /** As {@link #of(String...)}, with a password and/or TLS used for every server. */
    public static ClusterClient of(ClientSecurity security, String... nodes) {
        return new ClusterClient(List.of(nodes), ConsistentHashRing.DEFAULT_VIRTUAL_NODES,
                CacheClient.DEFAULT_CONNECT_TIMEOUT_MILLIS, CacheClient.DEFAULT_READ_TIMEOUT_MILLIS, security);
    }

    /** Convenience for tests and demos. */
    public static ClusterClient of(String... nodes) {
        return new ClusterClient(List.of(nodes));
    }
}
