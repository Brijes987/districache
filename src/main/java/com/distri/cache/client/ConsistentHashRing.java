package com.distri.cache.client;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A consistent-hash ring with virtual nodes.
 *
 * <p>Each node id is hashed to many points on a 64-bit ring. A key belongs to the first point at or after
 * its own hash (wrapping around). When a node is added only the keys that now fall between its points
 * and their predecessors move, about {@code 1 / (n + 1)} of all keys; with {@code hash % n} nearly all
 * keys would move. Virtual nodes smooth out how evenly keys spread across nodes.
 *
 * <p>The hash is MD5 truncated to 64 bits. It is used for its stability across JVMs and its even
 * spread, not for security. Reads are lock-free (the ring is an immutable snapshot that writers replace).
 */
public final class ConsistentHashRing {

    public static final int DEFAULT_VIRTUAL_NODES = 160;

    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is required by the JDK", e);
        }
    });

    private final int virtualNodes;
    private final Set<String> nodeIds = new TreeSet<>(); // guarded by this
    private volatile NavigableMap<Long, String> ring = Collections.emptyNavigableMap();

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        this.virtualNodes = virtualNodes;
    }

    /** @return true if the node was not already present */
    public synchronized boolean add(String nodeId) {
        requireId(nodeId);
        if (!nodeIds.add(nodeId)) {
            return false;
        }
        rebuild();
        return true;
    }

    /** @return true if the node was present */
    public synchronized boolean remove(String nodeId) {
        requireId(nodeId);
        if (!nodeIds.remove(nodeId)) {
            return false;
        }
        rebuild();
        return true;
    }

    /**
     * @return the id of the node responsible for {@code key}
     * @throws IllegalStateException if the ring has no nodes
     */
    public String nodeFor(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        NavigableMap<Long, String> snapshot = ring;
        if (snapshot.isEmpty()) {
            throw new IllegalStateException("the ring has no nodes");
        }
        Map.Entry<Long, String> entry = snapshot.ceilingEntry(hash(key));
        return (entry != null ? entry : snapshot.firstEntry()).getValue();
    }

    public synchronized Set<String> nodes() {
        return Collections.unmodifiableSet(new TreeSet<>(nodeIds));
    }

    public synchronized int size() {
        return nodeIds.size();
    }

    private void rebuild() {
        // Rebuilt from scratch in sorted order, so the same set of nodes always gives the same ring,
        // whatever order they were added in.
        NavigableMap<Long, String> next = new TreeMap<>();
        for (String id : nodeIds) {
            for (int i = 0; i < virtualNodes; i++) {
                next.putIfAbsent(hash(id + "#" + i), id);
            }
        }
        ring = next;
    }

    static long hash(String s) {
        byte[] digest = MD5.get().digest(s.getBytes(StandardCharsets.UTF_8));
        long h = 0;
        for (int i = 0; i < 8; i++) {
            h = (h << 8) | (digest[i] & 0xFF);
        }
        return h;
    }

    private static void requireId(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("nodeId cannot be empty");
        }
    }
}
