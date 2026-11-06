package com.distri.cache.client;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.server.CacheServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** Runs several real servers in this JVM and routes between them. */
class ClusterClientTest {

    private final List<CacheServer> servers = new ArrayList<>();

    private List<String> startServers(int n) throws IOException {
        List<String> addresses = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            CacheServer s = new CacheServer(new ShardedLRUCache<>(10_000, 4, 0), CacheServer.DEFAULT_HOST, 0, 16).start();
            servers.add(s);
            addresses.add("127.0.0.1:" + s.port());
        }
        return addresses;
    }

    private void stopAll() {
        for (CacheServer s : servers) s.close();
        servers.clear();
    }

    @Test
    void everyKeyIsReadableAndSpreadAcrossAllServers() throws Exception {
        try {
            List<String> addresses = startServers(3);
            try (ClusterClient cluster = new ClusterClient(addresses)) {
                int keys = 600;
                for (int k = 0; k < keys; k++) cluster.set("key-" + k, "v" + k);
                for (int k = 0; k < keys; k++) {
                    assertEquals("v" + k, cluster.get("key-" + k).orElseThrow());
                }
                assertEquals(keys, cluster.dbSize());
                // each server must hold a real share, and the shares must add up
                long sum = 0;
                for (String address : addresses) {
                    String[] hp = address.split(":");
                    try (CacheClient direct = new CacheClient(hp[0], Integer.parseInt(hp[1]))) {
                        long size = direct.dbSize();
                        assertTrue(size > keys / 6, "server " + address + " holds only " + size);
                        sum += size;
                    }
                }
                assertEquals(keys, sum);
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void aKeyAlwaysLandsOnTheServerTheRingNames() throws Exception {
        try {
            List<String> addresses = startServers(3);
            try (ClusterClient cluster = new ClusterClient(addresses)) {
                for (int k = 0; k < 50; k++) {
                    String key = "key-" + k;
                    cluster.set(key, "v");
                    String owner = cluster.nodeFor(key);
                    String[] hp = owner.split(":");
                    try (CacheClient direct = new CacheClient(hp[0], Integer.parseInt(hp[1]))) {
                        assertEquals("v", direct.get(key).orElseThrow(), "key " + key + " is not on " + owner);
                    }
                }
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void removingANodeOnlyLosesTheKeysItOwned() throws Exception {
        try {
            List<String> addresses = startServers(3);
            try (ClusterClient cluster = new ClusterClient(addresses)) {
                int keys = 600;
                Map<String, String> owner = new ConcurrentHashMap<>();
                for (int k = 0; k < keys; k++) {
                    cluster.set("key-" + k, "v" + k);
                    owner.put("key-" + k, cluster.nodeFor("key-" + k));
                }
                String victim = addresses.get(1);
                cluster.removeNode(victim);
                int stillThere = 0;
                for (int k = 0; k < keys; k++) {
                    boolean present = cluster.get("key-" + k).isPresent();
                    if (owner.get("key-" + k).equals(victim)) {
                        assertFalse(present, "a key owned by the removed node cannot still be found elsewhere");
                    } else {
                        assertTrue(present, "key-" + k + " lived on a surviving node and must still be there");
                        stillThere++;
                    }
                }
                assertTrue(stillThere > keys / 2, "only " + stillThere + " keys survived the removal of one of three nodes");
                assertEquals(2, cluster.nodes().size());
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void addingANodeRoutesSomeKeysToItAndTheyStartEmpty() throws Exception {
        try {
            List<String> addresses = startServers(3);
            try (ClusterClient cluster = new ClusterClient(addresses.subList(0, 2))) {
                int keys = 600;
                for (int k = 0; k < keys; k++) cluster.set("key-" + k, "v" + k);
                cluster.addNode(addresses.get(2));
                int moved = 0;
                for (int k = 0; k < keys; k++) {
                    if (cluster.nodeFor("key-" + k).equals(addresses.get(2))) {
                        moved++;
                        assertTrue(cluster.get("key-" + k).isEmpty(), "no data is migrated; moved keys start empty");
                    }
                }
                assertTrue(moved > keys / 6 && moved < keys / 2, "moved " + moved + " of " + keys);
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void aDeadServerFailsOnlyItsOwnKeys() throws Exception {
        try {
            List<String> addresses = startServers(3);
            try (ClusterClient cluster = new ClusterClient(addresses, ConsistentHashRing.DEFAULT_VIRTUAL_NODES, 500, 1000)) {
                int keys = 300;
                for (int k = 0; k < keys; k++) cluster.set("key-" + k, "v");
                String dead = addresses.get(0);
                servers.get(0).close();
                int ok = 0;
                int failed = 0;
                for (int k = 0; k < keys; k++) {
                    String key = "key-" + k;
                    if (cluster.nodeFor(key).equals(dead)) {
                        assertThrows(CacheClientException.class, () -> cluster.get(key));
                        failed++;
                    } else {
                        assertEquals("v", cluster.get(key).orElseThrow());
                        ok++;
                    }
                }
                assertTrue(ok > 0 && failed > 0);
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void ttlAndDeleteWorkThroughTheCluster() throws Exception {
        try {
            List<String> addresses = startServers(2);
            try (ClusterClient cluster = new ClusterClient(addresses)) {
                cluster.set("a", "1", 50);
                cluster.set("b", "2");
                assertTrue(cluster.delete("b"));
                assertFalse(cluster.delete("b"));
                Thread.sleep(150);
                assertTrue(cluster.get("a").isEmpty());
            }
        } finally {
            stopAll();
        }
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(List.of("no-port")));
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(List.of("host:abc")));
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(List.of("host:0")));
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(List.of(":6380")));
        ClusterClient one = new ClusterClient(List.of("127.0.0.1:6399"));
        assertThrows(IllegalStateException.class, () -> one.removeNode("127.0.0.1:6399"));
        one.close();
        assertThrows(IllegalStateException.class, () -> one.get("k"));
    }
}
