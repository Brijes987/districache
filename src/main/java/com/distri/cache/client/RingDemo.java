package com.distri.cache.client;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Prints how evenly a consistent-hash ring spreads keys, and how many keys move when a server is added,
 * compared with plain {@code hash % n}. Run: java -cp target/classes com.distri.cache.client.RingDemo
 */
public final class RingDemo {

    private RingDemo() {}

    public static void main(String[] args) {
        int keys = 100_000;
        int servers = 5;
        ConsistentHashRing ring = new ConsistentHashRing();
        for (int i = 0; i < servers; i++) {
            ring.add("server" + i + ":6380");
        }
        Map<String, Integer> load = new TreeMap<>();
        String[] before = new String[keys];
        for (int k = 0; k < keys; k++) {
            before[k] = ring.nodeFor("key-" + k);
            load.merge(before[k], 1, Integer::sum);
        }
        System.out.println(keys + " keys over " + servers + " servers, " + ConsistentHashRing.DEFAULT_VIRTUAL_NODES
                + " virtual nodes each:");
        load.forEach((node, n) -> System.out.printf("  %-14s %6d keys (%.1f%%)%n", node, n, 100.0 * n / keys));
        System.out.printf("  largest/smallest = %.2f%n%n",
                (double) Collections.max(load.values()) / Collections.min(load.values()));

        ring.add("server" + servers + ":6380");
        int ringMoved = 0;
        int movedElsewhere = 0;
        for (int k = 0; k < keys; k++) {
            String now = ring.nodeFor("key-" + k);
            if (!now.equals(before[k])) {
                ringMoved++;
                if (!now.equals("server" + servers + ":6380")) {
                    movedElsewhere++;
                }
            }
        }
        int moduloMoved = 0;
        for (int k = 0; k < keys; k++) {
            int h = ("key-" + k).hashCode();
            if (Math.floorMod(h, servers) != Math.floorMod(h, servers + 1)) {
                moduloMoved++;
            }
        }
        System.out.println("Adding a 6th server:");
        System.out.printf("  consistent hashing: %.1f%% of keys moved (ideal %.1f%%); %d moved between old servers%n",
                100.0 * ringMoved / keys, 100.0 / (servers + 1), movedElsewhere);
        System.out.printf("  hash %% n:          %.1f%% of keys moved%n", 100.0 * moduloMoved / keys);
    }
}
