package com.distri.cache.node;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Throughput and latency of a three-server replicated group on THIS machine.
 *
 * <p>What this measures: closed-loop clients (each sends its next request when the previous one is answered) against
 * three server processes, over loopback TCP, with every request going through the Raft log and a majority
 * acknowledgement, with and without forcing each write to disk. Reads also go through the log.
 *
 * <p>What it does not: the three servers, the clients and the disk all share this one machine, so there is no real
 * network latency and the CPUs are contended; results depend heavily on how fast the disk can fsync. It is a way to see
 * relative costs (durable versus not, one client versus many), not a statement about production capacity. There is no
 * comparison with other systems here because none was measured.
 *
 * <p>Run: {@code java -cp target/classes com.distri.cache.node.ClusterBenchmark [--seconds N]} (default 5 per cell).
 */
public final class ClusterBenchmark {

    private ClusterBenchmark() {}

    public static void main(String[] args) throws Exception {
        int seconds = 5;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--seconds")) {
                seconds = Integer.parseInt(args[++i]);
            } else {
                System.err.println("usage: ClusterBenchmark [--seconds N]");
                System.exit(2);
            }
        }
        System.out.println("3 server processes + clients on one machine, loopback TCP, 100-byte values, "
                + seconds + " s per cell after 1 s warm-up");
        System.out.println("java " + System.getProperty("java.version") + ", " + Runtime.getRuntime().availableProcessors()
                + " CPUs, " + System.getProperty("os.name"));
        System.out.println();
        System.out.printf("%-9s %-6s %8s %12s %9s %9s %9s%n", "fsync", "op", "clients", "ops/second", "p50 ms",
                "p99 ms", "max ms");
        for (boolean fsync : new boolean[] {true, false}) {
            Path root = Files.createTempDirectory("districache-bench");
            try (ClusterLauncher cluster = new ClusterLauncher(3, root, fsync)) {
                cluster.startAll();
                try (ReplicatedClient warm = cluster.newClient(30_000)) {
                    warm.set("warmup", "x");
                }
                for (String op : new String[] {"SET", "GET"}) {
                    for (int clients : new int[] {1, 8, 32}) {
                        run(cluster, fsync, op, clients, seconds);
                    }
                }
            } finally {
                try (var walk = Files.walk(root)) {
                    walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                } catch (java.io.IOException ignored) {
                    // leave it
                }
            }
        }
        System.exit(0);
    }

    private static void run(ClusterLauncher cluster, boolean fsync, String op, int clients, int seconds)
            throws Exception {
        String value = "x".repeat(100);
        AtomicBoolean measuring = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        List<long[]> perThread = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        List<int[]> counts = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            long[] latencies = new long[2_000_000];
            int[] count = new int[1];
            perThread.add(latencies);
            counts.add(count);
            int id = c;
            Thread t = new Thread(() -> {
                try (ReplicatedClient client = cluster.newClient(30_000)) {
                    int n = 0;
                    while (!stop.get()) {
                        String key = "k" + id + "-" + (n++ % 1000);
                        long start = System.nanoTime();
                        if (op.equals("SET")) {
                            client.set(key, value);
                        } else {
                            client.get(key);
                        }
                        long took = System.nanoTime() - start;
                        if (measuring.get() && count[0] < latencies.length) {
                            latencies[count[0]++] = took;
                        }
                    }
                } catch (ReplicatedClientException e) {
                    System.err.println("client failed: " + e.getMessage());
                }
            }, "bench-" + c);
            t.start();
            threads.add(t);
        }
        Thread.sleep(1_000); // warm-up
        long begin = System.nanoTime();
        measuring.set(true);
        Thread.sleep(seconds * 1_000L);
        measuring.set(false);
        long elapsed = System.nanoTime() - begin;
        stop.set(true);
        for (Thread t : threads) {
            t.join(60_000);
        }
        int total = 0;
        for (int[] c : counts) {
            total += c[0];
        }
        long[] all = new long[total];
        int at = 0;
        for (int c = 0; c < clients; c++) {
            System.arraycopy(perThread.get(c), 0, all, at, counts.get(c)[0]);
            at += counts.get(c)[0];
        }
        Arrays.sort(all);
        double perSecond = total / (elapsed / 1e9);
        System.out.printf("%-9s %-6s %8d %12.0f %9.2f %9.2f %9.2f%n", fsync ? "on" : "off", op, clients, perSecond,
                all.length == 0 ? 0 : all[all.length / 2] / 1e6, all.length == 0 ? 0 : all[(int) (all.length * 0.99)] / 1e6,
                all.length == 0 ? 0 : all[all.length - 1] / 1e6);
    }
}
