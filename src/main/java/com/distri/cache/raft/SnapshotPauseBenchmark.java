package com.distri.cache.raft;

import java.util.Arrays;

/**
 * How long taking and restoring a snapshot of the replicated cache's state takes, for several cache sizes, in one JVM on
 * THIS machine. A snapshot has two parts: <b>freezing</b> the state ({@code captureSnapshot()}), which runs on the Raft node's
 * own thread, so that is how long the server cannot handle messages (heartbeats, votes, requests) while it happens; and
 * <b>encoding</b> it into bytes, which the node does in the background. The older, all-on-the-node's-thread way is the sum.
 * The numbers include garbage-collector work the copy causes. They do not include the disk write or sending the snapshot.
 *
 * <p>Run: {@code java -Xmx2g -cp target/classes com.distri.cache.raft.SnapshotPauseBenchmark [--value-bytes N] [--repeats N] [SIZES...]}
 * where SIZES are entry counts (default 10000 100000 1000000). Each size is run {@code --repeats} times (default 7, the
 * first 2 are dropped as warm-up) and the median and worst of the rest are printed.
 */
public final class SnapshotPauseBenchmark {

    private SnapshotPauseBenchmark() {}

    public static void main(String[] args) {
        int valueBytes = 100;
        int repeats = 7;
        java.util.List<Integer> sizes = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--value-bytes")) {
                valueBytes = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--repeats")) {
                repeats = Integer.parseInt(args[++i]);
            } else {
                sizes.add(Integer.parseInt(args[i]));
            }
        }
        if (sizes.isEmpty()) {
            sizes = java.util.List.of(10_000, 100_000, 1_000_000);
        }
        if (repeats < 3) {
            throw new IllegalArgumentException("--repeats must be at least 3 (2 are dropped as warm-up)");
        }
        Runtime rt = Runtime.getRuntime();
        System.out.println("java " + System.getProperty("java.version") + ", " + rt.availableProcessors() + " CPUs, max heap "
                + rt.maxMemory() / (1024 * 1024) + " MiB, " + valueBytes + "-byte values, " + repeats + " runs per size (first 2 dropped)");
        System.out.printf("%-10s %12s %15s %15s %15s %15s %14s %14s%n", "entries", "snapshot MiB", "freeze median ms",
                "freeze worst ms", "encode median ms", "encode worst ms", "restore med ms", "restore worst");
        String value = "x".repeat(valueBytes);
        for (int size : sizes) {
            ReplicatedCache cache = new ReplicatedCache(size + 10, 1_000, System::currentTimeMillis);
            for (int i = 0; i < size; i++) {
                cache.apply(i + 1, CacheCommand.set(i % 1_000 + 1, i / 1_000 + 1, "key-" + i, value + i, -1).encode()); // 1000 clients
            }
            double[] freeze = new double[repeats];
            double[] take = new double[repeats];
            double[] restore = new double[repeats];
            long bytes = 0;
            for (int r = 0; r < repeats; r++) {
                long t0 = System.nanoTime();
                StateMachine.SnapshotCapture capture = cache.captureSnapshot();
                long tFrozen = System.nanoTime();
                byte[] snap = capture.serialize();
                long t1 = System.nanoTime();
                ReplicatedCache other = new ReplicatedCache(size + 10, 1_000, System::currentTimeMillis);
                long t2 = System.nanoTime();
                other.restore(snap);
                long t3 = System.nanoTime();
                freeze[r] = (tFrozen - t0) / 1e6;
                take[r] = (t1 - tFrozen) / 1e6;
                restore[r] = (t3 - t2) / 1e6;
                bytes = snap.length;
            }
            double[] f = Arrays.copyOfRange(freeze, 2, repeats);
            Arrays.sort(f);
            double[] t = Arrays.copyOfRange(take, 2, repeats);
            double[] s = Arrays.copyOfRange(restore, 2, repeats);
            Arrays.sort(t);
            Arrays.sort(s);
            System.out.printf("%-10d %12.1f %15.1f %15.1f %15.1f %15.1f %14.1f %14.1f%n", size, bytes / 1048576.0,
                    f[f.length / 2], f[f.length - 1], t[t.length / 2], t[t.length - 1], s[s.length / 2], s[s.length - 1]);
        }
        System.out.println("(freezing runs on the Raft node's thread: that is how long the server is unresponsive; encoding runs in"
                + " the background; sizes above 128 MiB are not snapshotted at all)");
    }
}
