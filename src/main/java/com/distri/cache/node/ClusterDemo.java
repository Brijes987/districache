package com.distri.cache.node;

import com.distri.cache.linearizability.LinearizabilityChecker;
import com.distri.cache.linearizability.Operation;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kill-the-leader demonstration with three real server processes on this machine.
 *
 * <p>A writer stores numbered keys one after another and counts only writes the cluster ACKNOWLEDGED. Part-way through,
 * the demo finds the leader and kills that process outright. The writer keeps going; the demo reports how long the
 * writer was stalled, then reads every acknowledged key back and compares. A second client reads concurrently, and the
 * whole recorded history is checked for linearizability.
 *
 * <p>Run: {@code java -cp target/classes com.distri.cache.node.ClusterDemo} (after {@code mvn compile}).
 * Optional: {@code --writes N} (default 600).
 */
public final class ClusterDemo {

    private ClusterDemo() {}

    private static long micros() {
        return System.nanoTime() / 1_000;
    }

    private static String stamp(long startNanos) {
        return String.format("[%6.2fs] ", (System.nanoTime() - startNanos) / 1e9);
    }

    public static void main(String[] args) throws Exception {
        int writes = 600;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--writes")) {
                writes = Integer.parseInt(args[++i]);
            } else {
                System.err.println("usage: ClusterDemo [--writes N]");
                System.exit(2);
            }
        }
        long t0 = System.nanoTime();
        Path root = Files.createTempDirectory("districache-demo");
        List<Operation> history = Collections.synchronizedList(new ArrayList<>());
        try (ClusterLauncher cluster = new ClusterLauncher(3, root, true)) {
            System.out.println(stamp(t0) + "starting 3 server processes (real disks, fsync on) in " + root);
            cluster.startAll();

            ReplicatedClient writer = cluster.newClient(30_000);
            writer.set("warmup", "x");
            System.out.println(stamp(t0) + "cluster is up; leader is server " + cluster.indexOfClientAddress(writer.lastAnsweredBy()));

            AtomicBoolean stopReader = new AtomicBoolean();
            AtomicInteger writtenSoFar = new AtomicInteger();
            Thread reader = new Thread(() -> {
                try (ReplicatedClient r = cluster.newClient(30_000)) {
                    java.util.Random random = new java.util.Random(1);
                    while (!stopReader.get()) {
                        int upTo = writtenSoFar.get();
                        if (upTo == 0) {
                            Thread.sleep(5);
                            continue;
                        }
                        String key = "key" + random.nextInt(upTo);
                        long invoke = micros();
                        try {
                            Optional<String> v = r.get(key);
                            history.add(Operation.get(2, key, v.isPresent(), v.orElse(null), invoke, micros()));
                        } catch (ReplicatedClientException e) {
                            // no answer for a read constrains nothing
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "demo-reader");
            reader.start();

            int killAt = writes / 3;
            int acknowledged = 0;
            long worstStallMicros = 0;
            long killedAtNanos = 0;
            int killedServer = -1;
            for (int i = 0; i < writes; i++) {
                if (i == killAt) {
                    killedServer = cluster.indexOfClientAddress(writer.lastAnsweredBy());
                    System.out.println(stamp(t0) + "KILLING the leader, server " + killedServer
                            + ", with SIGKILL after " + acknowledged + " acknowledged writes");
                    killedAtNanos = System.nanoTime();
                    cluster.kill(killedServer);
                }
                String key = "key" + i;
                String value = "value-" + i;
                long invoke = micros();
                writer.set(key, value);
                long returned = micros();
                history.add(Operation.set(1, key, value, invoke, returned));
                acknowledged++;
                writtenSoFar.set(acknowledged);
                worstStallMicros = Math.max(worstStallMicros, returned - invoke);
                if (i == killAt) {
                    System.out.println(stamp(t0) + "first write after the kill acknowledged by server "
                            + cluster.indexOfClientAddress(writer.lastAnsweredBy()) + " after "
                            + (System.nanoTime() - killedAtNanos) / 1_000_000 + " ms");
                }
                if (i == (2 * writes) / 3) {
                    System.out.println(stamp(t0) + "restarting server " + killedServer + " from its files");
                    cluster.start(killedServer);
                }
            }
            stopReader.set(true);
            reader.join();
            System.out.println(stamp(t0) + acknowledged + " writes acknowledged; slowest single write took "
                    + worstStallMicros / 1_000 + " ms");

            int lost = 0;
            try (ReplicatedClient verifier = cluster.newClient(30_000)) {
                for (int i = 0; i < writes; i++) {
                    Optional<String> v = verifier.get("key" + i);
                    if (v.isEmpty() || !v.get().equals("value-" + i)) {
                        lost++;
                    }
                }
            }
            System.out.println(stamp(t0) + "read back all " + writes + " keys: " + lost + " lost or wrong");

            LinearizabilityChecker.Result result;
            synchronized (history) {
                result = new LinearizabilityChecker().check(new ArrayList<>(history));
            }
            System.out.println(stamp(t0) + "linearizability check of " + history.size()
                    + " recorded operations (writer and concurrent reader): " + result.status());
            if (!result.ok()) {
                System.out.println(result.explanation());
            }
            writer.close();
            System.out.println(lost == 0 && result.ok()
                    ? "RESULT: the leader was killed and no acknowledged write was lost."
                    : "RESULT: PROBLEM FOUND, see above.");
            System.exit(lost == 0 && result.ok() ? 0 : 1);
        } finally {
            // best effort cleanup of the temp directory
            try (var walk = Files.walk(root)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            } catch (java.io.IOException ignored) {
                // leave it
            }
        }
    }
}
