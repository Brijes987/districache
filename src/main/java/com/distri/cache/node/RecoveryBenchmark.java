package com.distri.cache.node;

import com.distri.cache.raft.FileRaftStorage;
import com.distri.cache.raft.RaftConfig;
import com.distri.cache.raft.RaftNode;
import com.distri.cache.raft.ReplicatedCache;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * What snapshots buy: disk used and recovery time against the number of writes. For each write count it runs the same
 * workload twice, with snapshots off and on, then closes everything and times a restart from the files alone.
 *
 * <p>One server (a group of one) with real files: the quantities measured, bytes on disk and the time to rebuild the
 * cache from them, do not depend on how many servers there are or on fsync, so fsync is off here to reach large write
 * counts quickly. The writes overwrite a fixed set of keys (the state stays the same size while the log grows).
 *
 * <p>Usage: {@code java -cp target/classes com.distri.cache.node.RecoveryBenchmark [--keys N] [--snapshot-every N]
 * [--writes N,N,...]}.
 */
public final class RecoveryBenchmark {

    private RecoveryBenchmark() {}

    private record Result(long writes, long diskBytes, long recoveryMillis, long logEntriesAfterRestart,
            long snapshotIndex) {}

    public static void main(String[] args) throws Exception {
        int keys = 10_000;
        int every = 10_000;
        long[] writeCounts = {100_000, 400_000, 1_000_000};
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--keys" -> keys = Integer.parseInt(args[++i]);
                case "--snapshot-every" -> every = Integer.parseInt(args[++i]);
                case "--writes" -> {
                    String[] parts = args[++i].split(",");
                    writeCounts = new long[parts.length];
                    for (int p = 0; p < parts.length; p++) {
                        writeCounts[p] = Long.parseLong(parts[p].trim());
                    }
                }
                default -> {
                    System.err.println("usage: RecoveryBenchmark [--keys N] [--snapshot-every N] [--writes N,N,...]");
                    return;
                }
            }
        }
        System.out.println("one server, real files, fsync off, 100-byte values, " + keys + " distinct keys, java "
                + System.getProperty("java.version") + ", " + Runtime.getRuntime().availableProcessors() + " CPUs");
        System.out.println();
        System.out.printf("%-12s %-16s %14s %14s %18s%n", "writes", "snapshots", "disk bytes", "restart ms",
                "log entries kept");
        for (long writes : writeCounts) {
            for (int snapshotEvery : new int[] {0, every}) {
                Path dir = Files.createTempDirectory("recovery-bench");
                try {
                    Result r = run(dir, writes, keys, snapshotEvery);
                    System.out.printf("%-12d %-16s %,14d %,14d %,18d%n", r.writes(),
                            snapshotEvery == 0 ? "off" : "every " + snapshotEvery, r.diskBytes(), r.recoveryMillis(),
                            r.logEntriesAfterRestart());
                } finally {
                    deleteTree(dir);
                }
            }
        }
    }

    private static Result run(Path dir, long writes, int keys, int snapshotEvery) throws IOException {
        RaftConfig config = new RaftConfig(10, 20, 2, 64, false, Set.of(), snapshotEvery);
        String value = "v".repeat(100);

        // ---- write phase
        try (FileRaftStorage storage = new FileRaftStorage(dir, false)) {
            ReplicatedCache cache = new ReplicatedCache(keys * 2, 1_000, System::currentTimeMillis);
            RaftNode node = new RaftNode(0, List.of(), storage, (to, m) -> {}, cache, config, new Random(1));
            cache.attach(node);
            cache.setBatching(true);
            for (int i = 0; i < 1_000 && node.role() != RaftNode.Role.LEADER; i++) {
                node.tick();
            }
            long seq = 0;
            while (seq < writes) {
                List<CompletableFuture<ReplicatedCache.Reply>> batch = new ArrayList<>();
                for (int b = 0; b < 256 && seq < writes; b++) {
                    seq++;
                    batch.add(cache.set(1, seq, "key" + (seq % keys), value, -1));
                }
                cache.flush();
                for (CompletableFuture<ReplicatedCache.Reply> f : batch) {
                    if (!f.isDone() || f.join().kind() != ReplicatedCache.Kind.OK) {
                        throw new IllegalStateException("a write was not acknowledged");
                    }
                }
            }
            cache.close();
        }
        long disk = 0;
        try (var files = Files.list(dir)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                disk += Files.size(f);
            }
        }

        // ---- restart phase: only the files exist now
        long start = System.nanoTime();
        try (FileRaftStorage storage = new FileRaftStorage(dir, false)) {
            ReplicatedCache cache = new ReplicatedCache(keys * 2, 1_000, System::currentTimeMillis);
            RaftNode node = new RaftNode(0, List.of(), storage, (to, m) -> {}, cache, config, new Random(1));
            cache.attach(node);
            for (int i = 0; i < 10_000 && (node.role() != RaftNode.Role.LEADER
                    || node.lastApplied() < node.lastLogIndex()); i++) {
                node.tick();
            }
            long millis = (System.nanoTime() - start) / 1_000_000;
            if (node.lastApplied() != node.lastLogIndex()) {
                throw new IllegalStateException("the restarted server did not apply its whole log");
            }
            if (cache.contents().size() != Math.min(keys, writes)) {
                throw new IllegalStateException("the restarted server holds " + cache.contents().size() + " keys");
            }
            long kept = storage.lastIndex() - storage.snapshotIndex();
            cache.close();
            return new Result(writes, disk, millis, kept, storage.snapshotIndex());
        }
    }

    private static void deleteTree(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
