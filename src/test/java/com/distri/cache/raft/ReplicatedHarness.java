package com.distri.cache.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.function.BooleanSupplier;

/** A simulated group in which every node runs a {@link ReplicatedCache}. Rebuilt on restart, like a real process. */
final class ReplicatedHarness implements SimCluster.NodeFactory {

    final SimCluster cluster;
    final ReplicatedCache[] caches;
    ReplicatedCache.Fault faultForAllNodes;

    private final Path storageDir; // null: keep Raft state in memory
    private final boolean batching; // group commit: requests wait in a queue until flushAll()
    private final Random tornTails;
    private final FileRaftStorage[] files;

    ReplicatedHarness(int n, long seed, RaftConfig config) {
        this(n, seed, config, null);
    }

    /**
     * @param storageDir if given, every node keeps its Raft state in files under it; a crash closes them, may leave
     *     a half-written record at the end of the log (what a crash during an append leaves), and the restart reopens
     *     and recovers from disk
     */
    ReplicatedHarness(int n, long seed, RaftConfig config, Path storageDir) {
        this(n, seed, config, storageDir, false);
    }

    ReplicatedHarness(int n, long seed, RaftConfig config, Path storageDir, boolean batching) {
        this.batching = batching;
        this.storageDir = storageDir;
        this.tornTails = new Random(seed * 17 + 3);
        this.files = new FileRaftStorage[n];
        this.caches = new ReplicatedCache[n];
        this.cluster = new SimCluster(n, seed, config, this);
    }

    @Override
    public StateMachine create(int node) {
        caches[node] = new ReplicatedCache(1_000, 100, System::currentTimeMillis, true);
        caches[node].setBatching(batching);
        if (faultForAllNodes != null) {
            caches[node].injectFault(faultForAllNodes);
        }
        return caches[node];
    }

    @Override
    public void started(int node, RaftNode raftNode) {
        rafts[node] = raftNode;
        caches[node].attach(raftNode);
    }

    @Override
    public void stopped(int node) {
        snapshotsTaken += rafts[node].snapshotsTaken();
        snapshotsInstalled += rafts[node].snapshotsInstalled();
        rafts[node] = null;
        collect(caches[node]);
        caches[node].close();
        caches[node] = null;
        if (files[node] != null) {
            try {
                files[node].close();
                files[node] = null;
                if (tornTails.nextInt(3) == 0) {
                    leaveHalfWrittenRecord(node);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public RaftStorage openStorage(int node) {
        if (storageDir == null) {
            return null;
        }
        try {
            files[node] = new FileRaftStorage(storageDir.resolve("node" + node), false);
            return files[node];
        } catch (IOException e) {
            throw new UncheckedIOException("recovery from disk failed for node " + node, e);
        }
    }

    /**
     * What a crash in the middle of an append leaves: a real record that was never acknowledged, cut short at a random
     * byte, or written completely but with a damaged byte (the data did not all reach the disk).
     */
    private void leaveHalfWrittenRecord(int node) throws IOException {
        byte[] command = new byte[tornTails.nextInt(40)];
        tornTails.nextBytes(command);
        java.nio.ByteBuffer body = java.nio.ByteBuffer.allocate(8 + command.length);
        body.putLong(99).put(command);
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(body.array());
        java.nio.ByteBuffer record = java.nio.ByteBuffer.allocate(8 + body.capacity());
        record.putInt(body.capacity()).putInt((int) crc.getValue()).put(body.array());
        byte[] bytes = record.array();
        if (tornTails.nextInt(4) == 0) {
            bytes[bytes.length - 1] ^= 0x10; // whole record, last byte damaged
        } else {
            bytes = java.util.Arrays.copyOf(bytes, 1 + tornTails.nextInt(bytes.length - 1));
        }
        Files.write(storageDir.resolve("node" + node).resolve("raft.log"), bytes, StandardOpenOption.APPEND);
    }

    void closeFiles() throws IOException {
        for (FileRaftStorage f : files) {
            if (f != null) {
                f.close();
            }
        }
    }

    /** One simulated step, then let every live front end notice a lost leadership. */
    void step() {
        cluster.step();
        pollAll();
    }

    final RaftNode[] rafts = new RaftNode[8];
    long snapshotsTaken;
    long snapshotsInstalled;
    long batches;
    long batchedEntries;
    long largestBatch;

    private void collect(ReplicatedCache c) {
        batches += c.batchesProposed();
        batchedEntries += c.entriesBatched();
        largestBatch = Math.max(largestBatch, c.largestBatch());
    }

    /** Batch statistics of every server so far, including those that crashed and were rebuilt. */
    void collectLive() {
        for (RaftNode r : rafts) {
            if (r != null) {
                snapshotsTaken += r.snapshotsTaken();
                snapshotsInstalled += r.snapshotsInstalled();
            }
        }
        for (ReplicatedCache c : caches) {
            if (c != null) {
                collect(c);
            }
        }
    }

    /** What the real node's loop does after running a batch of queued requests. */
    void flushAll() {
        for (ReplicatedCache c : caches) {
            if (c != null) {
                c.flush();
            }
        }
    }

    void pollAll() {
        flushAll();
        for (int i = 0; i < caches.length; i++) {
            if (caches[i] != null) {
                caches[i].poll();
            }
        }
    }

    boolean until(BooleanSupplier condition, int maxSteps) {
        for (int s = 0; s < maxSteps; s++) {
            if (condition.getAsBoolean()) {
                return true;
            }
            step();
        }
        return condition.getAsBoolean();
    }

    int electLeader() {
        if (!until(() -> cluster.leader() >= 0, 2_000)) {
            throw new AssertionError("no leader elected");
        }
        return cluster.leader();
    }

    /** True when every live node has applied everything the current leader has committed. */
    boolean fullyApplied() {
        int leader = cluster.leader();
        if (leader < 0) {
            return false;
        }
        RaftNode l = cluster.node(leader);
        if (l.commitIndex() < l.lastLogIndex()) {
            return false; // the leader's own no-op (or a recent entry) is not committed yet
        }
        for (int i = 0; i < caches.length; i++) {
            if (cluster.isUp(i) && cluster.node(i).lastApplied() != l.commitIndex()) {
                return false;
            }
        }
        return true;
    }
}
