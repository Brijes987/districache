package com.distri.cache.node;

import com.distri.cache.raft.RaftMessage;
import com.distri.cache.raft.RaftNode;
import com.distri.cache.raft.ReplicatedCache;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The one thread that owns a {@link RaftNode} and its {@link ReplicatedCache}, which are not thread-safe. Network
 * threads never touch them: they post tasks here. The same thread also supplies the clock: it calls
 * {@code tick()} every {@code tickMillis} (never several at once after a stall, because a burst of ticks after a long
 * garbage-collection pause would start needless elections).
 *
 * <p><b>Fail-stop.</b> If any task throws (a broken Raft invariant, a disk error), the runtime stops itself and calls
 * {@code onFatal}. Carrying on with state that may be inconsistent could break the promises already made to other
 * servers and to clients; stopping and recovering from disk is the safe response.
 */
final class NodeRuntime implements AutoCloseable {

    private static final int QUEUE_CAPACITY = 20_000;

    private final RaftNode node;
    private final ReplicatedCache cache;
    private final long tickMillis;
    private final int maxBatch;
    private final Runnable onFatal;
    private final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread thread;
    private final java.util.concurrent.ExecutorService snapshotThread;
    private volatile boolean closed;
    private volatile boolean failed;

    // a snapshot of the node's state that other threads may read without going through the loop
    private volatile RaftNode.Role role = RaftNode.Role.FOLLOWER;
    private volatile long term;
    private volatile long commitIndex;
    private volatile int leaderId = -1;
    private volatile long lastLogIndex;
    private volatile long lastApplied;
    private volatile long leaderChanges; // times this node saw its known leader change (including to "none" and back)
    private volatile long timesLeader;   // times this node became leader
    private volatile long snapshotIndex;
    private volatile long snapshotsTaken;
    private volatile long snapshotsInstalled;
    private volatile long snapshotsSkipped;
    private volatile long readsServed;
    private volatile long snapshotChunksSent;
    private volatile long snapshotChunksReceived;
    private volatile long pendingReads;

    /**
     * @param maxBatch group commit: the loop runs at most this many queued tasks in a row, without waiting, and proposes the client writes among them with ONE log append (one
     *     fsync). 1 turns group commit off: every request is proposed and fsynced alone.
     */
    NodeRuntime(RaftNode node, ReplicatedCache cache, long tickMillis, int maxBatch, Runnable onFatal) {
        if (tickMillis <= 0) {
            throw new IllegalArgumentException("tickMillis must be positive");
        }
        if (maxBatch <= 0) {
            throw new IllegalArgumentException("maxBatch must be positive");
        }
        this.maxBatch = maxBatch;
        if (maxBatch > 1) {
            cache.setBatching(true);
        }
        this.node = node;
        this.cache = cache;
        this.tickMillis = tickMillis;
        this.onFatal = onFatal;
        this.thread = new Thread(this::loop, "raft-loop-" + node.id());
        this.thread.setDaemon(true);
        // Encoding a big snapshot and writing it to disk take far longer than an election timeout, so they run here, not on
        // the loop thread, which has heartbeats to send. The result is handed back as an ordinary task.
        this.snapshotThread = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-snapshot-" + node.id());
            t.setDaemon(true);
            return t;
        });
        node.setSnapshotWorker((work, done) -> snapshotThread.execute(() -> {
            RaftNode.SnapshotOutcome outcome = work.get();
            while (!closed) {
                if (post(() -> done.accept(outcome))) {
                    return;
                }
                try {
                    Thread.sleep(5); // the task queue was full: try again rather than lose the result
                } catch (InterruptedException e) {
                    return;
                }
            }
            // closed: nobody will commit it; a leftover temporary file is deleted at the next start
        }));
    }

    void start() {
        thread.start();
    }

    /** Queues a task for the loop thread. False if the runtime is stopped or its queue is full. */
    boolean post(Runnable task) {
        return !closed && tasks.offer(task);
    }

    void deliver(int from, RaftMessage message) {
        post(() -> node.receive(from, message));
    }

    /** Runs {@code request} on the loop thread; the returned future completes with whatever it returns. */
    <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> request) {
        CompletableFuture<T> outer = new CompletableFuture<>();
        boolean queued = post(() -> {
            try {
                request.get().whenComplete((value, error) -> {
                    if (error != null) {
                        outer.completeExceptionally(error);
                    } else {
                        outer.complete(value);
                    }
                });
            } catch (RuntimeException e) {
                outer.completeExceptionally(e);
            }
        });
        if (!queued) {
            outer.completeExceptionally(new IllegalStateException("server is stopped or overloaded"));
        }
        return outer;
    }

    /** Runs a read-only inspection on the loop thread and waits for it. For tests and diagnostics. */
    <T> T inspect(Supplier<T> reader) throws InterruptedException {
        CompletableFuture<T> result = new CompletableFuture<>();
        if (!post(() -> result.complete(reader.get()))) {
            throw new IllegalStateException("server is stopped or overloaded");
        }
        try {
            return result.get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("inspection failed", e);
        }
    }

    RaftNode.Role role() {
        return role;
    }

    long term() {
        return term;
    }

    long commitIndex() {
        return commitIndex;
    }

    int leaderId() {
        return leaderId;
    }

    long lastLogIndex() {
        return lastLogIndex;
    }

    long lastApplied() {
        return lastApplied;
    }

    long leaderChanges() {
        return leaderChanges;
    }

    long timesLeader() {
        return timesLeader;
    }

    long snapshotIndex() {
        return snapshotIndex;
    }

    long snapshotsTaken() {
        return snapshotsTaken;
    }

    long snapshotsInstalled() {
        return snapshotsInstalled;
    }

    long snapshotsSkipped() {
        return snapshotsSkipped;
    }

    long snapshotChunksSent() {
        return snapshotChunksSent;
    }

    long snapshotChunksReceived() {
        return snapshotChunksReceived;
    }

    long readsServed() {
        return readsServed;
    }

    long pendingReads() {
        return pendingReads;
    }

    long proposalBatches() {
        return cache.batchesProposed();
    }

    long proposedEntries() {
        return cache.entriesBatched();
    }

    boolean failed() {
        return failed;
    }

    private void loop() {
        long nextTick = System.nanoTime() + tickMillis * 1_000_000;
        try {
            while (!closed) {
                long waitNanos = nextTick - System.nanoTime();
                Runnable task = waitNanos > 0 ? tasks.poll(waitNanos, TimeUnit.NANOSECONDS) : tasks.poll();
                if (task != null) {
                    task.run();
                    // Group commit: whatever else is already waiting joins this batch, without any added delay. While
                    // the previous batch was being fsynced, more requests queued up, so the load itself forms batches.
                    for (int more = 1; more < maxBatch; more++) {
                        Runnable next = tasks.poll();
                        if (next == null) {
                            break;
                        }
                        next.run();
                    }
                    cache.flush();
                }
                if (System.nanoTime() >= nextTick) {
                    node.tick();
                    cache.poll();
                    nextTick = System.nanoTime() + tickMillis * 1_000_000;
                }
                publish();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            if (!closed) {
                failed = true;
                System.err.println("FATAL: node " + node.id() + " stopping after an unexpected error: " + t);
                t.printStackTrace();
                closed = true;
                cache.abandonPending();
                onFatal.run();
            }
        }
    }

    private void publish() { // runs on the loop thread, the only writer of these fields
        RaftNode.Role newRole = node.role();
        if (newRole == RaftNode.Role.LEADER && role != RaftNode.Role.LEADER) {
            timesLeader++;
        }
        int newLeader = node.leaderId();
        if (newLeader != leaderId) {
            leaderChanges++;
        }
        role = newRole;
        term = node.term();
        commitIndex = node.commitIndex();
        leaderId = newLeader;
        lastLogIndex = node.lastLogIndex();
        lastApplied = node.lastApplied();
        snapshotIndex = node.snapshotIndex();
        snapshotsTaken = node.snapshotsTaken();
        snapshotsInstalled = node.snapshotsInstalled();
        snapshotsSkipped = node.snapshotsSkipped();
        readsServed = node.readsServed();
        snapshotChunksSent = node.snapshotChunksSent();
        snapshotChunksReceived = node.snapshotChunksReceived();
        pendingReads = node.pendingReads();
    }

    @Override
    public void close() {
        closed = true;
        snapshotThread.shutdownNow();
        if (Thread.currentThread() == thread) {
            return; // stopping itself after a fatal error: the loop exits as soon as this task returns
        }
        thread.interrupt();
        try {
            thread.join(3_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
