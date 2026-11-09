package com.distri.cache.raft;

import com.distri.cache.linearizability.Operation;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Runs clients against a simulated replicated cache while faults are injected, records what every client did and saw,
 * and checks the end state. Shared by the exactly-once tests and the linearizability tests.
 *
 * <p>Each of 4 clients has one request in flight at a time. If it gets no answer, or an answer that is not final
 * (NOT_LEADER, UNKNOWN), it retries the SAME request number, on the hinted leader or a random server.
 */
final class ReplicatedChaos {

    static final class Result {
        final List<Operation> history = new ArrayList<>();
        int acknowledged;
        int retries;
        int duplicatesInLog;
    }

    private static final class Client {
        final int id;
        long seq;
        boolean active;
        int target;
        String op;
        String key;
        String value;
        long invokedAt;
        CompletableFuture<ReplicatedCache.Reply> inFlight;
        long sentAt;

        Client(int id) {
            this.id = id;
        }
    }

    private final ReplicatedHarness h;
    private final Random random;
    private final List<Client> clients = new ArrayList<>();
    private final List<long[]> acknowledged = new ArrayList<>();
    private final Result result = new Result();
    private boolean accepting = true;

    private ReplicatedChaos(ReplicatedHarness h, long seed) {
        this.h = h;
        this.random = new Random(seed * 31 + 5);
        for (int c = 1; c <= 4; c++) {
            clients.add(new Client(c));
        }
    }

    /**
     * @param fault a deliberate bug to switch on in every server, or null
     * @param storageDir keep Raft state in files under this directory, or null for memory
     */
    static Result run(long seed, int steps, Chaos.Profile faults, RaftConfig config, ReplicatedCache.Fault fault,
            Path storageDir) {
        return run(seed, steps, faults, config, fault, storageDir, true);
    }

    /** @param endStateChecks false to return the history without the replica and exactly-once checks */
    static Result run(long seed, int steps, Chaos.Profile faults, RaftConfig config, ReplicatedCache.Fault fault,
            Path storageDir, boolean endStateChecks) {
        int n = seed % 2 == 0 ? 3 : 5;
        ReplicatedHarness h = new ReplicatedHarness(n, seed, config, storageDir);
        if (fault != null) {
            h.faultForAllNodes = fault;
            for (ReplicatedCache c : h.caches) {
                c.injectFault(fault);
            }
        }
        ReplicatedChaos run = new ReplicatedChaos(h, seed);
        Chaos chaos = new Chaos(h.cluster, seed);
        chaos.beforeStep = run::tick;
        Chaos.Profile noRandomProposals = new Chaos.Profile(0, faults.crashRate(), faults.restartRate(),
                faults.partitionRate(), faults.healRate(), faults.networkChangeRate());
        chaos.run(steps, noRandomProposals);
        run.settle();
        if (endStateChecks) {
            run.check();
        }
        return run.result;
    }

    private void tick() {
        for (Client c : clients) {
            drive(c);
        }
        h.pollAll();
    }

    private void settle() {
        accepting = false;
        for (int i = 0; i < h.caches.length; i++) {
            h.cluster.restart(i);
        }
        h.cluster.heal();
        h.cluster.dropRate = 0;
        h.cluster.duplicateRate = 0;
        h.cluster.minDelay = 1;
        h.cluster.maxDelay = 3;
        boolean settled = h.until(() -> {
            for (Client c : clients) {
                drive(c);
            }
            for (Client c : clients) {
                if (c.active) {
                    return false;
                }
            }
            return h.fullyApplied();
        }, 8_000);
        if (!settled) {
            throw h.cluster.violation("clients or servers did not settle after the faults ended");
        }
    }

    /** Replicas identical; no request executed twice anywhere; every acknowledged request executed everywhere. */
    private void check() {
        int n = h.caches.length;
        h.cluster.deepCheck();
        Map<String, String> reference = h.caches[0].contents();
        for (int i = 1; i < n; i++) {
            if (!reference.equals(h.caches[i].contents())) {
                throw h.cluster.violation("node " + i + " holds different data than node 0");
            }
        }
        int executed = 0;
        for (Client c : clients) {
            for (long s = 1; s <= c.seq; s++) {
                for (int i = 0; i < n; i++) {
                    int times = h.caches[i].executions(c.id, s);
                    if (times > 1) {
                        throw h.cluster.violation("request " + c.id + ":" + s + " was executed " + times
                                + " times on node " + i);
                    }
                }
                executed += h.caches[0].executions(c.id, s);
            }
        }
        for (long[] ack : acknowledged) {
            for (int i = 0; i < n; i++) {
                if (h.caches[i].executions(ack[0], ack[1]) != 1) {
                    throw h.cluster.violation("acknowledged request " + ack[0] + ":" + ack[1]
                            + " was not executed on node " + i);
                }
            }
        }
        result.duplicatesInLog = Math.max(0, h.cluster.applied(0).size() - executed);
        result.acknowledged = acknowledged.size();
    }

    private void drive(Client c) {
        int n = h.caches.length;
        if (!c.active) {
            if (!accepting || random.nextDouble() > 0.25) {
                return;
            }
            c.seq++;
            c.active = true;
            c.target = random.nextInt(n);
            c.inFlight = null;
            c.key = "k" + random.nextInt(5);
            c.value = "c" + c.id + "-" + c.seq;
            int pick = random.nextInt(10);
            c.op = pick < 5 ? "set" : pick < 8 ? "get" : "del";
            c.invokedAt = h.cluster.now;
        }
        if (c.inFlight == null) {
            if (h.caches[c.target] == null) {
                c.target = random.nextInt(n);
                return;
            }
            ReplicatedCache cache = h.caches[c.target];
            c.inFlight = switch (c.op) {
                case "set" -> cache.set(c.id, c.seq, c.key, c.value, -1);
                case "get" -> cache.get(c.id, c.seq, c.key);
                default -> cache.delete(c.id, c.seq, c.key);
            };
            c.sentAt = h.cluster.now;
            return;
        }
        if (c.inFlight.isDone()) {
            ReplicatedCache.Reply reply = c.inFlight.join();
            c.inFlight = null;
            switch (reply.kind()) {
                case OK -> {
                    long now = h.cluster.now;
                    result.history.add(switch (c.op) {
                        case "set" -> Operation.set(c.id, c.key, c.value, c.invokedAt, now);
                        case "get" -> Operation.get(c.id, c.key, reply.found(), reply.value(), c.invokedAt, now);
                        default -> Operation.delete(c.id, c.key, reply.found(), c.invokedAt, now);
                    });
                    acknowledged.add(new long[] {c.id, c.seq});
                    c.active = false;
                }
                case NOT_LEADER -> {
                    c.target = reply.leaderHint() >= 0 ? reply.leaderHint() : random.nextInt(n);
                    result.retries++;
                }
                case UNKNOWN -> {
                    c.target = random.nextInt(n);
                    result.retries++;
                }
                case STALE -> throw h.cluster.violation("client " + c.id + " got STALE for its current request");
                default -> throw new IllegalStateException();
            }
        } else if (h.cluster.now - c.sentAt > 80) {
            c.inFlight = null; // no answer: give up on this server and retry the same request number elsewhere
            c.target = random.nextInt(n);
            result.retries++;
        }
    }
}
