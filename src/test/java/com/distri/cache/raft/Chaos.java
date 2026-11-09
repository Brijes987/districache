package com.distri.cache.raft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** Random crashes, restarts, partitions, message loss and client proposals against a {@link SimCluster}. */
final class Chaos {

    /** How violent the run is. */
    record Profile(double proposeRate, double crashRate, double restartRate, double partitionRate, double healRate,
            double networkChangeRate) {

        static Profile normal() {
            return new Profile(0.35, 0.004, 0.02, 0.006, 0.012, 0.01);
        }

        /** Many more failures, to stress elections and log repair. */
        static Profile brutal() {
            return new Profile(0.35, 0.012, 0.03, 0.02, 0.03, 0.03);
        }
    }

    final SimCluster cluster;
    final Random random;
    final Set<String> proposed = new HashSet<>();
    private int counter;
    /** Optional: runs before every simulated step, so a test can drive clients alongside the faults. */
    Runnable beforeStep = () -> { };

    Chaos(SimCluster cluster, long seed) {
        this.cluster = cluster;
        this.random = new Random(seed * 7919 + 13);
    }

    /** Runs {@code steps} ticks of mayhem. The cluster's invariants are checked on every tick. */
    void run(int steps, Profile p) {
        int n = cluster.n;
        for (int s = 0; s < steps; s++) {
            if (random.nextDouble() < p.proposeRate()) {
                int node = random.nextInt(n);
                String command = "c" + cluster.seed + "-" + (counter++);
                if (cluster.propose(node, command).accepted()) {
                    proposed.add(command);
                }
            }
            if (random.nextDouble() < p.crashRate()) {
                cluster.crash(random.nextInt(n));
            }
            if (random.nextDouble() < p.restartRate()) {
                cluster.restart(random.nextInt(n));
            }
            if (random.nextDouble() < p.partitionRate()) {
                Set<Integer> group = new HashSet<>();
                for (int i = 0; i < n; i++) {
                    if (random.nextBoolean()) {
                        group.add(i);
                    }
                }
                if (!group.isEmpty() && group.size() < n) {
                    cluster.partition(group);
                }
            }
            if (random.nextDouble() < p.healRate()) {
                cluster.heal();
            }
            if (random.nextDouble() < p.networkChangeRate()) {
                double[] drops = {0, 0.05, 0.2, 0.4};
                double[] dups = {0, 0.1, 0.3};
                int[] maxDelays = {1, 3, 8, 20};
                cluster.dropRate = drops[random.nextInt(drops.length)];
                cluster.duplicateRate = dups[random.nextInt(dups.length)];
                cluster.maxDelay = maxDelays[random.nextInt(maxDelays.length)];
            }
            beforeStep.run();
            cluster.step();
        }
    }

    /**
     * Ends the mayhem (everything up, no partitions, a clean network) and checks that the group recovers: it elects
     * a leader, commits a fresh command, every node ends with the same applied commands, and no command was
     * invented.
     */
    void settleAndCheck(int maxSteps) {
        for (int i = 0; i < cluster.n; i++) {
            cluster.restart(i);
        }
        cluster.heal();
        cluster.dropRate = 0;
        cluster.duplicateRate = 0;
        cluster.minDelay = 1;
        cluster.maxDelay = 3;

        // A leader that accepts the command can be deposed before committing it, so keep offering fresh ones
        // (one every 100 ticks) until one of them has been applied everywhere.
        String prefix = "final-" + cluster.seed + "-";
        int attempt = 0;
        long lastOffer = -1_000;
        for (int s = 0; s < maxSteps; s++) {
            cluster.step();
            int leader = cluster.leader();
            if (leader >= 0 && cluster.now - lastOffer >= 100) {
                String command = prefix + attempt++;
                if (cluster.propose(leader, command).accepted()) {
                    proposed.add(command);
                    lastOffer = cluster.now;
                }
            }
            if (converged(prefix)) {
                break;
            }
        }
        if (!converged(prefix)) {
            throw cluster.violation("the group did not recover after the chaos ended (liveness)");
        }
        cluster.deepCheck();
        List<SimCluster.Applied> reference = cluster.applied(0);
        for (int i = 1; i < cluster.n; i++) {
            if (!cluster.applied(i).equals(reference)) {
                throw cluster.violation("node " + i + " applied a different command sequence than node 0");
            }
        }
        for (SimCluster.Applied a : reference) {
            if (!proposed.contains(a.command())) {
                throw cluster.violation("command '" + a.command() + "' was applied but never proposed");
            }
        }
    }

    private boolean converged(String finalPrefix) {
        List<SimCluster.Applied> first = cluster.applied(0);
        if (first.isEmpty() || !first.get(first.size() - 1).command().startsWith(finalPrefix)) {
            return false;
        }
        for (int i = 1; i < cluster.n; i++) {
            if (!cluster.applied(i).equals(first)) {
                return false;
            }
        }
        return true;
    }

    /** One full randomized run for a seed. */
    static void fullRun(long seed, int steps, Profile profile, RaftConfig config) {
        int n = seed % 2 == 0 ? 3 : 5;
        SimCluster cluster = new SimCluster(n, seed, config);
        Chaos chaos = new Chaos(cluster, seed);
        chaos.run(steps, profile);
        chaos.settleAndCheck(4_000);
    }

    static List<Long> seeds(long from, long toInclusive) {
        List<Long> out = new ArrayList<>();
        for (long s = from; s <= toInclusive; s++) {
            out.add(s);
        }
        return out;
    }
}
