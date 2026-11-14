package com.distri.cache.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client for a replicated group that runs on SEPARATE machines (or VMs, or containers): it only needs the servers'
 * client addresses, so run it from any machine that can reach them. Start the servers with
 * {@link ReplicatedServerMain} on each machine; docs/MULTI_MACHINE.md has the steps.
 *
 * <pre>
 * java -cp target/classes com.distri.cache.node.RemoteProbe MODE --servers host:port,host:port,host:port
 *      [--client-secret-file F] [--tls-truststore F --tls-truststore-password-file F] [MODE OPTIONS]
 *
 * smoke                       write a key, read it back, say which server answered
 * bench  [--clients N] [--seconds S] [--op set|get]   closed-loop load, prints ops/second and latency percentiles
 * watch  [--seconds S] [--rate R]   one writer sends R numbered writes per second (default 20) for S seconds (default 60);
 *                             every slow or failed write is printed as it happens, so you can kill or cut off the leader
 *                             by hand while it runs. At the end it reads back every ACKNOWLEDGED write and reports the
 *                             longest time without an acknowledged write, and any acknowledged write that is missing.
 * </pre>
 *
 * What it measures: latency and availability as seen from the machine it runs on, over whatever network lies between. It
 * does not know where the servers are or what failed; you do the failing (stop a process, pull a cable, block a port) and
 * note the time. Nothing here is a result until you have run it, and results from one set of machines say nothing about
 * another.
 */
public final class RemoteProbe {

    private RemoteProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
        }
        String mode = args[0];
        List<InetSocketAddress> servers = new ArrayList<>();
        String secretFile = null;
        String truststore = null;
        String truststorePasswordFile = null;
        int clients = 8;
        int seconds = -1;
        int rate = 20;
        String op = "set";
        try {
            for (int i = 1; i < args.length; i++) {
                switch (args[i]) {
                    case "--servers" -> {
                        for (String s : args[++i].split(",")) {
                            int colon = s.lastIndexOf(':');
                            if (colon <= 0) {
                                throw new IllegalArgumentException("server must be host:port, got " + s);
                            }
                            servers.add(InetSocketAddress.createUnresolved(s.substring(0, colon).trim(),
                                    Integer.parseInt(s.substring(colon + 1).trim())));
                        }
                    }
                    case "--client-secret-file" -> secretFile = args[++i];
                    case "--tls-truststore" -> truststore = args[++i];
                    case "--tls-truststore-password-file" -> truststorePasswordFile = args[++i];
                    case "--clients" -> clients = Integer.parseInt(args[++i]);
                    case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                    case "--rate" -> rate = Integer.parseInt(args[++i]);
                    case "--op" -> op = args[++i];
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            usage();
        }
        if (servers.isEmpty() || !Arrays.asList("smoke", "bench", "watch").contains(mode)
                || !(op.equals("set") || op.equals("get")) || clients < 1 || rate < 1) {
            usage();
        }
        List<InetSocketAddress> resolved = new ArrayList<>();
        for (InetSocketAddress s : servers) {
            resolved.add(new InetSocketAddress(s.getHostString(), s.getPort())); // resolve now, fail fast on a bad name
        }
        byte[] secret = null;
        if (secretFile != null) {
            byte[] bytes = Files.readAllBytes(Path.of(secretFile));
            int end = bytes.length;
            while (end > 0 && (bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
                end--;
            }
            secret = Arrays.copyOf(bytes, end);
        }
        javax.net.ssl.SSLContext tls = null;
        if (truststore != null) {
            if (truststorePasswordFile == null) {
                System.err.println("--tls-truststore needs --tls-truststore-password-file");
                System.exit(2);
            }
            String pw = new String(Files.readAllBytes(Path.of(truststorePasswordFile)), StandardCharsets.UTF_8).strip();
            tls = com.distri.cache.server.Tls.clientContext(Path.of(truststore), pw.toCharArray());
        }
        System.out.println("java " + System.getProperty("java.version") + " on " + System.getProperty("os.name") + ", servers "
                + servers.stream().map(a -> a.getHostString() + ":" + a.getPort()).toList() + (tls != null ? ", TLS" : ", plain TCP (no TLS)"));
        switch (mode) {
            case "smoke" -> smoke(resolved, secret, tls);
            case "bench" -> bench(resolved, secret, tls, clients, seconds < 0 ? 10 : seconds, op);
            default -> watch(resolved, secret, tls, seconds < 0 ? 60 : seconds, rate);
        }
        System.exit(0);
    }

    private static void usage() {
        System.err.println("usage: RemoteProbe smoke|bench|watch --servers h:p,h:p,h:p [--client-secret-file F]"
                + " [--tls-truststore F --tls-truststore-password-file F] [--clients N] [--seconds S] [--rate R] [--op set|get]");
        System.exit(2);
    }

    private static ReplicatedClient client(List<InetSocketAddress> servers, byte[] secret, javax.net.ssl.SSLContext tls,
            long timeoutMillis) {
        return new ReplicatedClient(servers, secret, timeoutMillis, tls);
    }

    private static void smoke(List<InetSocketAddress> servers, byte[] secret, javax.net.ssl.SSLContext tls) throws IOException {
        try (ReplicatedClient c = client(servers, secret, tls, 20_000)) {
            String key = "probe-smoke-" + System.currentTimeMillis();
            long t0 = System.nanoTime();
            c.set(key, "hello");
            long t1 = System.nanoTime();
            Optional<String> got = c.get(key);
            long t2 = System.nanoTime();
            System.out.printf("SET %.2f ms, GET %.2f ms, answered by %s (the leader)%n", (t1 - t0) / 1e6, (t2 - t1) / 1e6,
                    c.lastAnsweredBy());
            if (got.isPresent() && got.get().equals("hello")) {
                System.out.println("OK: read back what was written");
            } else {
                System.out.println("FAILED: read back " + got);
                System.exit(1);
            }
        }
    }

    private static void bench(List<InetSocketAddress> servers, byte[] secret, javax.net.ssl.SSLContext tls, int clients,
            int seconds, String op) throws Exception {
        System.out.println(clients + " closed-loop clients, " + seconds + " s after 2 s warm-up, op " + op.toUpperCase()
                + ", 100-byte values");
        String value = "x".repeat(100);
        AtomicBoolean measuring = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();
        long[][] lat = new long[clients][1_000_000];
        int[] counts = new int[clients];
        int[] errors = new int[clients];
        Thread[] threads = new Thread[clients];
        for (int c = 0; c < clients; c++) {
            int id = c;
            threads[c] = new Thread(() -> {
                try (ReplicatedClient client = client(servers, secret, tls, 30_000)) {
                    int n = 0;
                    if (op.equals("get")) {
                        client.set("k" + id, value); // something to read
                    }
                    while (!stop.get()) {
                        long start = System.nanoTime();
                        try {
                            if (op.equals("set")) {
                                client.set("k" + id + "-" + (n++ % 1000), value);
                            } else {
                                client.get("k" + id);
                            }
                        } catch (ReplicatedClientException e) {
                            errors[id]++;
                            continue;
                        }
                        long took = System.nanoTime() - start;
                        if (measuring.get() && counts[id] < lat[id].length) {
                            lat[id][counts[id]++] = took;
                        }
                    }
                } catch (IOException e) {
                    System.err.println("client " + id + " failed: " + e.getMessage());
                }
            }, "probe-" + c);
            threads[c].start();
        }
        Thread.sleep(2_000);
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
        int failed = 0;
        for (int c = 0; c < clients; c++) {
            total += counts[c];
            failed += errors[c];
        }
        long[] all = new long[total];
        int at = 0;
        for (int c = 0; c < clients; c++) {
            System.arraycopy(lat[c], 0, all, at, counts[c]);
            at += counts[c];
        }
        Arrays.sort(all);
        System.out.printf("%d operations, %.0f ops/second, errors %d%n", total, total / (elapsed / 1e9), failed);
        if (total > 0) {
            System.out.printf("latency ms: p50 %.2f  p99 %.2f  p99.9 %.2f  max %.2f%n", all[total / 2] / 1e6,
                    all[(int) (total * 0.99)] / 1e6, all[(int) Math.min(total - 1, total * 0.999)] / 1e6, all[total - 1] / 1e6);
        }
    }

    private static void watch(List<InetSocketAddress> servers, byte[] secret, javax.net.ssl.SSLContext tls, int seconds,
            int rate) throws Exception {
        System.out.println("one writer, " + rate + " writes/second for " + seconds + " s. Kill or cut off the leader (see"
                + " the 'answered by' lines) while this runs. Slow (>200 ms) or failed writes are printed.");
        long periodNanos = 1_000_000_000L / rate;
        List<Integer> acknowledged = new ArrayList<>();
        List<Integer> unknown = new ArrayList<>();
        int refused = 0;
        InetSocketAddress lastLeader = null;
        long start = System.nanoTime();
        long lastAckAt = start;
        long longestGapNanos = 0;
        long longestGapStart = 0;
        long next = start;
        try (ReplicatedClient c = client(servers, secret, tls, 60_000)) {
            for (int i = 0; System.nanoTime() - start < seconds * 1_000_000_000L; i++) {
                long t0 = System.nanoTime();
                try {
                    c.set("watch-" + i, Integer.toString(i));
                    long t1 = System.nanoTime();
                    acknowledged.add(i);
                    // the gap between two acknowledgements includes the time a failing write spent waiting or retrying
                    long gap = t1 - lastAckAt;
                    if (gap > longestGapNanos) {
                        longestGapNanos = gap;
                        longestGapStart = lastAckAt - start;
                    }
                    lastAckAt = t1;
                    if ((t1 - t0) > 200_000_000L) {
                        System.out.printf("[%7.2fs] write %d took %.0f ms%n", (t1 - start) / 1e9, i, (t1 - t0) / 1e6);
                    }
                    InetSocketAddress by = c.lastAnsweredBy();
                    if (by != null && !by.equals(lastLeader)) {
                        System.out.printf("[%7.2fs] write %d answered by %s%s%n", (t1 - start) / 1e9, i, by,
                                lastLeader == null ? " (first)" : " (the leader CHANGED)");
                        lastLeader = by;
                    }
                } catch (ReplicatedClientException e) {
                    if (e.outcomeUnknown()) {
                        unknown.add(i);
                    } else {
                        refused++;
                    }
                    System.out.printf("[%7.2fs] write %d FAILED after %.0f ms (%s): %s%n", (System.nanoTime() - start) / 1e9, i,
                            (System.nanoTime() - t0) / 1e6, e.outcomeUnknown() ? "outcome unknown" : "did not run",
                            e.getMessage());
                }
                next += periodNanos;
                long sleep = next - System.nanoTime();
                if (sleep > 0) {
                    Thread.sleep(sleep / 1_000_000, (int) (sleep % 1_000_000));
                } else {
                    next = System.nanoTime(); // behind schedule (a long stall): do not fire a burst to catch up
                }
            }
            System.out.println();
            System.out.println("reading back every acknowledged write...");
            int missing = 0;
            int wrong = 0;
            for (int i : acknowledged) {
                Optional<String> got = c.get("watch-" + i);
                if (got.isEmpty()) {
                    missing++;
                } else if (!got.get().equals(Integer.toString(i))) {
                    wrong++;
                }
            }
            int landed = 0;
            for (int i : unknown) {
                if (c.get("watch-" + i).isPresent()) {
                    landed++;
                }
            }
            System.out.printf("acknowledged writes: %d, failed (did not run): %d, failed (outcome unknown): %d (of which %d turned out to have been applied)%n",
                    acknowledged.size(), refused, unknown.size(), landed);
            System.out.printf("longest time between two acknowledged writes: %.0f ms (starting %.1f s into the run); at the"
                    + " configured rate the normal gap is %d ms%n", longestGapNanos / 1e6, longestGapStart / 1e9, 1000 / rate);
            System.out.println(missing == 0 && wrong == 0
                    ? "OK: every acknowledged write is present with the right value"
                    : "FAILED: " + missing + " acknowledged writes missing, " + wrong + " with a wrong value");
            if (missing != 0 || wrong != 0) {
                System.exit(1);
            }
        }
    }
}
