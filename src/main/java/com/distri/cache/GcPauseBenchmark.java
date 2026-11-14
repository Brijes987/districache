package com.distri.cache;

import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;

/**
 * How much the garbage collector gets in the way of an in-process cache full of small objects, on THIS machine.
 *
 * <p>Three things are reported, each from a different angle, because none of them alone is the whole story:
 * <ol>
 *   <li><b>Request latency</b> of get/put on a {@link ShardedLRUCache} filled with {@code --entries} String entries,
 *       from {@code --threads} threads, 90% reads / 10% writes over random keys (p50, p99, p99.9, max).</li>
 *   <li><b>Stalls seen by the application</b>: a separate thread sleeps 1 ms in a loop and records how much longer than
 *       1 ms each sleep took (the idea behind jHiccup). Any pause that stops application threads shows up here whatever
 *       its cause (GC, the operating system, other programs). An idle baseline of {@code --baseline-seconds} is measured
 *       first so the noise of this machine's timer and scheduler can be told apart from GC.</li>
 *   <li><b>Collections reported by the JVM</b>: every collection with the duration the JVM gives for it. For concurrent
 *       collectors the duration of a "cycle" includes time the application kept running, so it is not a pause; pass
 *       {@code -Xlog:gc,gc+phases:file=gc.log} and run {@code --summarize-gc-log gc.log} to get the stop-the-world pauses
 *       themselves from the JVM's own log.</li>
 * </ol>
 *
 * <p>What it does not measure: the network, the disk, Raft, or a server process (this is the cache in one JVM), and it
 * says nothing about other collectors or heap sizes than the ones you run it with. Compare collectors by running it again
 * with different flags (see scripts/gc-pause.bat and scripts/gc-pause.sh).
 *
 * <p>Run: {@code java -Xms2g -Xmx2g -XX:+UseG1GC "-Xlog:gc,gc+phases:file=gc-g1.log" -cp target/classes com.distri.cache.GcPauseBenchmark
 * [--entries N] [--value-bytes N] [--threads N] [--seconds N] [--baseline-seconds N]}
 */
public final class GcPauseBenchmark {

    private static final int BIN_NANOS = 10_000;      // histogram resolution: 10 microseconds
    private static final int BINS = 200_000;          // up to 2 s; slower samples go to "over" and are tracked exactly in max

    private GcPauseBenchmark() {}

    /** A fixed-resolution histogram, one per thread, merged at the end. */
    private static final class Histogram {
        final long[] bins = new long[BINS];
        long over;
        long count;
        long max;

        void record(long nanos) {
            count++;
            if (nanos > max) {
                max = nanos;
            }
            long bin = nanos / BIN_NANOS;
            if (bin >= BINS) {
                over++;
            } else {
                bins[(int) bin]++;
            }
        }

        void add(Histogram other) {
            for (int i = 0; i < BINS; i++) {
                bins[i] += other.bins[i];
            }
            over += other.over;
            count += other.count;
            max = Math.max(max, other.max);
        }

        /** Upper edge of the bin holding the given quantile, in milliseconds. */
        double quantileMillis(double q) {
            if (count == 0) {
                return 0;
            }
            long wanted = (long) Math.ceil(q * count);
            long seen = 0;
            for (int i = 0; i < BINS; i++) {
                seen += bins[i];
                if (seen >= wanted) {
                    return (i + 1) * BIN_NANOS / 1e6;
                }
            }
            return max / 1e6;
        }
    }

    private record Gc(String name, String action, String cause, long millis) {}

    public static void main(String[] args) throws Exception {
        int entries = 1_000_000;
        int valueBytes = 100;
        int threads = 4;
        int seconds = 30;
        int baselineSeconds = 5;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--entries" -> entries = Integer.parseInt(args[++i]);
                case "--value-bytes" -> valueBytes = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                case "--baseline-seconds" -> baselineSeconds = Integer.parseInt(args[++i]);
                case "--summarize-gc-log" -> {
                    summarizeLog(Path.of(args[++i]));
                    return;
                }
                default -> {
                    System.err.println("usage: GcPauseBenchmark [--entries N] [--value-bytes N] [--threads N] [--seconds N]"
                            + " [--baseline-seconds N] | --summarize-gc-log FILE");
                    System.exit(2);
                }
            }
        }
        Runtime rt = Runtime.getRuntime();
        System.out.println("java " + System.getProperty("java.version") + ", " + rt.availableProcessors() + " CPUs, "
                + System.getProperty("os.name") + ", max heap " + rt.maxMemory() / (1024 * 1024) + " MiB");
        System.out.println("JVM flags: " + ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println("collectors: " + ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName).toList());
        System.out.println(entries + " entries of " + valueBytes + "-byte values, " + threads + " threads, 90% get / 10% put, "
                + seconds + " s measured, " + baselineSeconds + " s idle baseline");
        System.out.println();

        List<Gc> collections = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean listening = new AtomicBoolean();
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (bean instanceof NotificationEmitter emitter) {
                emitter.addNotificationListener((notification, handback) -> {
                    if (!listening.get() || !notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
                        return;
                    }
                    GarbageCollectionNotificationInfo info =
                            GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                    collections.add(new Gc(info.getGcName(), info.getGcAction(), info.getGcCause(), info.getGcInfo().getDuration()));
                }, null, null);
            }
        }

        // 1. the machine's own noise, with nothing else running
        Histogram baseline = hiccups(baselineSeconds);

        // 2. fill the cache (not measured), then run the workload
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(entries * 2);
        String value = "x".repeat(valueBytes);
        long fillStart = System.nanoTime();
        for (int i = 0; i < entries; i++) {
            cache.put("key-" + i, value + i);
        }
        System.out.printf("filled %d entries in %.1f s (not measured)%n%n", cache.size(), (System.nanoTime() - fillStart) / 1e9);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean measuring = new AtomicBoolean();
        Histogram[] latencies = new Histogram[threads];
        Thread[] workers = new Thread[threads];
        final int keyRange = entries;
        for (int t = 0; t < threads; t++) {
            Histogram h = new Histogram();
            latencies[t] = h;
            workers[t] = new Thread(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                long n = 0;
                while (!stop.get()) {
                    String key = "key-" + random.nextInt(keyRange);
                    long start = System.nanoTime();
                    if (random.nextInt(10) == 0) {
                        cache.put(key, value + (n++));
                    } else {
                        cache.get(key);
                    }
                    long took = System.nanoTime() - start;
                    if (measuring.get()) {
                        h.record(took);
                    }
                }
            }, "gc-bench-" + t);
            workers[t].start();
        }
        Thread.sleep(3_000); // warm-up (JIT), not measured
        Histogram[] hiccupBox = new Histogram[1];
        final int measuredSeconds = seconds;
        Thread meter = new Thread(() -> hiccupBox[0] = hiccups(measuredSeconds), "hiccup");
        long gcCountBefore = totalGcCount();
        listening.set(true);
        measuring.set(true);
        meter.start();
        meter.join();
        measuring.set(false);
        listening.set(false);
        stop.set(true);
        for (Thread w : workers) {
            w.join();
        }
        Histogram all = new Histogram();
        for (Histogram h : latencies) {
            all.add(h);
        }

        System.out.printf("%-34s %9s %9s %9s %9s %12s%n", "", "p50 ms", "p99 ms", "p99.9 ms", "max ms", "samples");
        row("request latency under load", all);
        row("stalls, idle baseline (noise)", baseline);
        row("stalls, under load", hiccupBox[0]);
        System.out.println("(percentiles are the upper edge of a 10-microsecond bin; max is exact; a stall is how much longer a"
                + " 1 ms sleep took)");
        System.out.println();
        System.out.println("collections during the measured period: " + (totalGcCount() - gcCountBefore) + " (JVM counters)");
        java.util.Map<String, long[]> byName = new java.util.TreeMap<>();
        synchronized (collections) {
            for (Gc g : collections) {
                long[] s = byName.computeIfAbsent(g.name() + " / " + g.action() + " / " + g.cause(), k -> new long[3]);
                s[0]++;
                s[1] += g.millis();
                s[2] = Math.max(s[2], g.millis());
            }
        }
        System.out.printf("%-70s %6s %10s %8s%n", "collector / action / cause", "count", "total ms", "max ms");
        for (var e : byName.entrySet()) {
            System.out.printf("%-70s %6d %10d %8d%n", e.getKey(), e.getValue()[0], e.getValue()[1], e.getValue()[2]);
        }
        System.out.println("(durations are as reported by the JVM; for concurrent collectors a cycle includes time the application"
                + " kept running. Use -Xlog:gc,gc+phases and --summarize-gc-log for the stop-the-world pauses.)");
        cache.shutdown();
        System.exit(0);
    }

    private static long totalGcCount() {
        long n = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            n += Math.max(0, b.getCollectionCount());
        }
        return n;
    }

    private static void row(String label, Histogram h) {
        System.out.printf("%-34s %9.2f %9.2f %9.2f %9.2f %12d%n", label, h.quantileMillis(0.5), h.quantileMillis(0.99),
                h.quantileMillis(0.999), h.max / 1e6, h.count);
    }

    /** Sleeps 1 ms repeatedly for the given time and records how much longer than 1 ms each sleep took. */
    private static Histogram hiccups(int seconds) {
        Histogram h = new Histogram();
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        while (System.nanoTime() < end) {
            long start = System.nanoTime();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return h;
            }
            h.record(Math.max(0, System.nanoTime() - start - 1_000_000L));
        }
        return h;
    }

    // ------------------------------------------------------------------ the JVM's own log

    private static final Pattern PAUSE = Pattern.compile("Pause.* (\\d+(?:[.,]\\d+)?)ms\\s*$");

    /** Sums the stop-the-world pauses in a {@code -Xlog:gc,gc+phases} file: lines containing "Pause" and ending in a time. */
    static void summarizeLog(Path file) throws java.io.IOException {
        List<Double> pauses = new ArrayList<>();
        java.util.Map<String, double[]> byKind = new java.util.TreeMap<>();
        for (String line : Files.readAllLines(file)) {
            Matcher m = PAUSE.matcher(line);
            if (!line.contains("Pause") || !m.find()) {
                continue;
            }
            double ms = Double.parseDouble(m.group(1).replace(',', '.'));
            pauses.add(ms);
            String kind = line.substring(line.indexOf("Pause")).replaceAll("\\s+\\d+[KMGB]->.*$", "").replaceAll("\\s+\\d+(?:[.,]\\d+)?ms\\s*$", "").trim();
            double[] s = byKind.computeIfAbsent(kind, k -> new double[3]);
            s[0]++;
            s[1] += ms;
            s[2] = Math.max(s[2], ms);
        }
        if (pauses.isEmpty()) {
            System.out.println("no pause lines found in " + file + " (was the JVM started with -Xlog:gc,gc+phases:file=...?)");
            return;
        }
        java.util.Collections.sort(pauses);
        double total = 0;
        for (double p : pauses) {
            total += p;
        }
        System.out.printf("%d pauses in %s: total %.1f ms, p50 %.2f ms, p99 %.2f ms, max %.2f ms%n", pauses.size(), file, total,
                pauses.get(pauses.size() / 2), pauses.get((int) Math.min(pauses.size() - 1, pauses.size() * 0.99)),
                pauses.get(pauses.size() - 1));
        System.out.printf("%-40s %6s %10s %8s%n", "pause kind", "count", "total ms", "max ms");
        for (var e : byKind.entrySet()) {
            System.out.printf("%-40s %6.0f %10.1f %8.2f%n", e.getKey(), e.getValue()[0], e.getValue()[1], e.getValue()[2]);
        }
    }
}
