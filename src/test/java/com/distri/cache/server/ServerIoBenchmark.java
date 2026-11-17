package com.distri.cache.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Compares the two server I/O models by running each as its own process (so its memory and thread count are the
 * server's alone) and driving it from this process. Not a test: run it with
 *
 * <pre>
 * java -cp target/classes;target/test-classes com.distri.cache.server.ServerIoBenchmark --io nio --idle 2000
 * </pre>
 *
 * <p>Options: {@code --io blocking|nio} (required), {@code --idle N} idle connections to hold open (default 2000),
 * {@code --conns C} connections for the load phase (default 64), {@code --seconds S} per load run (default 5),
 * {@code --pipeline D} requests in flight per connection (default 1), {@code --io-threads N} for nio. The server's
 * heap is capped at 256 MiB; its capacity is 100,000 entries.
 *
 * <p>What it measures, and what it does not: (1) the server process's resident memory and thread count with 0 and
 * with N idle connections (each having sent one PING), after a forced GC; (2) closed-loop throughput and per-request
 * latency of {@code GET} on one small key. The load generator runs on the same machine and competes for the same
 * CPUs, so the absolute numbers are only meaningful next to the other server's numbers from the same machine and
 * session. Memory comes from {@code /proc} on Linux and {@code tasklist} (working set) on Windows; thread counts come
 * from {@code jcmd Thread.print}.
 */
public final class ServerIoBenchmark {

    private ServerIoBenchmark() {}

    public static void main(String[] args) throws Exception {
        String io = null;
        int idle = 2000;
        int conns = 64;
        int seconds = 5;
        int pipeline = 1;
        int ioThreads = 0;
        for (int i = 0; i < args.length; i += 2) {
            String v = args[i + 1];
            switch (args[i]) {
                case "--io" -> io = v;
                case "--idle" -> idle = Integer.parseInt(v);
                case "--conns" -> conns = Integer.parseInt(v);
                case "--seconds" -> seconds = Integer.parseInt(v);
                case "--pipeline" -> pipeline = Integer.parseInt(v);
                case "--io-threads" -> ioThreads = Integer.parseInt(v);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (io == null || !(io.equals("blocking") || io.equals("nio"))) {
            throw new IllegalArgumentException("--io blocking|nio is required");
        }
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m",
                "-cp", System.getProperty("java.class.path"), "com.distri.cache.server.ServerMain",
                "--port", String.valueOf(port), "--max-clients", String.valueOf(Math.max(idle, conns) + 64),
                "--capacity", "100000", "--io", io));
        if (ioThreads > 0) {
            cmd.addAll(List.of("--io-threads", String.valueOf(ioThreads)));
        }
        Process server = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try {
            waitUntilListening(port);
            long pid = server.pid();
            System.out.println("server: --io " + io + (ioThreads > 0 ? " --io-threads " + ioThreads : "") + ", pid " + pid
                    + ", " + Runtime.getRuntime().availableProcessors() + " CPUs, " + System.getProperty("java.version")
                    + ", " + System.getProperty("os.name"));

            // ---- idle connections
            Thread.sleep(1500);
            Sample base = sample(pid);
            System.out.println("baseline (0 connections):   threads " + base.threads + ", memory " + base.memoryMb() + " MiB");
            List<SocketChannel> held = new ArrayList<>();
            long openStart = System.nanoTime();
            ByteBuffer ping = ByteBuffer.wrap("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            ByteBuffer reply = ByteBuffer.allocate(16);
            for (int i = 0; i < idle; i++) {
                SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
                ping.rewind();
                ch.write(ping);
                reply.clear();
                while (reply.position() < 7) {
                    if (ch.read(reply) < 0) {
                        throw new IOException("server closed connection " + i);
                    }
                }
                held.add(ch);
            }
            long openMs = (System.nanoTime() - openStart) / 1_000_000;
            Thread.sleep(1500);
            Sample withIdle = sample(pid);
            System.out.println(idle + " idle connections open (took " + openMs + " ms): threads " + withIdle.threads
                    + ", memory " + withIdle.memoryMb() + " MiB");
            if (idle > 0) {
                System.out.printf("  extra threads %d, extra memory %.1f MiB, about %.1f KiB per connection%n",
                        withIdle.threads - base.threads, (withIdle.memoryKb - base.memoryKb) / 1024.0,
                        (withIdle.memoryKb - base.memoryKb) / (double) idle);
            }
            // is one of the idle connections still served?
            SocketChannel probe = held.isEmpty() ? null : held.get(held.size() / 2);
            if (probe != null) {
                ping.rewind();
                probe.write(ping);
                reply.clear();
                long t0 = System.nanoTime();
                while (reply.position() < 7) {
                    probe.read(reply);
                }
                System.out.printf("  a PING on an already-idle connection: %.2f ms%n", (System.nanoTime() - t0) / 1e6);
            }
            for (SocketChannel ch : held) {
                ch.close();
            }
            held.clear();
            Thread.sleep(1000);

            // ---- load
            try (Socket s = new Socket("127.0.0.1", port)) {
                s.getOutputStream().write("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes(StandardCharsets.US_ASCII));
                s.getInputStream().read(new byte[5]);
            }
            Load warm = load(port, conns, 2, pipeline);
            Load run = load(port, conns, seconds, pipeline);
            System.out.printf("load: %d connections, pipeline depth %d, %d s (after a 2 s warm-up of %.0f req/s)%n", conns,
                    pipeline, seconds, warm.perSecond());
            System.out.printf("  throughput %.0f requests/s; latency per request: median %.2f ms, p99 %.2f ms, max %.2f ms%n",
                    run.perSecond(), run.percentileMs(50), run.percentileMs(99), run.maxMs());
            Sample after = sample(pid);
            System.out.println("after load:                 threads " + after.threads + ", memory " + after.memoryMb() + " MiB");
        } finally {
            server.destroy();
            server.waitFor();
        }
    }

    // ---------------------------------------------------------------------------------------------- load generator

    private record Load(long completed, double seconds, long[] latenciesNanos) {
        double perSecond() {
            return completed / seconds;
        }

        double percentileMs(double p) {
            if (latenciesNanos.length == 0) {
                return Double.NaN;
            }
            return latenciesNanos[(int) Math.min(latenciesNanos.length - 1, Math.floor(p / 100 * latenciesNanos.length))] / 1e6;
        }

        double maxMs() {
            return latenciesNanos.length == 0 ? Double.NaN : latenciesNanos[latenciesNanos.length - 1] / 1e6;
        }
    }

    private static final byte[] GET = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int REPLY_BYTES = "$5\r\nvalue\r\n".length();

    private static final class Client {
        final SocketChannel ch;
        final long[] sentAt;
        int head;       // oldest request in flight
        int inFlight;
        int partial;    // bytes of the current reply already read
        Client(SocketChannel ch, int depth) {
            this.ch = ch;
            this.sentAt = new long[depth];
        }
    }

    /** Closed loop on one selector thread: each connection keeps {@code depth} requests in flight. */
    private static Load load(int port, int conns, int seconds, int depth) throws Exception {
        Selector selector = Selector.open();
        Client[] clients = new Client[conns];
        ByteBuffer request = ByteBuffer.allocate(GET.length * depth);
        for (int i = 0; i < depth; i++) {
            request.put(GET);
        }
        for (int i = 0; i < conns; i++) {
            SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
            ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            ch.configureBlocking(false);
            clients[i] = new Client(ch, depth);
            ch.register(selector, SelectionKey.OP_READ, clients[i]);
        }
        long[] lat = new long[4_000_000];
        int latCount = 0;
        long completed = 0;
        ByteBuffer in = ByteBuffer.allocate(64 * 1024);
        long start = System.nanoTime();
        for (Client c : clients) { // fill each pipeline
            request.rewind();
            long now = System.nanoTime();
            for (int d = 0; d < depth; d++) {
                c.sentAt[d] = now;
            }
            c.head = 0;
            c.inFlight = depth;
            c.ch.write(request);
        }
        long end = start + seconds * 1_000_000_000L;
        ByteBuffer one = ByteBuffer.wrap(GET);
        while (System.nanoTime() < end) {
            selector.select(100);
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                Client c = (Client) key.attachment();
                in.clear();
                int n = c.ch.read(in);
                if (n < 0) {
                    throw new IOException("server closed a load connection");
                }
                int total = c.partial + n;
                int replies = total / REPLY_BYTES;
                c.partial = total % REPLY_BYTES;
                long now = System.nanoTime();
                for (int r = 0; r < replies; r++) {
                    if (latCount < lat.length) {
                        lat[latCount++] = now - c.sentAt[c.head];
                    }
                    completed++;
                    c.sentAt[c.head] = now;
                    c.head = (c.head + 1) % depth;
                    one.rewind();
                    c.ch.write(one); // a small request into an empty send buffer: never partial in practice
                }
            }
        }
        double elapsed = (System.nanoTime() - start) / 1e9;
        for (Client c : clients) {
            c.ch.close();
        }
        selector.close();
        long[] sorted = Arrays.copyOf(lat, latCount);
        Arrays.sort(sorted);
        return new Load(completed, elapsed, sorted);
    }

    // ---------------------------------------------------------------------------------------------- measuring

    private record Sample(int threads, long memoryKb) {
        String memoryMb() {
            return memoryKb < 0 ? "n/a" : String.format("%.1f", memoryKb / 1024.0);
        }
    }

    private static Sample sample(long pid) throws Exception {
        run("jcmd", String.valueOf(pid), "GC.run");
        Thread.sleep(300);
        int threads = 0;
        for (String line : run("jcmd", String.valueOf(pid), "Thread.print")) {
            if (line.startsWith("\"")) {
                threads++;
            }
        }
        return new Sample(threads, residentKb(pid));
    }

    private static long residentKb(long pid) throws Exception {
        Path status = Path.of("/proc/" + pid + "/status");
        if (Files.exists(status)) {
            for (String line : Files.readAllLines(status)) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", ""));
                }
            }
        }
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            for (String line : run("tasklist", "/FI", "PID eq " + pid, "/FO", "CSV", "/NH")) {
                String[] parts = line.split("\",\"");
                if (parts.length >= 5) {
                    return Long.parseLong(parts[4].replaceAll("[^0-9]", ""));
                }
            }
        }
        return -1;
    }

    private static List<String> run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                lines.add(line);
            }
        }
        p.waitFor();
        return lines;
    }

    private static void waitUntilListening(int port) throws Exception {
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 500);
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        throw new IOException("the server did not start listening on port " + port + " (is target/classes built?)");
    }
}
