package com.distri.cache.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Starts one member of a replicated cache group.
 *
 * <pre>
 * --id N                       this server's id (a key in --members)
 * --members LIST               id=host:peerPort:clientPort,... for EVERY server in the group, including this one
 * --data DIR                   where this server keeps its Raft state (log, term, vote)
 * --peer-secret-file FILE      shared secret servers must prove they know to join (strongly recommended)
 * --client-secret-file FILE    shared secret clients must prove they know
 * --tick-ms N                  milliseconds per Raft tick (default 10); election timeout is 30 to 60 ticks
 * --capacity N                 cache capacity in entries (default 10000)
 * --no-fsync                   do not force writes to disk (FASTER, NOT DURABLE: only for experiments)
 * --max-batch N               group commit: up to N queued client requests share one log write and fsync
 *                              (default 256; 1 = every request is fsynced alone)
 * --metrics-port P             serve Prometheus metrics at http://127.0.0.1:P/metrics (--metrics-host H to bind elsewhere;
 *                              no authentication or TLS)
 * </pre>
 *
 * Passwords are read from files, not the command line, because command lines show up in process listings.
 * Every server in the group must be started with the same --members list. The group is fixed: there is no way to
 * add or remove servers while it runs.
 */
public final class ReplicatedServerMain {

    private ReplicatedServerMain() {}

    public static void main(String[] args) throws Exception {
        int id = -1;
        String membersText = null;
        String dataDir = null;
        String peerSecretFile = null;
        String clientSecretFile = null;
        long tickMillis = 10;
        int capacity = 10_000;
        boolean fsync = true;
        int maxBatch = ReplicatedServer.DEFAULT_MAX_BATCH;
        String metricsHost = "127.0.0.1";
        int metricsPort = -1;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--id" -> id = Integer.parseInt(value(args, ++i));
                    case "--members" -> membersText = value(args, ++i);
                    case "--data" -> dataDir = value(args, ++i);
                    case "--peer-secret-file" -> peerSecretFile = value(args, ++i);
                    case "--client-secret-file" -> clientSecretFile = value(args, ++i);
                    case "--tick-ms" -> tickMillis = Long.parseLong(value(args, ++i));
                    case "--capacity" -> capacity = Integer.parseInt(value(args, ++i));
                    case "--no-fsync" -> fsync = false;
                    case "--max-batch" -> maxBatch = Integer.parseInt(value(args, ++i));
                    case "--metrics-port" -> metricsPort = Integer.parseInt(value(args, ++i));
                    case "--metrics-host" -> metricsHost = value(args, ++i);
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (id < 0 || membersText == null || dataDir == null) {
                throw new IllegalArgumentException("--id, --members and --data are required");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println("usage: ReplicatedServerMain --id N --members 0=host:peerPort:clientPort,... --data DIR"
                    + " [--peer-secret-file F] [--client-secret-file F] [--tick-ms N] [--capacity N] [--no-fsync] [--max-batch N]"
                    + " [--metrics-port P [--metrics-host H]]");
            System.exit(2);
            return;
        }

        Map<Integer, ReplicatedServer.Member> members = parseMembers(membersText);
        ReplicatedServer.Config base = ReplicatedServer.Config.of(id, members, Path.of(dataDir)).withFsync(fsync);
        ReplicatedServer.Config config = new ReplicatedServer.Config(base.id(), base.members(), base.dataDir(),
                readSecret(peerSecretFile), readSecret(clientSecretFile),
                new com.distri.cache.raft.RaftConfig(30, 60, 6, 64, true, java.util.Set.of()), tickMillis, fsync,
                capacity, base.requestTimeoutMillis());
        if (!fsync) {
            System.err.println("WARNING: --no-fsync: acknowledged writes can be lost if the machine loses power.");
        }
        if (config.peerSecret() == null) {
            System.err.println("WARNING: no --peer-secret-file: anyone who can reach the peer port can inject Raft messages.");
        }
        ReplicatedServer server = new ReplicatedServer(config).setMaxBatch(maxBatch);
        com.distri.cache.metrics.MetricsServer metricsServer = null;
        if (metricsPort >= 0) {
            com.distri.cache.metrics.MetricsRegistry registry = new com.distri.cache.metrics.MetricsRegistry();
            server.enableMetrics(registry);
            registry.gauge("districache_up", "1 while the server process is running.", () -> 1);
            metricsServer = new com.distri.cache.metrics.MetricsServer(registry, metricsHost, metricsPort).start();
            System.out.println("Metrics at http://" + metricsHost + ":" + metricsServer.port() + "/metrics");
            if (!metricsHost.equals("127.0.0.1") && !metricsHost.equals("localhost") && !metricsHost.equals("::1")) {
                System.err.println("WARNING: the metrics endpoint has no authentication or TLS and is reachable from the network.");
            }
        }
        final com.distri.cache.metrics.MetricsServer finalMetrics = metricsServer;
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (finalMetrics != null) {
                finalMetrics.close();
            }
            server.close();
        }));
        System.out.println("server " + id + " ready: peers on port " + server.peerPort() + ", clients on port "
                + server.clientPort() + ", data in " + dataDir);
        System.out.flush();
        Thread.currentThread().join();
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + args[index - 1]);
        }
        return args[index];
    }

    private static byte[] readSecret(String file) throws IOException {
        if (file == null) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(Path.of(file));
        int end = bytes.length;
        while (end > 0 && (bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
            end--; // an editor's trailing newline is not part of the secret
        }
        if (end == 0) {
            throw new IllegalArgumentException("secret file " + file + " is empty");
        }
        return java.util.Arrays.copyOf(bytes, end);
    }

    /** Parses {@code 0=host:7000:7100,1=host:7001:7101}. */
    static Map<Integer, ReplicatedServer.Member> parseMembers(String text) {
        Map<Integer, ReplicatedServer.Member> members = new LinkedHashMap<>();
        for (String entry : text.split(",")) {
            String[] idAndAddress = entry.trim().split("=", 2);
            if (idAndAddress.length != 2) {
                throw new IllegalArgumentException("bad member '" + entry + "', expected id=host:peerPort:clientPort");
            }
            String[] parts = idAndAddress[1].split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("bad member '" + entry + "', expected id=host:peerPort:clientPort");
            }
            int memberId;
            int peerPort;
            int clientPort;
            try {
                memberId = Integer.parseInt(idAndAddress[0].trim());
                peerPort = Integer.parseInt(parts[1]);
                clientPort = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("bad member '" + entry + "': ids and ports must be numbers");
            }
            if (parts[0].isEmpty() || peerPort < 1 || peerPort > 65535 || clientPort < 1 || clientPort > 65535
                    || peerPort == clientPort) {
                throw new IllegalArgumentException("bad member '" + entry + "': need a host and two different ports");
            }
            if (members.put(memberId, new ReplicatedServer.Member(parts[0], peerPort, clientPort)) != null) {
                throw new IllegalArgumentException("duplicate member id " + memberId);
            }
        }
        if (members.isEmpty()) {
            throw new IllegalArgumentException("no members given");
        }
        return members;
    }
}
