package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;

/**
 * Starts a cache server. Options: {@code --host H} (default 127.0.0.1), {@code --port P} (default 6380),
 * {@code --capacity N} (default 100000), {@code --shards N} (default 16), {@code --max-clients N} (default 128).
 * Persistence is off unless {@code --aof PATH} is given; then {@code --fsync always|everysec|no} (default
 * everysec) and {@code --aof-compact-bytes N} (default 64 MiB, 0 turns automatic compaction off) apply.
 *
 * <p>Security: {@code --password-file PATH} (or the DISTRICACHE_PASSWORD environment variable) requires clients to
 * send AUTH; {@code --tls-keystore PATH} turns on TLS, with the keystore password in the file given by
 * {@code --tls-keystore-password-file} or in DISTRICACHE_KEYSTORE_PASSWORD. Passwords are deliberately not accepted
 * as command-line values, because those show up in process listings.
 *
 * <p>Replication (asynchronous, primary to replicas): {@code --allow-replicas} lets this server act as a primary;
 * {@code --replicaof HOST:PORT} starts it as a read-only replica of that primary. {@code --repl-backlog-bytes N}
 * (default 16 MiB) is how much recent history a primary keeps for replicas that reconnect. A replica reaches a
 * primary that needs a password or TLS with {@code --primary-password-file} (or DISTRICACHE_PRIMARY_PASSWORD) and
 * {@code --primary-truststore PATH} with {@code --primary-truststore-password-file} (or DISTRICACHE_TRUSTSTORE_PASSWORD).
 * While persistence or replication is on, writes are serialised.
 */
public final class ServerMain {

    private ServerMain() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = CacheServer.DEFAULT_HOST;
        int port = 6380;
        int capacity = 100_000;
        int shards = ShardedLRUCache.DEFAULT_SHARDS;
        int maxClients = CacheServer.DEFAULT_MAX_CLIENTS;
        String aofPath = null;
        AppendOnlyLog.FsyncPolicy fsync = AppendOnlyLog.FsyncPolicy.EVERYSEC;
        long compactBytes = AppendOnlyLog.DEFAULT_COMPACTION_THRESHOLD_BYTES;
        String passwordFile = null;
        boolean allowReplicas = false;
        String replicaOf = null;
        long backlogBytes = ReplicationHub.DEFAULT_BACKLOG_BYTES;
        String primaryPasswordFile = null;
        String primaryTruststore = null;
        String primaryTruststorePasswordFile = null;
        String keystorePath = null;
        String keystorePasswordFile = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (flag.equals("--allow-replicas")) {
                allowReplicas = true;
                continue;
            }
            if (i + 1 >= args.length) {
                usage("missing value for " + flag);
                return;
            }
            String value = args[++i];
            try {
                switch (flag) {
                    case "--host" -> host = value;
                    case "--port" -> port = Integer.parseInt(value);
                    case "--capacity" -> capacity = Integer.parseInt(value);
                    case "--shards" -> shards = Integer.parseInt(value);
                    case "--max-clients" -> maxClients = Integer.parseInt(value);
                    case "--aof" -> aofPath = value;
                    case "--fsync" -> fsync = AppendOnlyLog.FsyncPolicy.parse(value);
                    case "--aof-compact-bytes" -> compactBytes = Long.parseLong(value);
                    case "--password-file" -> passwordFile = value;
                    case "--replicaof" -> replicaOf = value;
                    case "--repl-backlog-bytes" -> backlogBytes = Long.parseLong(value);
                    case "--primary-password-file" -> primaryPasswordFile = value;
                    case "--primary-truststore" -> primaryTruststore = value;
                    case "--primary-truststore-password-file" -> primaryTruststorePasswordFile = value;
                    case "--tls-keystore" -> keystorePath = value;
                    case "--tls-keystore-password-file" -> keystorePasswordFile = value;
                    default -> {
                        usage("unknown option " + flag);
                        return;
                    }
                }
            } catch (NumberFormatException e) {
                usage("not a number: " + value);
                return;
            } catch (IllegalArgumentException e) {
                usage(e.getMessage());
                return;
            }
        }
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(capacity, shards, 1_000);
        CacheServer.Security security;
        try {
            String password = secret(passwordFile, "DISTRICACHE_PASSWORD");
            javax.net.ssl.SSLContext tls = null;
            if (keystorePath != null) {
                String keystorePassword = secret(keystorePasswordFile, "DISTRICACHE_KEYSTORE_PASSWORD");
                if (keystorePassword == null) {
                    usage("--tls-keystore needs the keystore password in --tls-keystore-password-file or DISTRICACHE_KEYSTORE_PASSWORD");
                    return;
                }
                tls = Tls.serverContext(java.nio.file.Path.of(keystorePath), keystorePassword.toCharArray());
            }
            security = new CacheServer.Security(password, tls);
        } catch (IOException | IllegalArgumentException e) {
            usage(e.getMessage());
            return;
        }
        boolean loopback = host.equals("127.0.0.1") || host.equals("localhost") || host.equals("::1");
        if (!loopback && security.password() == null) {
            System.err.println("WARNING: listening on " + host + " with no password: anyone who can reach this port can read, "
                    + "change and delete all data.");
        }
        if (!loopback && security.password() != null && security.tlsContext() == null) {
            System.err.println("WARNING: a password is set but TLS is off, so the password and all data cross the network unencrypted.");
        }
        AppendOnlyLog log = null;
        if (aofPath != null) {
            log = AppendOnlyLog.open(java.nio.file.Path.of(aofPath), fsync, compactBytes, cache);
            AppendOnlyLog.ReplayResult r = log.replayResult();
            System.out.println("Append-only log " + aofPath + ": replayed " + r.records() + " records"
                    + (r.truncatedBytes() > 0 ? ", dropped " + r.truncatedBytes() + " bytes of an incomplete last record" : "")
                    + "; " + cache.size() + " live entries; fsync " + fsync.name().toLowerCase());
        }
        final AppendOnlyLog finalLog = log;
        Replication replication = null;
        String primaryHost = null;
        int primaryPort = 0;
        if (allowReplicas || replicaOf != null) {
            try {
                if (replicaOf != null) {
                    int colon = replicaOf.lastIndexOf(':');
                    if (colon <= 0) {
                        usage("--replicaof must look like HOST:PORT");
                        return;
                    }
                    primaryHost = replicaOf.substring(0, colon);
                    primaryPort = Integer.parseInt(replicaOf.substring(colon + 1));
                }
                com.distri.cache.client.ClientSecurity linkSecurity = com.distri.cache.client.ClientSecurity.NONE;
                String linkPassword = secret(primaryPasswordFile, "DISTRICACHE_PRIMARY_PASSWORD");
                if (primaryTruststore != null) {
                    String trustPassword = secret(primaryTruststorePasswordFile, "DISTRICACHE_TRUSTSTORE_PASSWORD");
                    if (trustPassword == null) {
                        usage("--primary-truststore needs its password in --primary-truststore-password-file or DISTRICACHE_TRUSTSTORE_PASSWORD");
                        return;
                    }
                    linkSecurity = com.distri.cache.client.ClientSecurity.tls(java.nio.file.Path.of(primaryTruststore),
                            trustPassword.toCharArray(), linkPassword);
                } else if (linkPassword != null) {
                    linkSecurity = com.distri.cache.client.ClientSecurity.password(linkPassword);
                }
                replication = new Replication(cache, log, Replication.Config.defaults()
                        .withBacklog(backlogBytes).withSecurity(linkSecurity));
            } catch (IOException | IllegalArgumentException e) {
                usage(e.getMessage());
                return;
            }
        }
        final Replication finalReplication = replication;
        CacheServer server = new CacheServer(cache, host, port, maxClients, log, security, replication).start();
        if (replication != null && primaryHost != null) {
            replication.replicaOf(primaryHost, primaryPort);
            System.out.println("Replicating from " + primaryHost + ":" + primaryPort + " (read-only replica)");
        } else if (replication != null) {
            System.out.println("Accepting replicas (primary). Writes are serialised while replication is on.");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            if (finalReplication != null) {
                finalReplication.close();
            }
            cache.shutdown();
            if (finalLog != null) {
                try {
                    finalLog.close();
                } catch (IOException e) {
                    System.err.println("closing the append-only log failed: " + e);
                }
            }
        }));
        System.out.println("DistriCache listening on " + host + ":" + server.port()
                + " (capacity " + capacity + ", " + cache.shardCount() + " shards; password "
                + (security.password() != null ? "required" : "not required") + ", TLS "
                + (security.tlsContext() != null ? "on" : "off") + "). Ctrl+C to stop.");
        // The server threads are daemon threads, so keep the main thread alive until the process is stopped.
        Thread.currentThread().join();
    }

    /** Reads a secret from a file (first line) if given, else from an environment variable; null if neither is set. */
    private static String secret(String file, String environmentVariable) throws IOException {
        String value;
        if (file != null) {
            java.util.List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Path.of(file));
            value = lines.isEmpty() ? "" : lines.get(0).strip();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(file + " is empty");
            }
        } else {
            value = System.getenv(environmentVariable);
            if (value != null && value.isEmpty()) {
                value = null;
            }
        }
        return value;
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: ServerMain [--host H] [--port P] [--capacity N] [--shards N] [--max-clients N]\n"
                + "             [--aof PATH [--fsync always|everysec|no] [--aof-compact-bytes N]]\n"
                + "             [--password-file PATH] [--tls-keystore PATH [--tls-keystore-password-file PATH]]\n"
                + "             [--allow-replicas | --replicaof HOST:PORT] [--repl-backlog-bytes N]\n"
                + "             [--primary-password-file PATH] [--primary-truststore PATH [--primary-truststore-password-file PATH]]");
    }
}
