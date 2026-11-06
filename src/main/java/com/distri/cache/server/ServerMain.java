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
        String keystorePath = null;
        String keystorePasswordFile = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
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
        CacheServer server = new CacheServer(cache, host, port, maxClients, log, security).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
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
                + "             [--password-file PATH] [--tls-keystore PATH [--tls-keystore-password-file PATH]]");
    }
}
