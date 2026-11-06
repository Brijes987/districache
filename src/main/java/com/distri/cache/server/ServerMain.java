package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;

/**
 * Starts a cache server. Options: {@code --host H} (default 127.0.0.1), {@code --port P} (default 6380),
 * {@code --capacity N} (default 100000), {@code --shards N} (default 16), {@code --max-clients N} (default 128).
 * Persistence is off unless {@code --aof PATH} is given; then {@code --fsync always|everysec|no} (default
 * everysec) and {@code --aof-compact-bytes N} (default 64 MiB, 0 turns automatic compaction off) apply.
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
        AppendOnlyLog log = null;
        if (aofPath != null) {
            log = AppendOnlyLog.open(java.nio.file.Path.of(aofPath), fsync, compactBytes, cache);
            AppendOnlyLog.ReplayResult r = log.replayResult();
            System.out.println("Append-only log " + aofPath + ": replayed " + r.records() + " records"
                    + (r.truncatedBytes() > 0 ? ", dropped " + r.truncatedBytes() + " bytes of an incomplete last record" : "")
                    + "; " + cache.size() + " live entries; fsync " + fsync.name().toLowerCase());
        }
        final AppendOnlyLog finalLog = log;
        CacheServer server = new CacheServer(cache, host, port, maxClients, log).start();
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
                + " (capacity " + capacity + ", " + cache.shardCount() + " shards). Ctrl+C to stop.");
        // The server threads are daemon threads, so keep the main thread alive until the process is stopped.
        Thread.currentThread().join();
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: ServerMain [--host H] [--port P] [--capacity N] [--shards N] [--max-clients N]\n"
                + "             [--aof PATH [--fsync always|everysec|no] [--aof-compact-bytes N]]");
    }
}
