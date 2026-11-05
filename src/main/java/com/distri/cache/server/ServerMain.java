package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;

/**
 * Starts a cache server. Options: {@code --host H} (default 127.0.0.1), {@code --port P} (default 6380),
 * {@code --capacity N} (default 100000), {@code --shards N} (default 16), {@code --max-clients N} (default 128).
 */
public final class ServerMain {

    private ServerMain() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = CacheServer.DEFAULT_HOST;
        int port = 6380;
        int capacity = 100_000;
        int shards = ShardedLRUCache.DEFAULT_SHARDS;
        int maxClients = CacheServer.DEFAULT_MAX_CLIENTS;
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
                    default -> {
                        usage("unknown option " + flag);
                        return;
                    }
                }
            } catch (NumberFormatException e) {
                usage("not a number: " + value);
                return;
            }
        }
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(capacity, shards, 1_000);
        CacheServer server = new CacheServer(cache, host, port, maxClients).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            cache.shutdown();
        }));
        System.out.println("DistriCache listening on " + host + ":" + server.port()
                + " (capacity " + capacity + ", " + cache.shardCount() + " shards). Ctrl+C to stop.");
        // The server threads are daemon threads, so keep the main thread alive until the process is stopped.
        Thread.currentThread().join();
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: ServerMain [--host H] [--port P] [--capacity N] [--shards N] [--max-clients N]");
    }
}
