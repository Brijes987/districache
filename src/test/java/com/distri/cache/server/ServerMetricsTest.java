package com.distri.cache.server;

import static com.distri.cache.metrics.MetricsText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.distri.cache.ShardedLRUCache;
import com.distri.cache.metrics.MetricsRegistry;
import com.distri.cache.metrics.MetricsServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The single-server metrics, checked against what a real client did over a real socket. */
class ServerMetricsTest {

    private static final String CMD = "districache_commands_total{command=\"%s\"}";

    @Test
    void countsCommandsHitsMissesAndRecordsOneLatencySamplePerCommand() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
        try (CacheServer server = new CacheServer(cache, 0).enableMetrics(registry).start();
                RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "a", "1");
            c.call("SET", "b", "2");
            c.call("GET", "a");     // hit
            c.call("GET", "b");     // hit
            c.call("GET", "zzz");   // miss
            c.call("DEL", "a");
            c.call("PING");
            c.call("GET", "a");     // miss: deleted

            String text = registry.render();
            assertEquals(2, value(text, String.format(CMD, "SET")), 0);
            assertEquals(4, value(text, String.format(CMD, "GET")), 0);
            assertEquals(1, value(text, String.format(CMD, "DEL")), 0);
            assertEquals(1, value(text, String.format(CMD, "PING")), 0);
            assertEquals(2, value(text, "districache_cache_hits_total"), 0);
            assertEquals(2, value(text, "districache_cache_misses_total"), 0);
            assertEquals(0.5, value(text, "districache_cache_hit_ratio"), 1e-9);
            assertEquals(8, value(text, "districache_command_duration_seconds_count"), 0); // one sample per command
            assertEquals(8, value(text, "districache_command_duration_seconds_bucket{le=\"+Inf\"}"), 0);
            assertTrue(value(text, "districache_command_duration_seconds_sum") > 0);
            double p50 = value(text, "districache_command_duration_estimated_seconds{quantile=\"0.5\"}");
            double p999 = value(text, "districache_command_duration_estimated_seconds{quantile=\"0.999\"}");
            assertTrue(p50 > 0 && p999 >= p50, "p50=" + p50 + " p99.9=" + p999);
            assertEquals(1, value(text, "districache_cache_entries"), 0); // only "b" is left
            assertEquals(100, value(text, "districache_cache_capacity"), 0);
            assertEquals(1, value(text, "districache_connections"), 0);
            assertEquals(1, value(text, "districache_connections_accepted_total"), 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void theHitRatioIsNaNUntilTheFirstGet() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 2, 0);
        try (CacheServer server = new CacheServer(cache, 0).enableMetrics(registry).start()) {
            assertTrue(Double.isNaN(value(registry.render(), "districache_cache_hit_ratio")));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void anUnknownCommandIsCountedAsOtherSoClientsCannotCreateSeries() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 2, 0);
        try (CacheServer server = new CacheServer(cache, 0).enableMetrics(registry).start();
                RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("FROBNICATE").type());
            assertEquals('-', c.call("evil\"}\nname{x=\"y").type());
            String text = registry.render();
            assertEquals(2, value(text, String.format(CMD, "OTHER")), 0);
            assertFalse(text.contains("FROBNICATE"));
            assertFalse(text.contains("evil"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void wrongPasswordsAreCounted() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 2, 0);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 8,
                new CacheServer.Security("right", null)).enableMetrics(registry).start();
                RespTestClient c = new RespTestClient(server.port())) {
            assertEquals('-', c.call("AUTH", "wrong").type());
            assertEquals('-', c.call("AUTH", "wrong again").type());
            assertEquals('+', c.call("AUTH", "right").type());
            assertEquals(2, value(registry.render(), "districache_auth_failures_total"), 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void connectionsRefusedAtTheLimitAreCounted() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 2, 0);
        try (CacheServer server = new CacheServer(cache, CacheServer.DEFAULT_HOST, 0, 1).enableMetrics(registry).start();
                RespTestClient first = new RespTestClient(server.port())) {
            assertEquals('+', first.call("PING").type()); // the only slot is taken
            try (RespTestClient second = new RespTestClient(server.port())) {
                second.sendRaw(new byte[0]);
                for (int i = 0; i < 100 && value(registry.render(), "districache_connections_rejected_total") < 1; i++) {
                    Thread.sleep(20);
                }
            }
            String text = registry.render();
            assertEquals(1, value(text, "districache_connections_rejected_total"), 0);
            assertEquals(1, value(text, "districache_connections_accepted_total"), 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void metricsMustBeEnabledBeforeStart() throws Exception {
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(10, 2, 0);
        try (CacheServer server = new CacheServer(cache, 0).start()) {
            assertThrows(IllegalStateException.class, () -> server.enableMetrics(new MetricsRegistry()));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void theLogSizeAppearsWhenPersistenceIsOn() throws Exception {
        Path dir = Files.createTempDirectory("metrics-aof");
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
        AppendOnlyLog log = AppendOnlyLog.open(dir.resolve("a.aof"), AppendOnlyLog.FsyncPolicy.NO, 0, cache);
        try (CacheServer server = new CacheServer(cache, "127.0.0.1", 0, 8, log).enableMetrics(registry).start();
                RespTestClient c = new RespTestClient(server.port())) {
            double before = value(registry.render(), "districache_aof_size_bytes");
            c.call("SET", "k", "v");
            assertTrue(value(registry.render(), "districache_aof_size_bytes") > before);
        } finally {
            log.close();
            cache.shutdown();
        }
    }

    @Test
    void theNumbersAreAvailableOverHttpFromARunningServer() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
        try (CacheServer server = new CacheServer(cache, 0).enableMetrics(registry).start();
                MetricsServer http = new MetricsServer(registry, "127.0.0.1", 0).start();
                RespTestClient c = new RespTestClient(server.port())) {
            c.call("SET", "x", "y");
            c.call("GET", "x");
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + http.port() + "/metrics")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertEquals(1, value(response.body(), "districache_cache_hits_total"), 0);
            assertEquals(1, value(response.body(), String.format(CMD, "SET")), 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void manyConnectionsCountedWithoutLoss() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        // Big enough that nothing is evicted: a key another client's SETs push out between this client's SET and GET
        // would be a miss, and this test counts hits (it failed once with 3999 of 4000 for that reason).
        ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100_000, 4, 0);
        int clients = 8;
        int each = 500;
        try (CacheServer server = new CacheServer(cache, 0).enableMetrics(registry).start()) {
            Thread[] threads = new Thread[clients];
            Throwable[] failure = new Throwable[1];
            for (int t = 0; t < clients; t++) {
                final int id = t;
                threads[t] = new Thread(() -> {
                    try (RespTestClient c = new RespTestClient(server.port())) {
                        for (int i = 0; i < each; i++) {
                            c.call("SET", "k" + id + "-" + i, "v");
                            c.call("GET", "k" + id + "-" + i);
                        }
                    } catch (Throwable e) {
                        failure[0] = e;
                    }
                });
                threads[t].start();
            }
            for (Thread th : threads) {
                th.join();
            }
            assertEquals(null, failure[0]);
            String text = registry.render();
            assertEquals(clients * each, value(text, String.format(CMD, "SET")), 0);
            assertEquals(clients * each, value(text, String.format(CMD, "GET")), 0);
            assertEquals(clients * each, value(text, "districache_cache_hits_total"), 0);
            assertEquals(2L * clients * each, value(text, "districache_command_duration_seconds_count"), 0);
        } finally {
            cache.shutdown();
        }
    }
}
