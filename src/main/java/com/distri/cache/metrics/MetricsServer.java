package com.distri.cache.metrics;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Serves a {@link MetricsRegistry} over HTTP using the JDK's built-in server: {@code GET /metrics} returns the
 * Prometheus text format and {@code GET /healthz} returns {@code ok}. Anything else is a 404, other methods a 405.
 *
 * <p><b>Security:</b> this endpoint has no authentication or TLS and shows operational detail, so it binds to the
 * loopback address by default. Put it behind a firewall or a reverse proxy before binding it elsewhere.
 */
public final class MetricsServer implements AutoCloseable {

    public static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final HttpServer http;
    private final ExecutorService pool;

    /** @param port the port to listen on; 0 picks a free one (see {@link #port()}) */
    public MetricsServer(MetricsRegistry registry, String host, int port) throws IOException {
        this.http = HttpServer.create(new InetSocketAddress(host, port), 16);
        this.pool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "metrics-http");
            t.setDaemon(true);
            return t;
        });
        http.setExecutor(pool);
        http.createContext("/metrics", exchange -> {
            try (exchange) {
                if (!allowed(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] body = registry.render().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
                respond(exchange, body);
            }
        });
        http.createContext("/healthz", exchange -> {
            try (exchange) {
                if (!allowed(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                respond(exchange, "ok\n".getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    private static boolean allowed(String method) {
        return method.equals("GET") || method.equals("HEAD");
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, byte[] body) throws IOException {
        if (exchange.getRequestMethod().equals("HEAD")) {
            exchange.sendResponseHeaders(200, -1);
            return;
        }
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    public MetricsServer start() {
        http.start();
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
        pool.shutdownNow();
        try {
            pool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
