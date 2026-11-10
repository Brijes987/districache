package com.distri.cache.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsServerTest {

    private static HttpResponse<String> send(HttpClient client, int port, String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void servesTheRegistryAtMetricsWithThePrometheusContentType() throws Exception {
        MetricsRegistry registry = new MetricsRegistry();
        registry.counter("hits_total", "h").add(5);
        try (MetricsServer server = new MetricsServer(registry, "127.0.0.1", 0).start()) {
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response = send(client, server.port(), "GET", "/metrics");
            assertEquals(200, response.statusCode());
            assertEquals(MetricsServer.CONTENT_TYPE, response.headers().firstValue("Content-Type").orElse(""));
            assertTrue(response.body().contains("hits_total 5"), response.body());

            registry.counter("hits_total", "h").add(1); // each scrape reads the current values
            assertTrue(send(client, server.port(), "GET", "/metrics").body().contains("hits_total 6"));
        }
    }

    @Test
    void healthzOtherPathsAndOtherMethods() throws Exception {
        try (MetricsServer server = new MetricsServer(new MetricsRegistry(), "127.0.0.1", 0).start()) {
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> health = send(client, server.port(), "GET", "/healthz");
            assertEquals(200, health.statusCode());
            assertEquals("ok\n", health.body());
            assertEquals(404, send(client, server.port(), "GET", "/nope").statusCode());
            HttpResponse<String> post = send(client, server.port(), "POST", "/metrics");
            assertEquals(405, post.statusCode());
            assertEquals("GET, HEAD", post.headers().firstValue("Allow").orElse(""));
            HttpResponse<String> head = send(client, server.port(), "HEAD", "/metrics");
            assertEquals(200, head.statusCode());
            assertEquals("", head.body());
        }
    }

    @Test
    void isNoLongerReachableAfterClose() throws Exception {
        MetricsServer server = new MetricsServer(new MetricsRegistry(), "127.0.0.1", 0).start();
        int port = server.port();
        HttpClient client = HttpClient.newHttpClient();
        assertEquals(200, send(client, port, "GET", "/healthz").statusCode());
        server.close();
        boolean refused = false;
        try {
            send(client, port, "GET", "/healthz");
        } catch (java.io.IOException e) {
            refused = true;
        }
        assertTrue(refused);
    }
}
