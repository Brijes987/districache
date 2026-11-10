package com.distri.cache.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class MetricsRegistryTest {

    private static final Pattern SAMPLE = Pattern.compile(
            "^[a-zA-Z_:][a-zA-Z0-9_:]*(\\{([a-zA-Z_][a-zA-Z0-9_]*=\"([^\"\\\\\\n]|\\\\.)*\",?)*\\})? (-?[0-9][0-9.eE+-]*|NaN|\\+Inf|-Inf)$");

    /** Every line must be a comment or a well-formed sample, and every family must be declared before its samples. */
    private static void assertWellFormed(String text) {
        assertTrue(text.endsWith("\n"));
        String family = null;
        for (String line : text.split("\n")) {
            if (line.startsWith("# HELP ")) {
                continue;
            }
            if (line.startsWith("# TYPE ")) {
                family = line.split(" ")[2];
                continue;
            }
            assertTrue(SAMPLE.matcher(line).matches(), "not a valid sample line: " + line);
            assertTrue(family != null && line.startsWith(family), "sample outside its family: " + line);
        }
    }

    @Test
    void aCounterIsRenderedWithHelpAndType() {
        MetricsRegistry r = new MetricsRegistry();
        LongAdder c = r.counter("demo_total", "A demo.");
        c.add(3);
        assertEquals("# HELP demo_total A demo.\n# TYPE demo_total counter\ndemo_total 3\n", r.render());
    }

    @Test
    void theSameNameAndLabelsGiveTheSameCounter() {
        MetricsRegistry r = new MetricsRegistry();
        assertSame(r.counter("x_total", "h", "a", "1"), r.counter("x_total", "h", "a", "1"));
        assertTrue(r.counter("x_total", "h", "a", "1") != r.counter("x_total", "h", "a", "2"));
        String text = r.render();
        assertEquals(1, text.split("# TYPE").length - 1); // one family, two series
        assertWellFormed(text);
    }

    @Test
    void labelValuesAreEscaped() {
        MetricsRegistry r = new MetricsRegistry();
        r.counter("e_total", "h", "v", "a\"b\\c\nd").increment();
        String text = r.render();
        assertTrue(text.contains("e_total{v=\"a\\\"b\\\\c\\nd\"} 1"), text);
        assertWellFormed(text);
    }

    @Test
    void aGaugeIsReadAtRenderTime() {
        MetricsRegistry r = new MetricsRegistry();
        long[] value = {1};
        r.gauge("g", "h", () -> value[0]);
        assertTrue(r.render().contains("\ng 1\n"));
        value[0] = 7;
        assertTrue(r.render().contains("\ng 7\n"));
    }

    @Test
    void aGaugeThatThrowsDoesNotBreakThePage() {
        MetricsRegistry r = new MetricsRegistry();
        r.gauge("bad", "h", () -> {
            throw new IllegalStateException("boom");
        });
        r.counter("ok_total", "h").increment();
        String text = r.render();
        assertTrue(text.contains("bad NaN"));
        assertTrue(text.contains("ok_total 1"));
        assertWellFormed(text);
    }

    @Test
    void aHistogramHasCumulativeBucketsAndTheInfBucketEqualsTheCount() {
        MetricsRegistry r = new MetricsRegistry();
        LatencyHistogram h = r.histogram("lat_seconds", "h", "op", "get");
        h.recordNanos(3_000);        // <= 5us
        h.recordNanos(30_000);       // <= 50us
        h.recordNanos(30_000);
        h.recordNanos(20_000_000_000L); // overflow
        String text = r.render();
        assertWellFormed(text);
        assertEquals(1, MetricsText.value(text, "lat_seconds_bucket{op=\"get\",le=\"5.0E-6\"}"), 0);
        assertEquals(3, MetricsText.value(text, "lat_seconds_bucket{op=\"get\",le=\"5.0E-5\"}"), 0);
        assertEquals(3, MetricsText.value(text, "lat_seconds_bucket{op=\"get\",le=\"10\"}"), 0);
        assertEquals(4, MetricsText.value(text, "lat_seconds_bucket{op=\"get\",le=\"+Inf\"}"), 0);
        assertEquals(4, MetricsText.value(text, "lat_seconds_count{op=\"get\"}"), 0);
        assertEquals(20.000063, MetricsText.value(text, "lat_seconds_sum{op=\"get\"}"), 1e-9);
        // cumulative buckets never decrease
        double previous = 0;
        for (String line : text.split("\n")) {
            if (line.startsWith("lat_seconds_bucket")) {
                double v = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
                assertTrue(v >= previous, line);
                previous = v;
            }
        }
    }

    @Test
    void quantileGaugesAreRenderedAndNaNWhenEmpty() {
        MetricsRegistry r = new MetricsRegistry();
        LatencyHistogram h = r.histogram("lat_seconds", "h");
        r.quantileGauges("lat", "h", h, 0.5, 0.999);
        String empty = r.render();
        assertTrue(Double.isNaN(MetricsText.value(empty, "lat_estimated_seconds{quantile=\"0.5\"}")));
        assertTrue(MetricsText.has(empty, "lat_estimated_seconds{quantile=\"0.999\"}"));
        h.recordNanos(30_000);
        assertEquals(50e-6, MetricsText.value(r.render(), "lat_estimated_seconds{quantile=\"0.5\"}"), 1e-12);
        assertWellFormed(r.render());
    }

    @Test
    void badNamesLabelsAndTypeConflictsAreRejected() {
        MetricsRegistry r = new MetricsRegistry();
        assertThrows(IllegalArgumentException.class, () -> r.counter("1bad", "h"));
        assertThrows(IllegalArgumentException.class, () -> r.counter("has space", "h"));
        assertThrows(IllegalArgumentException.class, () -> r.counter("ok_total", "h", "odd"));
        assertThrows(IllegalArgumentException.class, () -> r.counter("ok_total", "h", "bad-label", "v"));
        assertThrows(IllegalArgumentException.class, () -> r.counter("ok_total", "h", "le", "v"));
        r.counter("thing", "h");
        assertThrows(IllegalArgumentException.class, () -> r.gauge("thing", "h", () -> 1));
        assertThrows(IllegalArgumentException.class, () -> r.histogram("thing", "h"));
    }

    @Test
    void renderingWhileOtherThreadsRegisterAndCountIsSafe() throws Exception {
        MetricsRegistry r = new MetricsRegistry();
        LongAdder c = r.counter("busy_total", "h");
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 20_000; i++) {
                c.increment();
                if (i % 100 == 0) {
                    r.counter("dyn_total", "h", "i", Integer.toString(i % 50)).increment();
                }
            }
        });
        writer.start();
        while (writer.isAlive()) {
            assertWellFormed(r.render());
        }
        writer.join();
        assertEquals(20_000, MetricsText.value(r.render(), "busy_total"), 0);
    }
}
