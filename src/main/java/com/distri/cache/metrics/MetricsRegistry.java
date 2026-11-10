package com.distri.cache.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;
import java.util.regex.Pattern;

/**
 * A small metrics registry that renders the Prometheus text exposition format (version 0.0.4), with no dependencies.
 *
 * <p>Three kinds of metric: counters (only go up), gauges (read from a supplier at scrape time) and
 * {@link LatencyHistogram}s. Each metric name is one <i>family</i> with a type and help text; a family can have
 * several label sets (for example {@code command="GET"} and {@code command="SET"}). Registering the same name and
 * labels twice returns the same counter or histogram; registering a name with a different type throws.
 *
 * <p>Labels are passed as alternating name and value strings and are escaped for you. Keep label values to a small
 * fixed set: every distinct combination is a separate time series.
 */
public final class MetricsRegistry {

    private static final Pattern NAME = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern LABEL_NAME = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private enum Type { COUNTER, GAUGE, HISTOGRAM }

    private static final class Family {
        final Type type;
        final String help;
        final Map<String, Object> series = new LinkedHashMap<>(); // rendered labels -> LongAdder | DoubleSupplier | LatencyHistogram

        Family(Type type, String help) {
            this.type = type;
            this.help = help;
        }
    }

    private final Map<String, Family> families = new LinkedHashMap<>();

    /** A counter; the returned object is what the caller increments. */
    public synchronized LongAdder counter(String name, String help, String... labels) {
        Family f = family(name, help, Type.COUNTER);
        return (LongAdder) f.series.computeIfAbsent(renderLabels(labels), k -> new LongAdder());
    }

    /**
     * A gauge whose value is read from {@code value} each time the registry is rendered. The supplier runs on the
     * scraping thread, so it must be thread-safe and quick. Registering the same labels again replaces the supplier.
     */
    public synchronized void gauge(String name, String help, DoubleSupplier value, String... labels) {
        family(name, help, Type.GAUGE).series.put(renderLabels(labels), value);
    }

    /** A latency histogram, rendered in seconds with cumulative buckets, {@code _sum} and {@code _count}. */
    public synchronized LatencyHistogram histogram(String name, String help, String... labels) {
        Family f = family(name, help, Type.HISTOGRAM);
        return (LatencyHistogram) f.series.computeIfAbsent(renderLabels(labels), k -> new LatencyHistogram());
    }

    /**
     * Also publishes estimated percentiles of {@code histogram} as a gauge family
     * {@code <name>_estimated_seconds{quantile="0.99"}}. They are bucket upper edges, not exact values (see
     * {@link LatencyHistogram}). With Prometheus you would normally compute them from the buckets with
     * {@code histogram_quantile}; these are for people reading the page by eye.
     */
    public void quantileGauges(String name, String help, LatencyHistogram histogram, double... quantiles) {
        for (double q : quantiles) {
            gauge(name + "_estimated_seconds", help, () -> histogram.quantile(q), "quantile", trim(q));
        }
    }

    private static String trim(double q) {
        String s = Double.toString(q);
        return s.endsWith(".0") ? s.substring(0, s.length() - 2) : s;
    }

    private Family family(String name, String help, Type type) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid metric name: " + name);
        }
        Family f = families.get(name);
        if (f == null) {
            f = new Family(type, help);
            families.put(name, f);
        } else if (f.type != type) {
            throw new IllegalArgumentException(name + " is already registered as a " + f.type.name().toLowerCase());
        }
        return f;
    }

    /** {@code name="value",name2="value2"}, escaped, in the order given; empty for no labels. */
    static String renderLabels(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("labels must be name, value pairs");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < labels.length; i += 2) {
            if (!LABEL_NAME.matcher(labels[i]).matches() || labels[i].startsWith("__") || labels[i].equals("le")) {
                throw new IllegalArgumentException("invalid label name: " + labels[i]);
            }
            if (i > 0) {
                sb.append(',');
            }
            sb.append(labels[i]).append("=\"").append(escape(labels[i + 1])).append('"');
        }
        return sb.toString();
    }

    private static String escape(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** The whole registry in the Prometheus text format. */
    public String render() {
        List<String> names;
        List<Family> snapshot;
        synchronized (this) { // copy the structure; the values are read outside the lock
            names = new ArrayList<>(families.keySet());
            snapshot = new ArrayList<>();
            for (Family f : families.values()) {
                Family copy = new Family(f.type, f.help);
                copy.series.putAll(f.series);
                snapshot.add(copy);
            }
        }
        StringBuilder out = new StringBuilder(4096);
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Family f = snapshot.get(i);
            out.append("# HELP ").append(name).append(' ').append(f.help.replace("\\", "\\\\").replace("\n", "\\n")).append('\n');
            out.append("# TYPE ").append(name).append(' ').append(f.type.name().toLowerCase()).append('\n');
            for (Map.Entry<String, Object> s : f.series.entrySet()) {
                String labels = s.getKey();
                switch (f.type) {
                    case COUNTER -> line(out, name, labels, ((LongAdder) s.getValue()).sum());
                    case GAUGE -> {
                        double v;
                        try {
                            v = ((DoubleSupplier) s.getValue()).getAsDouble();
                        } catch (RuntimeException e) {
                            v = Double.NaN; // a gauge that cannot be read right now must not break the whole page
                        }
                        line(out, name, labels, v);
                    }
                    case HISTOGRAM -> histogram(out, name, labels, (LatencyHistogram) s.getValue());
                }
            }
        }
        return out.toString();
    }

    private static void histogram(StringBuilder out, String name, String labels, LatencyHistogram h) {
        long[] counts = h.counts();
        long cumulative = 0;
        String prefix = labels.isEmpty() ? "" : labels + ",";
        for (int i = 0; i < counts.length; i++) {
            cumulative += counts[i];
            String le = i < LatencyHistogram.BOUNDS_SECONDS.length ? format(LatencyHistogram.BOUNDS_SECONDS[i]) : "+Inf";
            line(out, name + "_bucket", prefix + "le=\"" + le + "\"", cumulative);
        }
        line(out, name + "_sum", labels, h.sumSeconds());
        line(out, name + "_count", labels, cumulative); // equals the +Inf bucket, as the format requires
    }

    private static void line(StringBuilder out, String name, String labels, double value) {
        out.append(name);
        if (!labels.isEmpty()) {
            out.append('{').append(labels).append('}');
        }
        out.append(' ').append(format(value)).append('\n');
    }

    static String format(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "+Inf" : "-Inf";
        }
        if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            return Long.toString((long) v);
        }
        return Double.toString(v);
    }
}
