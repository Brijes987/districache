package com.distri.cache.metrics;

/** Test helper: reads one series' value out of Prometheus text. */
public final class MetricsText {

    private MetricsText() {}

    /**
     * The value of the series {@code series} (the name plus the exact label text, for example
     * {@code districache_commands_total{command="GET"}}), or NaN if the text has no such line.
     */
    public static double value(String text, String series) {
        for (String line : text.split("\n")) {
            if (line.startsWith(series + " ")) {
                String v = line.substring(series.length() + 1).trim();
                return switch (v) {
                    case "NaN" -> Double.NaN;
                    case "+Inf" -> Double.POSITIVE_INFINITY;
                    case "-Inf" -> Double.NEGATIVE_INFINITY;
                    default -> Double.parseDouble(v);
                };
            }
        }
        return Double.NaN;
    }

    public static boolean has(String text, String series) {
        for (String line : text.split("\n")) {
            if (line.startsWith(series + " ")) {
                return true;
            }
        }
        return false;
    }
}
