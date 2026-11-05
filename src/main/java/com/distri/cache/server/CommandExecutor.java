package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * Runs one parsed command against the cache and writes the reply.
 *
 * <p>Supported: PING [message], ECHO message, GET key, SET key value [EX seconds | PX millis],
 * DEL key [key ...], DBSIZE, QUIT. Keys and values are UTF-8 text.
 */
final class CommandExecutor {

    enum Result { CONTINUE, CLOSE }

    private final ShardedLRUCache<String, String> cache;

    CommandExecutor(ShardedLRUCache<String, String> cache) {
        this.cache = cache;
    }

    Result execute(List<byte[]> args, RespWriter out) throws IOException {
        String name = text(args.get(0));
        String command = name.toUpperCase(Locale.ROOT);
        int argc = args.size();
        switch (command) {
            case "PING" -> {
                if (argc == 1) {
                    out.simple("PONG");
                } else if (argc == 2) {
                    out.bulk(args.get(1));
                } else {
                    arity(out, command);
                }
            }
            case "ECHO" -> {
                if (argc != 2) {
                    arity(out, command);
                } else {
                    out.bulk(args.get(1));
                }
            }
            case "GET" -> {
                if (argc != 2) {
                    arity(out, command);
                } else {
                    String value = cache.get(text(args.get(1))).orElse(null);
                    if (value == null) {
                        out.nullBulk();
                    } else {
                        out.bulk(value.getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
            case "SET" -> set(args, out);
            case "DEL" -> {
                if (argc < 2) {
                    arity(out, command);
                } else {
                    long removed = 0;
                    for (int i = 1; i < argc; i++) {
                        if (cache.delete(text(args.get(i)))) {
                            removed++;
                        }
                    }
                    out.integer(removed);
                }
            }
            case "DBSIZE" -> {
                if (argc != 1) {
                    arity(out, command);
                } else {
                    out.integer(cache.size());
                }
            }
            case "QUIT" -> {
                out.simple("OK");
                return Result.CLOSE;
            }
            default -> out.error("ERR unknown command '" + sanitize(name) + "'");
        }
        return Result.CONTINUE;
    }

    private void set(List<byte[]> args, RespWriter out) throws IOException {
        int argc = args.size();
        if (argc != 3 && argc != 5) {
            if (argc < 3) {
                arity(out, "SET");
            } else {
                out.error("ERR syntax error");
            }
            return;
        }
        String key = text(args.get(1));
        String value = text(args.get(2));
        if (argc == 3) {
            cache.put(key, value);
            out.simple("OK");
            return;
        }
        String option = text(args.get(3)).toUpperCase(Locale.ROOT);
        if (!option.equals("EX") && !option.equals("PX")) {
            out.error("ERR syntax error");
            return;
        }
        long amount;
        try {
            amount = Long.parseLong(text(args.get(4)));
        } catch (NumberFormatException e) {
            out.error("ERR value is not an integer or out of range");
            return;
        }
        long ttlMillis;
        if (option.equals("EX")) {
            if (amount > Long.MAX_VALUE / 1000) {
                out.error("ERR invalid expire time in 'set' command");
                return;
            }
            ttlMillis = amount * 1000;
        } else {
            ttlMillis = amount;
        }
        if (ttlMillis <= 0) {
            out.error("ERR invalid expire time in 'set' command");
            return;
        }
        cache.put(key, value, ttlMillis);
        out.simple("OK");
    }

    private static void arity(RespWriter out, String command) throws IOException {
        out.error("ERR wrong number of arguments for '" + command.toLowerCase(Locale.ROOT) + "' command");
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Error text must stay on one line and be bounded, whatever the client sent. */
    private static String sanitize(String s) {
        String clean = s.replaceAll("[\\r\\n]", " ");
        return clean.length() > 64 ? clean.substring(0, 64) + "..." : clean;
    }
}
