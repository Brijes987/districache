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
 * DEL key [key ...], DBSIZE, QUIT, AUTH, and REWRITEAOF when persistence is on. When a password is
 * required, every command except AUTH and QUIT is refused with NOAUTH until the connection authenticates. SET also accepts
 * PXAT (absolute expiry in epoch milliseconds). Keys and values are UTF-8 text.
 *
 * <p>With a log, each write is applied and appended while holding the log's lock, so the log
 * order is the order the writes took effect.
 */
final class CommandExecutor {

    enum Result { CONTINUE, CLOSE }

    private final ShardedLRUCache<String, String> cache;
    private final AppendOnlyLog log; // null when persistence is off
    private final Authenticator authenticator; // null when no password is required
    private boolean authenticated;
    private int failedAttempts;

    static final int MAX_FAILED_AUTH_ATTEMPTS = 5;
    static final long FAILED_AUTH_DELAY_MILLIS = 250;

    CommandExecutor(ShardedLRUCache<String, String> cache) {
        this(cache, null, null);
    }

    CommandExecutor(ShardedLRUCache<String, String> cache, AppendOnlyLog log) {
        this(cache, log, null);
    }

    /** One executor serves one connection: it remembers whether that connection has authenticated. */
    CommandExecutor(ShardedLRUCache<String, String> cache, AppendOnlyLog log, Authenticator authenticator) {
        this.cache = cache;
        this.log = log;
        this.authenticator = authenticator;
        this.authenticated = authenticator == null;
    }

    boolean isAuthenticated() {
        return authenticated;
    }

    Result execute(List<byte[]> args, RespWriter out) throws IOException {
        String name = text(args.get(0));
        String command = name.toUpperCase(Locale.ROOT);
        int argc = args.size();
        if (command.equals("AUTH")) {
            return auth(args, out);
        }
        if (!authenticated && !command.equals("QUIT")) {
            out.error("NOAUTH Authentication required.");
            return Result.CONTINUE;
        }
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
                    del(args, out);
                }
            }
            case "REWRITEAOF" -> {
                if (argc != 1) {
                    arity(out, command);
                } else if (log == null) {
                    out.error("ERR persistence is not enabled");
                } else {
                    log.lock();
                    try {
                        log.compact(cache);
                        out.simple("OK");
                    } catch (IOException e) {
                        out.error("ERR rewrite failed: " + sanitize(String.valueOf(e.getMessage())));
                    } finally {
                        log.unlock();
                    }
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

    /** AUTH password, or AUTH default password (the user name is accepted for client compatibility). */
    private Result auth(List<byte[]> args, RespWriter out) throws IOException {
        int argc = args.size();
        if (argc != 2 && argc != 3) {
            arity(out, "AUTH");
            return Result.CONTINUE;
        }
        if (authenticator == null) {
            out.error("ERR AUTH called without any password configured");
            return Result.CONTINUE;
        }
        if (argc == 3 && !text(args.get(1)).equals("default")) {
            return authFailed(out);
        }
        if (authenticator.matches(args.get(argc - 1))) {
            authenticated = true;
            failedAttempts = 0;
            out.simple("OK");
            return Result.CONTINUE;
        }
        return authFailed(out);
    }

    /** A fixed delay slows guessing on one connection; too many failures close it. There is no global rate limit. */
    private Result authFailed(RespWriter out) throws IOException {
        failedAttempts++;
        try {
            Thread.sleep(FAILED_AUTH_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        out.error("WRONGPASS invalid password");
        return failedAttempts >= MAX_FAILED_AUTH_ATTEMPTS ? Result.CLOSE : Result.CONTINUE;
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
            applySet(key, value, -1, out);
            return;
        }
        String option = text(args.get(3)).toUpperCase(Locale.ROOT);
        if (!option.equals("EX") && !option.equals("PX") && !option.equals("PXAT")) {
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
        long now = System.currentTimeMillis();
        long expiresAt;
        if (option.equals("PXAT")) {
            if (amount <= 0) {
                out.error("ERR invalid expire time in 'set' command");
                return;
            }
            expiresAt = amount;
        } else {
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
            expiresAt = ttlMillis > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + ttlMillis;
        }
        applySet(key, value, expiresAt, out);
    }

    /** SET without a TTL. */
    private void applySet(String key, String value, long expiresAt, RespWriter out) throws IOException {
        if (log != null) {
            log.lock();
        }
        try {
            long ttl = expiresAt > 0 ? expiresAt - System.currentTimeMillis() : -1;
            if (expiresAt > 0 && ttl <= 0) {
                cache.delete(key); // expiry already in the past: the key ends up absent
            } else if (expiresAt > 0) {
                cache.put(key, value, ttl);
            } else {
                cache.put(key, value);
            }
            if (log != null) {
                if (expiresAt > 0) {
                    log.append("SET", key, value, "PXAT", Long.toString(expiresAt));
                } else {
                    log.append("SET", key, value);
                }
                log.maybeCompact(cache);
            }
            out.simple("OK");
        } catch (IOException e) {
            out.error("ERR write to the append-only log failed: " + sanitize(String.valueOf(e.getMessage())));
        } finally {
            if (log != null) {
                log.unlock();
            }
        }
    }

    private void del(List<byte[]> args, RespWriter out) throws IOException {
        if (log != null) {
            log.lock();
        }
        try {
            long removed = 0;
            java.util.ArrayList<String> removedKeys = new java.util.ArrayList<>();
            for (int i = 1; i < args.size(); i++) {
                String key = text(args.get(i));
                if (cache.delete(key)) {
                    removed++;
                    removedKeys.add(key);
                }
            }
            if (log != null && !removedKeys.isEmpty()) {
                String[] record = new String[removedKeys.size() + 1];
                record[0] = "DEL";
                for (int i = 0; i < removedKeys.size(); i++) {
                    record[i + 1] = removedKeys.get(i);
                }
                log.append(record);
                log.maybeCompact(cache);
            }
            out.integer(removed);
        } catch (IOException e) {
            out.error("ERR write to the append-only log failed: " + sanitize(String.valueOf(e.getMessage())));
        } finally {
            if (log != null) {
                log.unlock();
            }
        }
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
