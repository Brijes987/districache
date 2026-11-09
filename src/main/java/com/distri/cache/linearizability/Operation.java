package com.distri.cache.linearizability;

/**
 * One client operation as the client saw it: when it started, when (if ever) it got an answer, and what the answer was.
 * Times are any increasing numbers (simulated ticks, nanoseconds); only their order matters. An operation that started
 * before another one's answer arrived overlaps it; one whose answer arrived before the other started precedes it.
 *
 * <p>An operation with no answer (the client gave up or the run ended) is <em>pending</em>: it may have taken effect at
 * any moment after it started, or never. A pending GET changes nothing, so only pending SETs and DELs matter.
 */
public final class Operation {

    public enum Kind { SET, GET, DEL }

    /** Marks an operation that never returned. */
    public static final long PENDING = Long.MAX_VALUE;

    private final int client;
    private final Kind kind;
    private final String key;
    private final String argument;
    private final long invoke;
    private final long returned;
    private final boolean found;
    private final String value;

    private Operation(int client, Kind kind, String key, String argument, long invoke, long returned, boolean found,
            String value) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (returned < invoke) {
            throw new IllegalArgumentException("an operation cannot return before it starts");
        }
        this.client = client;
        this.kind = kind;
        this.key = key;
        this.argument = argument;
        this.invoke = invoke;
        this.returned = returned;
        this.found = found;
        this.value = value;
    }

    public static Operation set(int client, String key, String value, long invoke, long returned) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        return new Operation(client, Kind.SET, key, value, invoke, returned, false, null);
    }

    /** A GET that returned: {@code found} says whether the key existed, {@code value} is its value if so. */
    public static Operation get(int client, String key, boolean found, String value, long invoke, long returned) {
        if (found && value == null) {
            throw new IllegalArgumentException("a found value must not be null");
        }
        return new Operation(client, Kind.GET, key, null, invoke, returned, found, found ? value : null);
    }

    /** A DEL that returned: {@code found} says whether the key existed (and was therefore removed). */
    public static Operation delete(int client, String key, boolean found, long invoke, long returned) {
        return new Operation(client, Kind.DEL, key, null, invoke, returned, found, null);
    }

    public static Operation pendingSet(int client, String key, String value, long invoke) {
        return set(client, key, value, invoke, PENDING);
    }

    public static Operation pendingDelete(int client, String key, long invoke) {
        return new Operation(client, Kind.DEL, key, null, invoke, PENDING, false, null);
    }

    public int client() {
        return client;
    }

    public Kind kind() {
        return kind;
    }

    public String key() {
        return key;
    }

    /** The value written by a SET; null for other kinds. */
    public String argument() {
        return argument;
    }

    public long invoke() {
        return invoke;
    }

    public long returned() {
        return returned;
    }

    public boolean isPending() {
        return returned == PENDING;
    }

    public boolean found() {
        return found;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        String call = switch (kind) {
            case SET -> "SET " + key + "=" + argument;
            case GET -> "GET " + key;
            case DEL -> "DEL " + key;
        };
        String result = isPending() ? "no answer" : switch (kind) {
            case SET -> "OK";
            case GET -> found ? "\"" + value + "\"" : "(absent)";
            case DEL -> found ? "removed" : "absent";
        };
        return "client " + client + ": " + call + " -> " + result + "  [" + invoke + ", "
                + (isPending() ? "never" : String.valueOf(returned)) + "]";
    }
}
