package com.distri.cache.node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The framed binary protocol between {@link ReplicatedClient} and {@link ReplicatedServer}. It is not RESP: a request
 * must carry the client id and sequence number that make retries safe, which Redis commands have no place for.
 */
final class ClientProtocol {

    static final byte SET = 1;
    static final byte DEL = 2;
    static final byte GET = 3;

    static final byte OK = 0;
    static final byte NOT_LEADER = 1;
    static final byte UNKNOWN = 2;
    static final byte STALE = 3;
    static final byte ERROR = 4;

    static final int MAX_KEY_BYTES = 64 * 1024;
    static final int MAX_VALUE_BYTES = 4 * 1024 * 1024;

    private ClientProtocol() {}

    record Request(byte op, long clientId, long seq, String key, String value, long ttlMillis) {}

    /**
     * @param found for GET and DEL: whether the key existed
     * @param value for GET: the value if found, otherwise empty
     * @param hint for NOT_LEADER: "host:port" of the believed leader's client port, or empty
     * @param message for ERROR: what was wrong
     */
    record Response(byte status, boolean found, String value, String hint, String message) {}

    static byte[] encode(Request r) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(r.op());
        out.writeLong(r.clientId());
        out.writeLong(r.seq());
        writeString(out, r.key());
        writeString(out, r.value());
        out.writeLong(r.ttlMillis());
        return bytes.toByteArray();
    }

    static Request decodeRequest(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        byte op = in.readByte();
        if (op != SET && op != DEL && op != GET) {
            throw new IOException("unknown operation " + op);
        }
        long clientId = in.readLong();
        long seq = in.readLong();
        String key = readString(in, MAX_KEY_BYTES);
        String value = readString(in, MAX_VALUE_BYTES);
        long ttl = in.readLong();
        if (in.available() != 0) {
            throw new IOException("trailing bytes in a request");
        }
        if (seq < 1) {
            throw new IOException("sequence numbers start at 1");
        }
        return new Request(op, clientId, seq, key, value, ttl);
    }

    static byte[] encode(Response r) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(r.status());
        out.writeBoolean(r.found());
        writeString(out, r.value());
        writeString(out, r.hint());
        writeString(out, r.message());
        return bytes.toByteArray();
    }

    static Response decodeResponse(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        byte status = in.readByte();
        boolean found = in.readBoolean();
        String value = readString(in, MAX_VALUE_BYTES);
        String hint = readString(in, 1024);
        String message = readString(in, 4096);
        if (in.available() != 0) {
            throw new IOException("trailing bytes in a response");
        }
        return new Response(status, found, value, hint, message);
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in, int max) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > max || length > in.available()) {
            throw new IOException("bad string length " + length);
        }
        byte[] b = new byte[length];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
