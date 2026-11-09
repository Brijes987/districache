package com.distri.cache.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * One client operation as it is stored in the Raft log.
 *
 * <p>Every command carries the client's id and a per-client sequence number, so a retried request can be recognised
 * and not applied twice. A SET carries an <em>absolute</em> expiry time chosen by the leader when it accepted the
 * request, never a relative TTL, so replaying the log later does not stretch the TTL.
 *
 * @param expiresAtMillis absolute epoch milliseconds for SET, or -1 for no expiry; unused for GET and DEL
 */
record CacheCommand(Op op, long clientId, long seq, String key, String value, long expiresAtMillis) {

    enum Op {
        SET((byte) 1), DEL((byte) 2), GET((byte) 3);

        final byte code;

        Op(byte code) {
            this.code = code;
        }

        static Op of(byte code) {
            for (Op op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            throw new IllegalArgumentException("unknown command code " + code);
        }
    }

    static CacheCommand set(long clientId, long seq, String key, String value, long expiresAtMillis) {
        return new CacheCommand(Op.SET, clientId, seq, key, value, expiresAtMillis);
    }

    static CacheCommand delete(long clientId, long seq, String key) {
        return new CacheCommand(Op.DEL, clientId, seq, key, "", -1);
    }

    static CacheCommand get(long clientId, long seq, String key) {
        return new CacheCommand(Op.GET, clientId, seq, key, "", -1);
    }

    byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(op.code);
            out.writeLong(clientId);
            out.writeLong(seq);
            writeString(out, key);
            writeString(out, value);
            out.writeLong(expiresAtMillis);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e); // cannot happen: writing to memory
        }
    }

    static CacheCommand decode(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            Op op = Op.of(in.readByte());
            long clientId = in.readLong();
            long seq = in.readLong();
            String key = readString(in);
            String value = readString(in);
            long expiresAt = in.readLong();
            if (in.available() != 0) {
                throw new IllegalArgumentException("trailing bytes in a cache command");
            }
            return new CacheCommand(op, clientId, seq, key, value, expiresAt);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed cache command", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new IOException("bad string length " + length);
        }
        byte[] b = new byte[length];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
