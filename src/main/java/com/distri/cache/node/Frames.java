package com.distri.cache.node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Length-prefixed frames: a 4-byte length, then that many bytes. The length is checked before anything is allocated. */
final class Frames {

    /** Largest frame accepted. Raft batches are at most 64 entries of at most a few MB each in practice. */
    static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private Frames() {}

    static void write(DataOutputStream out, byte[] payload) throws IOException {
        if (payload.length > MAX_FRAME_BYTES) {
            throw new IOException("frame of " + payload.length + " bytes is too large");
        }
        out.writeInt(payload.length);
        out.write(payload);
    }

    static byte[] read(DataInputStream in) throws IOException {
        return read(in, MAX_FRAME_BYTES);
    }

    /** For frames read before the peer has proved who it is: a small limit, so strangers cannot make us allocate. */
    static byte[] read(DataInputStream in, int maxBytes) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxBytes) {
            throw new IOException("bad frame length " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return payload;
    }
}
