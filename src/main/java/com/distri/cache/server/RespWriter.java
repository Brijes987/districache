package com.distri.cache.server;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** Writes replies in RESP. Output is buffered; call {@link #flush()} to send it. */
final class RespWriter {

    private static final byte[] CRLF = {'\r', '\n'};

    private final BufferedOutputStream out;

    RespWriter(OutputStream out) {
        this.out = new BufferedOutputStream(out, 8192);
    }

    /** {@code +text}; text must not contain CR or LF. */
    void simple(String text) throws IOException {
        line('+', text);
    }

    /** {@code -text}; text must not contain CR or LF. */
    void error(String text) throws IOException {
        line('-', text);
    }

    void integer(long value) throws IOException {
        line(':', Long.toString(value));
    }

    void bulk(byte[] data) throws IOException {
        line('$', Integer.toString(data.length));
        out.write(data);
        out.write(CRLF);
    }

    /** Writes bytes that are already RESP-encoded (a replicated record). */
    void raw(byte[] encoded) throws IOException {
        out.write(encoded);
    }

    void nullBulk() throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    void flush() throws IOException {
        out.flush();
    }

    private void line(char type, String text) throws IOException {
        out.write(type);
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }
}
