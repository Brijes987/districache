package com.distri.cache.server;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads client commands in the Redis wire format (RESP): an array of bulk strings
 * ({@code *2\r\n$3\r\nGET\r\n$1\r\nk\r\n}). A plain text line such as {@code PING\r\n} (an "inline
 * command", handy with telnet) is accepted too, split on whitespace with no quoting.
 *
 * <p>Sizes are bounded so a hostile client cannot make the server allocate unbounded memory.
 */
final class RespReader {

    static final int MAX_ARGS = 1024;
    static final int MAX_BULK_BYTES = 1 << 20; // 1 MiB per argument
    static final int MAX_COMMAND_BYTES = 8 << 20; // 8 MiB for all arguments of one command together
    static final int MAX_LINE_BYTES = 64 * 1024;

    private final CountingStream in;

    RespReader(InputStream in) {
        this.in = new CountingStream(new BufferedInputStream(in, 8192));
    }

    /** Number of bytes consumed so far (not counting bytes only buffered). Used to find the end of the last whole log record. */
    long position() {
        return in.count;
    }

    /** Counts the bytes handed to the parser, as opposed to bytes the buffer has read ahead. */
    private static final class CountingStream extends java.io.FilterInputStream {
        long count;

        CountingStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }
    }

    /**
     * Blocks until a full command is read.
     *
     * @return the arguments (the first is the command name); an empty list for a blank line or
     *     {@code *0}; {@code null} if the client closed the connection between commands
     * @throws RespProtocolException on malformed input
     * @throws EOFException if the connection closes in the middle of a command
     */
    List<byte[]> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first == '*') {
            return readArray();
        }
        return readInline(first);
    }

    /** True if more bytes are already buffered, i.e. the client pipelined another command. */
    boolean hasBufferedInput() throws IOException {
        return in.available() > 0;
    }

    private List<byte[]> readArray() throws IOException {
        long count = parseLong(readLine(-1), "invalid multibulk length");
        if (count < 0 || count > MAX_ARGS) {
            throw new RespProtocolException("invalid multibulk length");
        }
        List<byte[]> args = new ArrayList<>((int) Math.min(count, 16));
        long totalBytes = 0;
        for (int i = 0; i < count; i++) {
            int marker = in.read();
            if (marker == -1) {
                throw new EOFException("connection closed inside a command");
            }
            if (marker != '$') {
                throw new RespProtocolException("expected '$', got '" + (char) marker + "'");
            }
            long len = parseLong(readLine(-1), "invalid bulk length");
            if (len < 0 || len > MAX_BULK_BYTES) {
                throw new RespProtocolException("invalid bulk length");
            }
            totalBytes += len;
            if (totalBytes > MAX_COMMAND_BYTES) {
                // checked before reading the data, so a hostile client cannot make us buffer it
                throw new RespProtocolException("command too large");
            }
            byte[] data = in.readNBytes((int) len);
            if (data.length < len) {
                throw new EOFException("connection closed inside a command");
            }
            int cr = in.read();
            int lf = cr == -1 ? -1 : in.read();
            if (cr == -1 || lf == -1) {
                throw new EOFException("connection closed inside a command");
            }
            if (cr != '\r' || lf != '\n') {
                throw new RespProtocolException("bulk string not terminated by CRLF");
            }
            args.add(data);
        }
        return args;
    }

    private List<byte[]> readInline(int firstByte) throws IOException {
        String line = new String(readLine(firstByte), StandardCharsets.UTF_8).trim();
        List<byte[]> args = new ArrayList<>();
        if (line.isEmpty()) {
            return args;
        }
        for (String token : line.split("\\s+")) {
            args.add(token.getBytes(StandardCharsets.UTF_8));
        }
        if (args.size() > MAX_ARGS) {
            throw new RespProtocolException("too many arguments");
        }
        return args;
    }

    /** Reads up to the next LF, dropping a preceding CR. {@code firstByte >= 0} is a byte already consumed. */
    private byte[] readLine(int firstByte) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(32);
        int b = firstByte >= 0 ? firstByte : in.read();
        while (true) {
            if (b == -1) {
                throw new EOFException("connection closed inside a line");
            }
            if (b == '\n') {
                break;
            }
            buf.write(b);
            if (buf.size() > MAX_LINE_BYTES) {
                throw new RespProtocolException("line too long");
            }
            b = in.read();
        }
        byte[] bytes = buf.toByteArray();
        if (bytes.length > 0 && bytes[bytes.length - 1] == '\r') {
            return java.util.Arrays.copyOf(bytes, bytes.length - 1);
        }
        return bytes;
    }

    private static long parseLong(byte[] digits, String errorMessage) throws RespProtocolException {
        try {
            return Long.parseLong(new String(digits, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new RespProtocolException(errorMessage);
        }
    }
}
