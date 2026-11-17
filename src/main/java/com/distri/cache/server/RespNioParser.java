package com.distri.cache.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Incremental version of {@link RespReader} for the event-loop server: bytes are fed in as they arrive and
 * {@link #next()} returns a command only when a whole one is buffered.
 *
 * <p>It accepts exactly what {@code RespReader} accepts, applies the same limits and gives the same error messages
 * (the differential test sends the same bytes to both servers and compares the replies). A command that is not
 * complete yet is scanned without copying anything, so a large value arriving in many pieces costs one cheap header
 * walk per piece, and every limit is checked as soon as the header that violates it is visible, before the data
 * arrives.
 *
 * <p>Not thread-safe: one parser belongs to one connection, used from its event-loop thread.
 */
final class RespNioParser {

    private static final int INITIAL_CAPACITY = 256;
    /** A buffer that grew beyond this is dropped (not kept) once it has been emptied. */
    private static final int SHRINK_ABOVE = 16 * 1024;

    private byte[] buf = new byte[INITIAL_CAPACITY];
    private int pos; // first unread byte
    private int lim; // end of buffered bytes

    // scratch for the two-pass parse of one array; sized by the number of arguments
    private int[] argStart = new int[16];
    private int[] argLen = new int[16];

    /** Appends everything remaining in {@code src}. */
    void feed(ByteBuffer src) throws RespProtocolException {
        int n = src.remaining();
        ensureRoom(n);
        src.get(buf, lim, n);
        lim += n;
    }

    /** Appends bytes (used by tests). */
    void feed(byte[] bytes, int off, int len) throws RespProtocolException {
        ensureRoom(len);
        System.arraycopy(bytes, off, buf, lim, len);
        lim += len;
    }

    /** Bytes buffered and not yet consumed. */
    int buffered() {
        return lim - pos;
    }

    /** Buffer capacity currently held (for memory accounting in tests and the benchmark). */
    int capacity() {
        return buf.length;
    }

    private void ensureRoom(int extra) throws RespProtocolException {
        if (pos == lim) {
            pos = 0;
            lim = 0;
            if (buf.length > SHRINK_ABOVE) {
                buf = new byte[INITIAL_CAPACITY];
            }
        }
        if (lim + extra <= buf.length) {
            return;
        }
        int live = lim - pos;
        if (live + extra <= buf.length) { // fits once the consumed prefix is dropped
            System.arraycopy(buf, pos, buf, 0, live);
            pos = 0;
            lim = live;
            return;
        }
        long wanted = Math.max((long) buf.length * 2, (long) live + extra);
        if (wanted > Integer.MAX_VALUE - 16) {
            throw new RespProtocolException("command too large");
        }
        byte[] bigger = new byte[(int) wanted];
        System.arraycopy(buf, pos, bigger, 0, live);
        buf = bigger;
        pos = 0;
        lim = live;
    }

    /**
     * @return the arguments of the next whole command (an empty list for a blank line or {@code *0}), or
     *     {@code null} if no whole command is buffered yet
     * @throws RespProtocolException on malformed input; the connection should be answered and closed
     */
    List<byte[]> next() throws RespProtocolException {
        if (pos == lim) {
            return null;
        }
        if (buf[pos] == '*') {
            return nextArray();
        }
        return nextInline();
    }

    private List<byte[]> nextArray() throws RespProtocolException {
        int lineEnd = indexOfLf(pos + 1);
        if (lineEnd < 0) {
            requireShortLine(pos + 1);
            return null;
        }
        long count = parseLong(pos + 1, lineEnd, "invalid multibulk length");
        if (count < 0 || count > RespReader.MAX_ARGS) {
            throw new RespProtocolException("invalid multibulk length");
        }
        int n = (int) count;
        if (argStart.length < n) {
            argStart = new int[n];
            argLen = new int[n];
        }
        int p = lineEnd + 1;
        long total = 0;
        for (int i = 0; i < n; i++) {
            if (p >= lim) {
                return null;
            }
            if (buf[p] != '$') {
                throw new RespProtocolException("expected '$', got '" + (char) (buf[p] & 0xff) + "'");
            }
            int le = indexOfLf(p + 1);
            if (le < 0) {
                requireShortLine(p + 1);
                return null;
            }
            long len = parseLong(p + 1, le, "invalid bulk length");
            if (len < 0 || len > RespReader.MAX_BULK_BYTES) {
                throw new RespProtocolException("invalid bulk length");
            }
            total += len;
            if (total > RespReader.MAX_COMMAND_BYTES) {
                throw new RespProtocolException("command too large");
            }
            int dataStart = le + 1;
            long dataEnd = (long) dataStart + len;
            if (dataEnd + 2 > lim) {
                // Not all of it is here. A wrong terminator is still reported as soon as it is visible.
                if (dataEnd < lim && buf[(int) dataEnd] != '\r') {
                    throw new RespProtocolException("bulk string not terminated by CRLF");
                }
                return null;
            }
            if (buf[(int) dataEnd] != '\r' || buf[(int) dataEnd + 1] != '\n') {
                throw new RespProtocolException("bulk string not terminated by CRLF");
            }
            argStart[i] = dataStart;
            argLen[i] = (int) len;
            p = (int) dataEnd + 2;
        }
        List<byte[]> args = new ArrayList<>(Math.min(n, 16));
        for (int i = 0; i < n; i++) {
            args.add(Arrays.copyOfRange(buf, argStart[i], argStart[i] + argLen[i]));
        }
        consume(p);
        return args;
    }

    private List<byte[]> nextInline() throws RespProtocolException {
        int lf = indexOfLf(pos);
        if (lf < 0) {
            requireShortLine(pos);
            return null;
        }
        if (lf - pos > RespReader.MAX_LINE_BYTES) { // the blocking reader counts a trailing CR too
            throw new RespProtocolException("line too long");
        }
        int end = lf;
        if (end > pos && buf[end - 1] == '\r') {
            end--;
        }
        String line = new String(buf, pos, end - pos, StandardCharsets.UTF_8).trim();
        consume(lf + 1);
        List<byte[]> args = new ArrayList<>();
        if (line.isEmpty()) {
            return args;
        }
        for (String token : line.split("\\s+")) {
            args.add(token.getBytes(StandardCharsets.UTF_8));
        }
        if (args.size() > RespReader.MAX_ARGS) {
            throw new RespProtocolException("too many arguments");
        }
        return args;
    }

    private void consume(int newPos) {
        pos = newPos;
        if (pos == lim) {
            pos = 0;
            lim = 0;
            if (buf.length > SHRINK_ABOVE) {
                buf = new byte[INITIAL_CAPACITY];
            }
        }
    }

    /** A line with no LF yet may not already be longer than the line limit. */
    private void requireShortLine(int lineStart) throws RespProtocolException {
        if (lim - lineStart > RespReader.MAX_LINE_BYTES) {
            throw new RespProtocolException("line too long");
        }
    }

    private int indexOfLf(int from) {
        for (int i = from; i < lim; i++) {
            if (buf[i] == '\n') {
                return i;
            }
        }
        return -1;
    }

    /** Parses the digits in [from, lfIndex), dropping one trailing CR; same acceptance as Long.parseLong. */
    private long parseLong(int from, int lfIndex, String errorMessage) throws RespProtocolException {
        if (lfIndex - from > RespReader.MAX_LINE_BYTES) {
            throw new RespProtocolException("line too long");
        }
        int end = lfIndex;
        if (end > from && buf[end - 1] == '\r') {
            end--;
        }
        try {
            return Long.parseLong(new String(buf, from, end - from, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new RespProtocolException(errorMessage);
        }
    }
}
