package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RespNioParserTest {

    private static List<String> text(List<byte[]> args) {
        List<String> out = new ArrayList<>();
        for (byte[] a : args) {
            out.add(new String(a, StandardCharsets.UTF_8));
        }
        return out;
    }

    private static RespNioParser parserWith(byte[] bytes) throws Exception {
        RespNioParser p = new RespNioParser();
        p.feed(bytes, 0, bytes.length);
        return p;
    }

    @Test
    void parsesOneCommandAndReportsNothingMore() throws Exception {
        RespNioParser p = parserWith(RespTestClient.encode("SET", "k", "v"));
        assertEquals(List.of("SET", "k", "v"), text(p.next()));
        assertNull(p.next());
        assertEquals(0, p.buffered());
    }

    @Test
    void parsesPipelinedCommandsInOrder() throws Exception {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        b.writeBytes(RespTestClient.encode("SET", "a", "1"));
        b.writeBytes(RespTestClient.encode("GET", "a"));
        b.writeBytes("PING\r\n".getBytes());
        RespNioParser p = parserWith(b.toByteArray());
        assertEquals(List.of("SET", "a", "1"), text(p.next()));
        assertEquals(List.of("GET", "a"), text(p.next()));
        assertEquals(List.of("PING"), text(p.next()));
        assertNull(p.next());
    }

    @Test
    void aCommandFedOneByteAtATimeIsOnlyReturnedWhenComplete() throws Exception {
        byte[] all = RespTestClient.encode("SET", "key", "a value with spaces\r\ninside");
        RespNioParser p = new RespNioParser();
        for (int i = 0; i < all.length; i++) {
            assertNull(p.next(), "returned early at byte " + i);
            p.feed(all, i, 1);
        }
        assertEquals(List.of("SET", "key", "a value with spaces\r\ninside"), text(p.next()));
        assertNull(p.next());
    }

    @Test
    void everySplitPointGivesTheSameCommands() throws Exception {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        b.writeBytes(RespTestClient.encode("SET", "a", "1"));
        b.writeBytes("  PING   x \r\n".getBytes());
        b.writeBytes(RespTestClient.encode("GET", "a"));
        byte[] all = b.toByteArray();
        for (int split = 0; split <= all.length; split++) {
            RespNioParser p = new RespNioParser();
            p.feed(all, 0, split);
            List<List<String>> got = new ArrayList<>();
            List<byte[]> c;
            while ((c = p.next()) != null) {
                got.add(text(c));
            }
            p.feed(all, split, all.length - split);
            while ((c = p.next()) != null) {
                got.add(text(c));
            }
            assertEquals(List.of(List.of("SET", "a", "1"), List.of("PING", "x"), List.of("GET", "a")), got,
                    "split at " + split);
        }
    }

    @Test
    void blankLineAndEmptyArrayGiveEmptyCommands() throws Exception {
        RespNioParser p = parserWith("\r\n*0\r\n".getBytes());
        assertTrue(p.next().isEmpty());
        assertTrue(p.next().isEmpty());
        assertNull(p.next());
    }

    @Test
    void emptyAndBinaryishArgumentsSurvive() throws Exception {
        RespNioParser p = parserWith("*2\r\n$0\r\n\r\n$3\r\néa\r\n".getBytes(StandardCharsets.UTF_8));
        List<byte[]> args = p.next();
        assertEquals(0, args.get(0).length);
        assertEquals("éa", new String(args.get(1), StandardCharsets.UTF_8));
    }

    private static void assertProtocolError(String expected, byte[] input) throws Exception {
        RespNioParser p = parserWith(input);
        RespProtocolException e = assertThrows(RespProtocolException.class, p::next);
        assertEquals(expected, e.getMessage());
    }

    @Test
    void malformedInputGivesTheSameMessagesAsTheBlockingReader() throws Exception {
        assertProtocolError("invalid multibulk length", "*abc\r\n".getBytes());
        assertProtocolError("invalid multibulk length", "*-1\r\n".getBytes());
        assertProtocolError("invalid multibulk length", ("*" + (RespReader.MAX_ARGS + 1) + "\r\n").getBytes());
        assertProtocolError("expected '$', got ':'", "*1\r\n:5\r\n".getBytes());
        assertProtocolError("invalid bulk length", "*1\r\n$x\r\n".getBytes());
        assertProtocolError("invalid bulk length", "*1\r\n$-1\r\n".getBytes());
        assertProtocolError("invalid bulk length", ("*1\r\n$" + (RespReader.MAX_BULK_BYTES + 1) + "\r\n").getBytes());
        assertProtocolError("bulk string not terminated by CRLF", "*1\r\n$3\r\nabcXY".getBytes());
    }

    @Test
    void anOversizedBulkIsRefusedFromItsHeaderBeforeAnyDataArrives() throws Exception {
        // only the header has been fed: the limit must already fire, not wait for 2 MiB of data
        assertProtocolError("invalid bulk length", ("*1\r\n$" + (2 << 20) + "\r\n").getBytes());
    }

    @Test
    void totalCommandSizeIsLimitedFromTheHeaders() throws Exception {
        StringBuilder s = new StringBuilder("*" + 9 + "\r\n");
        for (int i = 0; i < 9; i++) {
            s.append("$").append(RespReader.MAX_BULK_BYTES).append("\r\n");
            if (i < 8) {
                // headers only would not be valid for earlier args; feed real data so parsing reaches the 9th header
                s.append("x".repeat(RespReader.MAX_BULK_BYTES)).append("\r\n");
            }
        }
        assertProtocolError("command too large", s.toString().getBytes(StandardCharsets.ISO_8859_1));
    }

    @Test
    void aLineWithoutANewlineThatIsTooLongIsRefused() throws Exception {
        assertProtocolError("line too long", "x".repeat(RespReader.MAX_LINE_BYTES + 1).getBytes());
        assertProtocolError("line too long", ("*" + "1".repeat(RespReader.MAX_LINE_BYTES + 1)).getBytes());
        // exactly at the limit is still just "incomplete"
        assertNull(parserWith("x".repeat(RespReader.MAX_LINE_BYTES).getBytes()).next());
    }

    @Test
    void tooManyInlineTokensAreRefused() throws Exception {
        assertProtocolError("too many arguments", ("a ".repeat(RespReader.MAX_ARGS + 1) + "\r\n").getBytes());
    }

    @Test
    void theBufferIsReleasedAfterALargeCommand() throws Exception {
        RespNioParser p = new RespNioParser();
        byte[] big = RespTestClient.encode("SET", "k", "v".repeat(200_000));
        p.feed(big, 0, big.length);
        assertTrue(p.capacity() > 100_000);
        assertNotNull(p.next());
        assertTrue(p.capacity() <= 1024, "buffer kept at " + p.capacity());
    }

    @Test
    void consumedBytesAreReusedSoAStreamOfSmallCommandsDoesNotGrowTheBuffer() throws Exception {
        RespNioParser p = new RespNioParser();
        byte[] one = RespTestClient.encode("SET", "key", "value");
        for (int i = 0; i < 10_000; i++) {
            p.feed(one, 0, one.length - 3); // leave a tail so the buffer is never completely empty
            p.feed(one, one.length - 3, 3);
            assertNotNull(p.next());
        }
        assertTrue(p.capacity() <= 1024, "buffer grew to " + p.capacity());
    }
}
