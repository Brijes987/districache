package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RespReaderTest {

    private static RespReader reader(String input) {
        return new RespReader(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    }

    private static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void parsesAnArrayOfBulkStrings() throws IOException {
        List<byte[]> cmd = reader("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").readCommand();
        assertEquals(2, cmd.size());
        assertEquals("GET", s(cmd.get(0)));
        assertEquals("k", s(cmd.get(1)));
    }

    @Test
    void bulkStringMayContainCrlfAndSpaces() throws IOException {
        List<byte[]> cmd = reader("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\na\r\nb c\r\n").readCommand();
        assertEquals("a\r\nb c", s(cmd.get(2)));
    }

    @Test
    void parsesInlineCommands() throws IOException {
        List<byte[]> cmd = reader("SET  key   value\r\n").readCommand();
        assertEquals(3, cmd.size());
        assertEquals("key", s(cmd.get(1)));
        assertEquals("value", s(cmd.get(2)));
    }

    @Test
    void inlineCommandMayEndWithBareLf() throws IOException {
        List<byte[]> cmd = reader("PING\n").readCommand();
        assertEquals(1, cmd.size());
        assertEquals("PING", s(cmd.get(0)));
    }

    @Test
    void blankLineAndEmptyArrayGiveEmptyCommands() throws IOException {
        assertTrue(reader("\r\n").readCommand().isEmpty());
        assertTrue(reader("*0\r\n").readCommand().isEmpty());
    }

    @Test
    void readsPipelinedCommandsOneAfterAnother() throws IOException {
        RespReader r = reader("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assertEquals("PING", s(r.readCommand().get(0)));
        assertEquals("ECHO", s(r.readCommand().get(0)));
        assertTrue(r.readCommand() == null, "clean EOF between commands gives null");
    }

    @Test
    void emptyInputIsCleanEof() throws IOException {
        assertTrue(reader("").readCommand() == null);
    }

    @Test
    void emptyBulkStringIsAllowed() throws IOException {
        List<byte[]> cmd = reader("*2\r\n$3\r\nGET\r\n$0\r\n\r\n").readCommand();
        assertEquals(0, cmd.get(1).length);
    }

    @Test
    void rejectsBadLengthsAndMarkers() {
        assertThrows(RespProtocolException.class, () -> reader("*abc\r\n").readCommand());
        assertThrows(RespProtocolException.class, () -> reader("*-1\r\n").readCommand());
        assertThrows(RespProtocolException.class, () -> reader("*1\r\nXYZ\r\n").readCommand());
        assertThrows(RespProtocolException.class, () -> reader("*1\r\n$-5\r\n").readCommand());
        assertThrows(RespProtocolException.class, () -> reader("*1\r\n$1\r\nabX\r\n").readCommand());
    }

    @Test
    void rejectsOversizedBulkAndTooManyArguments() {
        assertThrows(RespProtocolException.class,
                () -> reader("*1\r\n$" + (RespReader.MAX_BULK_BYTES + 1) + "\r\n").readCommand());
        assertThrows(RespProtocolException.class,
                () -> reader("*" + (RespReader.MAX_ARGS + 1) + "\r\n").readCommand());
    }

    @Test
    void rejectsACommandWhoseArgumentsAreTooLargeInTotal() {
        // nine arguments of exactly the per-argument maximum: each is allowed, together they are not
        int n = RespReader.MAX_COMMAND_BYTES / RespReader.MAX_BULK_BYTES + 1;
        StringBuilder sb = new StringBuilder("*" + n + "\r\n");
        String payload = "x".repeat(RespReader.MAX_BULK_BYTES);
        for (int i = 0; i < n; i++) {
            sb.append("$").append(RespReader.MAX_BULK_BYTES).append("\r\n").append(payload).append("\r\n");
        }
        RespProtocolException e = assertThrows(RespProtocolException.class, () -> reader(sb.toString()).readCommand());
        assertTrue(e.getMessage().contains("too large"));
    }

    @Test
    void acceptsACommandJustUnderTheTotalLimit() throws IOException {
        int n = RespReader.MAX_COMMAND_BYTES / RespReader.MAX_BULK_BYTES;
        StringBuilder sb = new StringBuilder("*" + n + "\r\n");
        String payload = "x".repeat(RespReader.MAX_BULK_BYTES);
        for (int i = 0; i < n; i++) {
            sb.append("$").append(RespReader.MAX_BULK_BYTES).append("\r\n").append(payload).append("\r\n");
        }
        assertEquals(n, reader(sb.toString()).readCommand().size());
    }

    @Test
    void rejectsOverlongLine() {
        String line = "A".repeat(RespReader.MAX_LINE_BYTES + 10) + "\r\n";
        assertThrows(RespProtocolException.class, () -> reader(line).readCommand());
    }

    @Test
    void truncatedCommandIsAnEofError() {
        assertThrows(EOFException.class, () -> reader("*2\r\n$3\r\nGET\r\n").readCommand());
        assertThrows(EOFException.class, () -> reader("*1\r\n$10\r\nabc").readCommand());
    }
}
