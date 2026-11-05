package com.distri.cache.server;

import static org.junit.jupiter.api.Assertions.*;

import com.distri.cache.ShardedLRUCache;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandExecutorTest {

    private final ShardedLRUCache<String, String> cache = new ShardedLRUCache<>(100, 4, 0);
    private final CommandExecutor executor = new CommandExecutor(cache);

    /** Runs one command and returns the exact bytes written as text. */
    private String run(String... args) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(bytes);
        List<byte[]> list = new ArrayList<>();
        for (String a : args) list.add(a.getBytes(StandardCharsets.UTF_8));
        executor.execute(list, writer);
        writer.flush();
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void ping() throws IOException {
        assertEquals("+PONG\r\n", run("PING"));
        assertEquals("$2\r\nhi\r\n", run("PING", "hi"));
        assertEquals("-ERR wrong number of arguments for 'ping' command\r\n", run("PING", "a", "b"));
    }

    @Test
    void echo() throws IOException {
        assertEquals("$5\r\nhello\r\n", run("ECHO", "hello"));
        assertEquals("-ERR wrong number of arguments for 'echo' command\r\n", run("ECHO"));
    }

    @Test
    void setThenGet() throws IOException {
        assertEquals("+OK\r\n", run("SET", "name", "Asha"));
        assertEquals("$4\r\nAsha\r\n", run("GET", "name"));
    }

    @Test
    void getOfMissingKeyIsNullBulk() throws IOException {
        assertEquals("$-1\r\n", run("GET", "missing"));
    }

    @Test
    void commandNamesAreCaseInsensitive() throws IOException {
        assertEquals("+OK\r\n", run("set", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("GeT", "k"));
    }

    @Test
    void valuesMayContainUnicodeAndSpaces() throws IOException {
        run("SET", "k", "नमस्ते world");
        assertEquals("नमस्ते world", cache.get("k").orElseThrow());
        assertEquals("$" + "नमस्ते world".getBytes(StandardCharsets.UTF_8).length + "\r\nनमस्ते world\r\n", run("GET", "k"));
    }

    @Test
    void setWithExpiry() throws Exception {
        assertEquals("+OK\r\n", run("SET", "k", "v", "PX", "40"));
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
        Thread.sleep(120);
        assertEquals("$-1\r\n", run("GET", "k"));
        assertEquals("+OK\r\n", run("SET", "e", "v", "ex", "100"));
        assertEquals("$1\r\nv\r\n", run("GET", "e"));
    }

    @Test
    void setWithoutExpiryClearsAnEarlierExpiry() throws Exception {
        run("SET", "k", "v1", "PX", "40");
        run("SET", "k", "v2");
        Thread.sleep(120);
        assertEquals("$2\r\nv2\r\n", run("GET", "k"));
    }

    @Test
    void setRejectsBadExpiryAndBadSyntax() throws IOException {
        assertEquals("-ERR invalid expire time in 'set' command\r\n", run("SET", "k", "v", "EX", "0"));
        assertEquals("-ERR invalid expire time in 'set' command\r\n", run("SET", "k", "v", "PX", "-5"));
        assertEquals("-ERR invalid expire time in 'set' command\r\n",
                run("SET", "k", "v", "EX", String.valueOf(Long.MAX_VALUE)));
        assertEquals("-ERR value is not an integer or out of range\r\n", run("SET", "k", "v", "EX", "abc"));
        assertEquals("-ERR syntax error\r\n", run("SET", "k", "v", "NX", "10"));
        assertEquals("-ERR syntax error\r\n", run("SET", "k", "v", "EX"));
        assertEquals("-ERR wrong number of arguments for 'set' command\r\n", run("SET", "k"));
        assertEquals("$-1\r\n", run("GET", "k"));
    }

    @Test
    void delCountsOnlyExistingKeys() throws IOException {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("DEL", "a", "b", "nope"));
        assertEquals(":0\r\n", run("DEL", "a"));
        assertEquals("-ERR wrong number of arguments for 'del' command\r\n", run("DEL"));
    }

    @Test
    void delDoesNotCountAnExpiredKey() throws Exception {
        run("SET", "k", "v", "PX", "30");
        Thread.sleep(100);
        assertEquals(":0\r\n", run("DEL", "k"));
    }

    @Test
    void dbsizeCountsValidKeys() throws Exception {
        assertEquals(":0\r\n", run("DBSIZE"));
        run("SET", "a", "1");
        run("SET", "b", "2", "PX", "30");
        assertEquals(":2\r\n", run("DBSIZE"));
        Thread.sleep(100);
        assertEquals(":1\r\n", run("DBSIZE"));
    }

    @Test
    void quitRepliesOkAndAsksToClose() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(bytes);
        CommandExecutor.Result r = executor.execute(List.of("QUIT".getBytes(StandardCharsets.UTF_8)), writer);
        writer.flush();
        assertEquals(CommandExecutor.Result.CLOSE, r);
        assertEquals("+OK\r\n", bytes.toString(StandardCharsets.UTF_8));
    }

    @Test
    void unknownCommandNamesAreEchoedSafely() throws IOException {
        assertEquals("-ERR unknown command 'BOGUS'\r\n", run("BOGUS"));
        String longName = "x".repeat(500);
        String reply = run(longName);
        assertTrue(reply.length() < 120, "an attacker-controlled name must not be echoed unbounded");
        assertTrue(reply.endsWith("\r\n") && reply.indexOf('\n') == reply.length() - 1,
                "the error must stay on one line");
    }
}
