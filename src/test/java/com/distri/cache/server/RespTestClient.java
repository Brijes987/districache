package com.distri.cache.server;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** A minimal raw-socket RESP client, used only by the tests. */
final class RespTestClient implements Closeable {

    /** type is one of + - : $ ; text is null for a null bulk string. */
    record Reply(char type, String text) {}

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    RespTestClient(int port) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port), 3000);
        socket.setSoTimeout(5000); // a hung server fails the test instead of hanging the build
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
    }

    static byte[] encode(String... args) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String a : args) {
            byte[] bytes = a.getBytes(StandardCharsets.UTF_8);
            b.writeBytes(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            b.writeBytes(bytes);
            b.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        return b.toByteArray();
    }

    void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    void send(String... args) throws IOException {
        sendRaw(encode(args));
    }

    Reply call(String... args) throws IOException {
        send(args);
        return read();
    }

    Reply read() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("connection closed");
        }
        String line = readLine();
        if (type == '$') {
            int len = Integer.parseInt(line);
            if (len < 0) {
                return new Reply('$', null);
            }
            byte[] data = in.readNBytes(len);
            in.read(); // CR
            in.read(); // LF
            return new Reply('$', new String(data, StandardCharsets.UTF_8));
        }
        return new Reply((char) type, line);
    }

    /** True if the server has closed the connection. */
    boolean isClosedByServer() throws IOException {
        return in.read() == -1;
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != '\r') {
            if (c == -1) throw new IOException("connection closed inside a line");
            sb.append((char) c);
        }
        in.read(); // LF
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
