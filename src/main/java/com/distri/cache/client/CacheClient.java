package com.distri.cache.client;

import com.distri.cache.server.CacheServer;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * A blocking client for one cache server (Redis wire protocol, text keys and values in UTF-8).
 *
 * <p>The connection is opened on first use. If a request fails because of an I/O problem or a timeout,
 * the connection is dropped (its state is unknown) and the next request opens a fresh one. A reply that
 * is an error message from the server does not drop the connection.
 *
 * <p>All methods are synchronized, so one instance can be shared between threads, but requests then run
 * one at a time. For parallelism use one client per thread.
 */
public final class CacheClient implements AutoCloseable {

    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 3_000;
    public static final int DEFAULT_READ_TIMEOUT_MILLIS = 5_000;

    private static final int MAX_BULK_BYTES = 16 << 20; // refuse absurd lengths from a misbehaving server

    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final ClientSecurity security;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private boolean closed;

    public CacheClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MILLIS, DEFAULT_READ_TIMEOUT_MILLIS);
    }

    public CacheClient(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        this(host, port, connectTimeoutMillis, readTimeoutMillis, ClientSecurity.NONE);
    }

    /**
     * With a password and/or TLS. The password is sent with AUTH every time a connection is (re)opened. With
     * TLS the server's certificate is checked against the trust store and against {@code host}.
     */
    public CacheClient(String host, int port, int connectTimeoutMillis, int readTimeoutMillis,
            ClientSecurity security) {
        if (security == null) {
            throw new IllegalArgumentException("security cannot be null (use ClientSecurity.NONE)");
        }
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host cannot be empty");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (connectTimeoutMillis < 0 || readTimeoutMillis < 0) {
            throw new IllegalArgumentException("timeouts cannot be negative (0 means no timeout)");
        }
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.security = security;
    }

    /** Returns the value for {@code key}, or empty if it is missing or expired. */
    public synchronized Optional<String> get(String key) {
        requireText(key, "key");
        Reply reply = call("GET", key);
        return Optional.ofNullable((String) reply.value);
    }

    /** Stores a value without expiry. */
    public synchronized void set(String key, String value) {
        requireText(key, "key");
        requireText(value, "value");
        call("SET", key, value);
    }

    /** Stores a value that expires after {@code ttlMillis} milliseconds. */
    public synchronized void set(String key, String value, long ttlMillis) {
        requireText(key, "key");
        requireText(value, "value");
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be positive");
        }
        call("SET", key, value, "PX", Long.toString(ttlMillis));
    }

    /** Removes the key; returns true if a live entry existed. */
    public synchronized boolean delete(String key) {
        requireText(key, "key");
        return (Long) call("DEL", key).value > 0;
    }

    /** Number of live entries on the server. */
    public synchronized long dbSize() {
        return (Long) call("DBSIZE").value;
    }

    /** Returns "PONG" if the server is alive. */
    public synchronized String ping() {
        return (String) call("PING").value;
    }

    /** Closes the connection. The client cannot be used afterwards. */
    @Override
    public synchronized void close() {
        closed = true;
        disconnect();
    }

    @Override
    public String toString() {
        return "CacheClient[" + host + ":" + port + "]";
    }

    // ------------------------------------------------------------------ protocol

    private record Reply(char type, Object value) {}

    private Reply call(String... args) {
        if (closed) {
            throw new IllegalStateException("client is closed");
        }
        try {
            connectIfNeeded();
            out.write(encode(args));
            out.flush();
            return readReply();
        } catch (ServerError e) {
            throw new CacheClientException("server error from " + host + ":" + port + ": " + e.getMessage());
        } catch (IOException e) {
            disconnect(); // unknown state: do not reuse this connection
            throw new CacheClientException("request to " + host + ":" + port + " failed: " + e, e);
        }
    }

    /** An error reply: the connection itself is still fine. */
    private static final class ServerError extends IOException {
        private static final long serialVersionUID = 1L;

        ServerError(String message) {
            super(message);
        }
    }

    private void connectIfNeeded() throws IOException {
        if (socket != null) {
            return;
        }
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            s.setSoTimeout(readTimeoutMillis);
            s.setTcpNoDelay(true);
            if (security.tlsContext() != null) {
                SSLSocket tls = (SSLSocket) security.tlsContext().getSocketFactory().createSocket(s, host, port, true);
                s = tls; // closing it closes the underlying socket too
                SSLParameters params = tls.getSSLParameters();
                params.setEndpointIdentificationAlgorithm("HTTPS"); // the certificate must match the host name
                params.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
                tls.setSSLParameters(params);
                tls.startHandshake();
            }
            InputStream newIn = new BufferedInputStream(s.getInputStream());
            OutputStream newOut = s.getOutputStream();
            if (security.password() != null) {
                newOut.write(encode("AUTH", security.password()));
                newOut.flush();
                in = newIn; // readReply reads from the field
                try {
                    readReply();
                } catch (ServerError e) {
                    throw new CacheClientException("authentication to " + host + ":" + port + " failed: " + e.getMessage());
                }
            }
            in = newIn;
            out = newOut;
            socket = s;
        } catch (IOException | RuntimeException e) {
            in = null;
            out = null;
            try {
                s.close();
            } catch (IOException ignored) {
                // already failing
            }
            throw e;
        }
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
            socket = null;
            in = null;
            out = null;
        }
    }

    static byte[] encode(String... args) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String a : args) {
            byte[] bytes = a.getBytes(StandardCharsets.UTF_8);
            b.writeBytes(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            b.writeBytes(bytes);
            b.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return b.toByteArray();
    }

    private Reply readReply() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("server closed the connection");
        }
        String line = readLine();
        switch (type) {
            case '+':
                return new Reply('+', line);
            case '-':
                throw new ServerError(line);
            case ':':
                try {
                    return new Reply(':', Long.parseLong(line));
                } catch (NumberFormatException e) {
                    throw new IOException("malformed integer reply: " + line);
                }
            case '$': {
                int len;
                try {
                    len = Integer.parseInt(line);
                } catch (NumberFormatException e) {
                    throw new IOException("malformed bulk length: " + line);
                }
                if (len < 0) {
                    return new Reply('$', null);
                }
                if (len > MAX_BULK_BYTES) {
                    throw new IOException("bulk reply too large: " + len);
                }
                byte[] data = in.readNBytes(len);
                if (data.length < len || in.read() != '\r' || in.read() != '\n') {
                    throw new IOException("truncated bulk reply");
                }
                return new Reply('$', new String(data, StandardCharsets.UTF_8));
            }
            default:
                throw new IOException("unexpected reply type '" + (char) type + "'");
        }
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != '\r') {
            if (c == -1) {
                throw new IOException("server closed the connection inside a reply");
            }
            sb.append((char) c);
            if (sb.length() > 64 * 1024) {
                throw new IOException("reply line too long");
            }
        }
        if (in.read() != '\n') {
            throw new IOException("malformed reply line ending");
        }
        return sb.toString();
    }

    private static void requireText(String s, String what) {
        if (s == null) {
            throw new IllegalArgumentException(what + " cannot be null");
        }
        // The server rejects larger arguments by dropping the connection; say so clearly instead.
        // Every UTF-8 character is at most 3 bytes per UTF-16 unit, so short strings skip the exact count.
        if (s.length() > CacheServer.MAX_ARGUMENT_BYTES / 3
                && s.getBytes(StandardCharsets.UTF_8).length > CacheServer.MAX_ARGUMENT_BYTES) {
            throw new IllegalArgumentException(what + " is larger than the server limit of "
                    + CacheServer.MAX_ARGUMENT_BYTES + " bytes");
        }
    }
}
