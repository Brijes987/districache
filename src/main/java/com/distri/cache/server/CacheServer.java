package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP server that exposes a {@link ShardedLRUCache} through a small subset of the Redis protocol.
 *
 * <p>One thread serves each connection (blocking I/O), up to {@code maxClients} at a time; further
 * connections get an error reply and are closed. Pipelined commands are answered in order and their
 * replies are flushed together.
 *
 * <p><b>Security:</b> there is no authentication or TLS, so the server binds to the loopback address by
 * default. Do not expose it to an untrusted network. There is also no idle timeout.
 */
public final class CacheServer implements AutoCloseable {

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_MAX_CLIENTS = 128;

    private static final System.Logger LOG = System.getLogger(CacheServer.class.getName());

    private final ShardedLRUCache<String, String> cache;
    private final String host;
    private final int requestedPort;
    private final Semaphore clientSlots;
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();

    private ServerSocket serverSocket;
    private ExecutorService pool;
    private Thread acceptThread;
    private volatile boolean running;

    public CacheServer(ShardedLRUCache<String, String> cache, int port) {
        this(cache, DEFAULT_HOST, port, DEFAULT_MAX_CLIENTS);
    }

    /**
     * @param port the port to listen on; 0 picks a free port (see {@link #port()})
     * @param maxClients maximum simultaneous connections
     */
    public CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients) {
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be positive");
        }
        this.cache = cache;
        this.host = host;
        this.requestedPort = port;
        this.clientSlots = new Semaphore(maxClients);
    }

    /** Binds the socket and starts accepting connections. */
    public synchronized CacheServer start() throws IOException {
        if (running) {
            throw new IllegalStateException("already started");
        }
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(host, requestedPort));
        AtomicInteger counter = new AtomicInteger();
        pool = Executors.newCachedThreadPool(runnable -> {
            Thread t = new Thread(runnable, "cache-conn-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        running = true;
        acceptThread = new Thread(this::acceptLoop, "cache-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        return this;
    }

    /** The port actually bound (useful when 0 was requested). */
    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "accept failed", e);
                }
                return;
            }
            if (!clientSlots.tryAcquire()) {
                reject(socket);
                continue;
            }
            clients.add(socket);
            try {
                pool.execute(() -> serve(socket));
            } catch (RuntimeException e) { // pool already shut down
                clients.remove(socket);
                clientSlots.release();
                closeQuietly(socket);
                return;
            }
        }
    }

    private void reject(Socket socket) {
        try (socket) {
            socket.getOutputStream().write(
                    "-ERR max number of clients reached\r\n".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
        } catch (IOException ignored) {
            // the client is gone; nothing to do
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true);
            RespReader reader = new RespReader(socket.getInputStream());
            RespWriter writer = new RespWriter(socket.getOutputStream());
            CommandExecutor executor = new CommandExecutor(cache);
            while (true) {
                List<byte[]> command;
                try {
                    command = reader.readCommand();
                } catch (RespProtocolException e) {
                    writer.error("ERR Protocol error: " + e.getMessage());
                    writer.flush();
                    return;
                }
                if (command == null) {
                    return; // clean disconnect
                }
                if (command.isEmpty()) {
                    continue;
                }
                CommandExecutor.Result result;
                try {
                    result = executor.execute(command, writer);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.ERROR, "command failed", e);
                    writer.error("ERR internal error");
                    result = CommandExecutor.Result.CONTINUE;
                }
                if (result == CommandExecutor.Result.CLOSE || !reader.hasBufferedInput()) {
                    writer.flush();
                }
                if (result == CommandExecutor.Result.CLOSE) {
                    return;
                }
            }
        } catch (EOFException | SocketException e) {
            // client went away mid-command, or the server is shutting down
        } catch (IOException e) {
            if (running) {
                LOG.log(System.Logger.Level.WARNING, "connection error", e);
            }
        } finally {
            clients.remove(socket);
            clientSlots.release();
        }
    }

    /** Stops accepting, closes every connection and waits briefly for the worker threads. */
    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        closeQuietly(serverSocket);
        for (Socket client : clients) {
            closeQuietly(client);
        }
        pool.shutdownNow();
        try {
            pool.awaitTermination(2, TimeUnit.SECONDS);
            acceptThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(java.io.Closeable c) {
        try {
            c.close();
        } catch (IOException ignored) {
            // closing: nothing useful to do
        }
    }
}
