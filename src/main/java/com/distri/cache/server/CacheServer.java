package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
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

    /** Largest key or value the server accepts, in UTF-8 bytes. */
    public static final int MAX_ARGUMENT_BYTES = RespReader.MAX_BULK_BYTES;

    private static final System.Logger LOG = System.getLogger(CacheServer.class.getName());

    private final ShardedLRUCache<String, String> cache;
    private final String host;
    private final int requestedPort;
    private final Semaphore clientSlots;
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();

    private final AppendOnlyLog log;
    private final Replication replication;      // null: replication not enabled
    private final WriteCoordinator coordinator; // null: neither persistence nor replication
    private final Authenticator authenticator; // null: no password
    private final SSLContext tls; // null: plain TCP

    private ServerSocket serverSocket;
    private ExecutorService pool;
    private Thread acceptThread;
    private volatile boolean running;
    private volatile ServerMetrics metrics; // null: metrics off

    public CacheServer(ShardedLRUCache<String, String> cache, int port) {
        this(cache, DEFAULT_HOST, port, DEFAULT_MAX_CLIENTS);
    }

    /**
     * @param port the port to listen on; 0 picks a free port (see {@link #port()})
     * @param maxClients maximum simultaneous connections
     */
    public CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients) {
        this(cache, host, port, maxClients, null, Security.NONE);
    }

    /** As above, with a password and/or TLS. */
    public CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients,
            Security security) {
        this(cache, host, port, maxClients, null, security);
    }

    /** As above, with persistence: every write is also appended to {@code log}. The caller closes the log. */
    CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients, AppendOnlyLog log) {
        this(cache, host, port, maxClients, log, Security.NONE);
    }

    CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients, AppendOnlyLog log,
            Security security) {
        this(cache, host, port, maxClients, log, security, null);
    }

    /**
     * With replication. {@code replication} must have been built on the same cache and the same log. The caller
     * closes the log and the replication object.
     */
    CacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients, AppendOnlyLog log,
            Security security, Replication replication) {
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be positive");
        }
        if (security == null) {
            throw new IllegalArgumentException("security cannot be null (use Security.NONE)");
        }
        this.cache = cache;
        this.log = log;
        this.replication = replication;
        this.coordinator = replication != null ? replication.coordinator()
                : (log != null ? new WriteCoordinator(cache, log, null) : null);
        this.authenticator = security.password() == null ? null : new Authenticator(security.password());
        this.tls = security.tlsContext();
        this.host = host;
        this.requestedPort = port;
        this.clientSlots = new Semaphore(maxClients);
    }

    /**
     * Optional protection for connections.
     *
     * @param password one shared password clients must send with {@code AUTH} before any other command, or null
     * @param tlsContext if not null, connections use TLS with this context's certificate
     */
    public record Security(String password, SSLContext tlsContext) {
        public static final Security NONE = new Security(null, null);

        public Security {
            if (password != null && password.isEmpty()) {
                throw new IllegalArgumentException("password cannot be empty");
            }
        }

        @Override
        public String toString() { // never print the password
            return "Security[password=" + (password == null ? "none" : "set") + ", tls=" + (tlsContext != null) + "]";
        }
    }

    /** How long a client may take to finish the TLS handshake, and to send AUTH when a password is required. */
    static final int PRE_AUTH_TIMEOUT_MILLIS = 10_000;

    private volatile int preAuthTimeoutMillis = PRE_AUTH_TIMEOUT_MILLIS;

    /** For tests: shorten the pre-authentication timeout. Call before {@link #start()}. */
    void setPreAuthTimeoutMillis(int millis) {
        this.preAuthTimeoutMillis = millis;
    }

    /**
     * Publishes this server's numbers (commands, hit ratio, latency, connections, cache size, and the log and
     * replication state when those are on) into {@code registry}. Call before {@link #start()}. Without a call, the
     * server records nothing and pays nothing.
     */
    public synchronized CacheServer enableMetrics(com.distri.cache.metrics.MetricsRegistry registry) {
        if (running) {
            throw new IllegalStateException("call enableMetrics before start()");
        }
        ServerMetrics m = new ServerMetrics(registry);
        registry.gauge("districache_cache_entries", "Live entries in the cache.", cache::size);
        registry.gauge("districache_cache_capacity", "Configured capacity in entries.", cache::capacity);
        registry.gauge("districache_connections", "Currently open client connections.", clients::size);
        if (log != null) {
            registry.gauge("districache_aof_size_bytes", "Size of the append-only log file.", log::sizeBytes);
        }
        if (replication != null) {
            registry.gauge("districache_replication_is_replica", "1 when this server is a replica, 0 when a primary.",
                    () -> replication.isReplica() ? 1 : 0);
            registry.gauge("districache_replication_offset", "Replication stream offset in bytes.", () -> {
                ReplicaLink link = replication.link();
                return link != null ? link.offset() : replication.hub().offset();
            });
            registry.gauge("districache_replication_connected_replicas", "Replicas attached to this primary.",
                    () -> replication.hub().replicaCount());
            registry.gauge("districache_replication_link_up", "On a replica: 1 when connected to its primary.", () -> {
                ReplicaLink link = replication.link();
                return link != null && link.isConnected() ? 1 : 0;
            });
            registry.gauge("districache_replication_full_syncs", "Full resynchronisations served so far (primary).",
                    () -> replication.hub().fullSyncs());
            registry.gauge("districache_replication_partial_syncs", "Partial resynchronisations served so far (primary).",
                    () -> replication.hub().partialSyncs());
        }
        this.metrics = m;
        return this;
    }

    /** Binds the socket and starts accepting connections. */
    public synchronized CacheServer start() throws IOException {
        if (running) {
            throw new IllegalStateException("already started");
        }
        if (tls != null) {
            SSLServerSocket tlsSocket = (SSLServerSocket) tls.getServerSocketFactory().createServerSocket();
            tlsSocket.setEnabledProtocols(Tls.PROTOCOLS);
            serverSocket = tlsSocket;
        } else {
            serverSocket = new ServerSocket();
        }
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
            ServerMetrics m = metrics;
            if (!clientSlots.tryAcquire()) {
                if (m != null) {
                    m.connectionsRejected.increment();
                }
                reject(socket);
                continue;
            }
            if (m != null) {
                m.connectionsAccepted.increment();
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
        if (socket instanceof SSLSocket) {
            closeQuietly(socket); // no TLS handshake on the accept thread just to deliver an error
            return;
        }
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
            if (tls != null || authenticator != null) {
                // A client that never finishes the handshake or never authenticates must not hold a slot forever.
                socket.setSoTimeout(preAuthTimeoutMillis);
            }
            if (socket instanceof SSLSocket tlsConnection) {
                tlsConnection.startHandshake();
            }
            RespReader reader = new RespReader(socket.getInputStream());
            RespWriter writer = new RespWriter(socket.getOutputStream());
            CommandExecutor executor = new CommandExecutor(cache, coordinator, authenticator, replication);
            final ServerMetrics m = metrics;
            executor.setMetrics(m);
            boolean preAuthLimit = !executor.isAuthenticated() && authenticator != null;
            if (!preAuthLimit) {
                socket.setSoTimeout(0);
            }
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
                long startedAt = m != null ? System.nanoTime() : 0;
                try {
                    result = executor.execute(command, writer);
                    if (m != null && result != CommandExecutor.Result.REPLICATE) {
                        // a REPLICATE command turns this connection into a long-lived stream: not a latency sample
                        m.record(new String(command.get(0), StandardCharsets.UTF_8).toUpperCase(java.util.Locale.ROOT),
                                System.nanoTime() - startedAt);
                    }
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.ERROR, "command failed", e);
                    writer.error("ERR internal error");
                    result = CommandExecutor.Result.CONTINUE;
                }
                if (preAuthLimit && executor.isAuthenticated()) {
                    preAuthLimit = false;
                    socket.setSoTimeout(0); // authenticated: no more pre-auth limit (there is still no idle timeout)
                }
                if (result == CommandExecutor.Result.REPLICATE) {
                    // This connection now carries the replication stream until it ends.
                    writer.flush();
                    String[] psync = executor.pendingPsync();
                    socket.setSoTimeout(0);
                    replication.hub().serveReplica(socket, reader, writer, psync[0], Long.parseLong(psync[1]), cache,
                            replication.coordinator());
                    return;
                }
                if (result == CommandExecutor.Result.CLOSE || !reader.hasBufferedInput()) {
                    writer.flush();
                }
                if (result == CommandExecutor.Result.CLOSE) {
                    return;
                }
            }
        } catch (EOFException | SocketException | java.net.SocketTimeoutException e) {
            // client went away mid-command, timed out before authenticating, or the server is shutting down
        } catch (javax.net.ssl.SSLException e) {
            // failed handshake (a plain-text client, a scanner, a wrong certificate): not worth a warning each time
            LOG.log(System.Logger.Level.DEBUG, "TLS failure: " + e.getMessage());
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
