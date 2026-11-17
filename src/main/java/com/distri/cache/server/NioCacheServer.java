package com.distri.cache.server;

import com.distri.cache.ShardedLRUCache;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The same Redis-protocol server as {@link CacheServer}, driven by a few event-loop threads instead of one thread per
 * connection.
 *
 * <p><b>Model:</b> one accept thread, and {@code ioThreads} loop threads that each own a {@link Selector} and a share
 * of the connections. A loop reads whatever bytes have arrived, parses every whole command out of them
 * ({@link RespNioParser}), runs each with the same {@link CommandExecutor} the blocking server uses, and writes the
 * replies. Commands run on the loop thread, so a command must not block: the cache operations are in-memory and
 * short. Two places could block and are handled: a failed {@code AUTH} is delayed by holding that connection's reply,
 * not by sleeping; and a client that does not read its replies stops being read from once 256 KiB of replies are
 * waiting, so memory stays bounded.
 *
 * <p><b>Not supported</b> (use {@link CacheServer} for these): TLS, and replication (a replica link takes over a
 * blocking socket). Persistence works, but an append-only log with {@code fsync always} makes a write wait for the
 * disk <i>on the loop thread</i>, delaying every other connection of that loop meanwhile.
 *
 * <p>Differences in behaviour from the blocking server: the pre-authentication timeout is measured from connect
 * time to a successful {@code AUTH}, not as a per-read timeout; there is still no idle timeout once authenticated.
 */
public final class NioCacheServer implements AutoCloseable {

    /** Replies waiting to be written beyond which the connection is not read from (and no more commands are run). */
    static final int OUTPUT_HIGH_WATER_BYTES = 256 * 1024;

    private static final int READ_CHUNK_BYTES = 64 * 1024;
    private static final long SWEEP_MILLIS = 50;
    private static final System.Logger LOG = System.getLogger(NioCacheServer.class.getName());

    private final ShardedLRUCache<String, String> cache;
    private final String host;
    private final int requestedPort;
    private final int maxClients;
    private final WriteCoordinator coordinator; // null: no persistence
    private final Authenticator authenticator;  // null: no password
    private final AtomicInteger open = new AtomicInteger();

    private int ioThreads = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
    private volatile int preAuthTimeoutMillis = CacheServer.PRE_AUTH_TIMEOUT_MILLIS;
    private volatile boolean running;
    private volatile ServerMetrics metrics;
    private ServerSocketChannel serverChannel;
    private Thread acceptThread;
    private Loop[] loops;

    public NioCacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients) {
        this(cache, host, port, maxClients, null, CacheServer.Security.NONE);
    }

    public NioCacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients,
            CacheServer.Security security) {
        this(cache, host, port, maxClients, null, security);
    }

    /** With persistence: every write is also appended to {@code log}. The caller closes the log. */
    NioCacheServer(ShardedLRUCache<String, String> cache, String host, int port, int maxClients, AppendOnlyLog log,
            CacheServer.Security security) {
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be positive");
        }
        if (security == null) {
            throw new IllegalArgumentException("security cannot be null (use Security.NONE)");
        }
        if (security.tlsContext() != null) {
            throw new IllegalArgumentException("TLS is not supported by the event-loop server; use the blocking server");
        }
        this.cache = cache;
        this.host = host;
        this.requestedPort = port;
        this.maxClients = maxClients;
        this.coordinator = log != null ? new WriteCoordinator(cache, log, null) : null;
        this.authenticator = security.password() == null ? null : new Authenticator(security.password());
    }

    /** Number of event-loop threads (default: the CPU count, at most 4). Call before {@link #start()}. */
    public synchronized NioCacheServer ioThreads(int n) {
        if (running) {
            throw new IllegalStateException("call ioThreads before start()");
        }
        if (n <= 0) {
            throw new IllegalArgumentException("ioThreads must be positive");
        }
        this.ioThreads = n;
        return this;
    }

    /** For tests: shorten the pre-authentication timeout. Call before {@link #start()}. */
    void setPreAuthTimeoutMillis(int millis) {
        this.preAuthTimeoutMillis = millis;
    }

    /** As {@link CacheServer#enableMetrics}: commands, hit ratio, latency, connections and cache size. */
    public synchronized NioCacheServer enableMetrics(com.distri.cache.metrics.MetricsRegistry registry) {
        if (running) {
            throw new IllegalStateException("call enableMetrics before start()");
        }
        ServerMetrics m = new ServerMetrics(registry);
        registry.gauge("districache_cache_entries", "Live entries in the cache.", cache::size);
        registry.gauge("districache_cache_capacity", "Configured capacity in entries.", cache::capacity);
        registry.gauge("districache_connections", "Currently open client connections.", open::get);
        this.metrics = m;
        return this;
    }

    public synchronized NioCacheServer start() throws IOException {
        if (running) {
            throw new IllegalStateException("already started");
        }
        serverChannel = ServerSocketChannel.open();
        try {
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverChannel.bind(new InetSocketAddress(host, requestedPort));
            loops = new Loop[ioThreads];
            for (int i = 0; i < loops.length; i++) {
                loops[i] = new Loop(i);
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly(serverChannel);
            throw e;
        }
        running = true;
        for (Loop loop : loops) {
            loop.thread.start();
        }
        acceptThread = new Thread(this::acceptLoop, "nio-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        return this;
    }

    public int port() {
        return ((InetSocketAddress) localAddress()).getPort();
    }

    private java.net.SocketAddress localAddress() {
        try {
            return serverChannel.getLocalAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Number of connections currently open. */
    public int connectionCount() {
        return open.get();
    }

    // ---- accepting -------------------------------------------------------------------------------------------

    private void acceptLoop() {
        int next = 0;
        while (running) {
            SocketChannel ch;
            try {
                ch = serverChannel.accept();
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "accept failed", e);
                }
                return;
            }
            ServerMetrics m = metrics;
            if (open.incrementAndGet() > maxClients) {
                open.decrementAndGet();
                if (m != null) {
                    m.connectionsRejected.increment();
                }
                reject(ch);
                continue;
            }
            if (m != null) {
                m.connectionsAccepted.increment();
            }
            try {
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                ch.configureBlocking(false);
            } catch (IOException e) {
                open.decrementAndGet();
                closeQuietly(ch);
                continue;
            }
            Loop loop = loops[next++ % loops.length];
            loop.incoming.add(ch);
            loop.selector.wakeup();
        }
    }

    private static void reject(SocketChannel ch) {
        try (ch) {
            ch.write(ByteBuffer.wrap("-ERR max number of clients reached\r\n".getBytes(StandardCharsets.US_ASCII)));
            ch.shutdownOutput(); // FIN after the reply, as in the blocking server
        } catch (IOException ignored) {
            // the client is gone
        }
    }

    // ---- one event loop --------------------------------------------------------------------------------------

    /** A reply buffer a {@link RespWriter} can write into; bytes between {@code sent} and {@code len} are unsent. */
    private static final class OutBuf extends OutputStream {
        byte[] data = new byte[128];
        int len;
        int sent;

        int pending() {
            return len - sent;
        }

        @Override
        public void write(int b) {
            ensure(1);
            data[len++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int n) {
            ensure(n);
            System.arraycopy(b, off, data, len, n);
            len += n;
        }

        private void ensure(int extra) {
            if (len + extra > data.length) {
                data = java.util.Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 16L,
                        Math.max(data.length * 2L, (long) len + extra)));
            }
        }

        void reset() {
            len = 0;
            sent = 0;
            if (data.length > 16 * 1024) {
                data = new byte[128];
            }
        }
    }

    private final class Conn {
        final SocketChannel channel;
        SelectionKey key;
        final RespNioParser parser = new RespNioParser();
        final OutBuf out = new OutBuf();
        final RespWriter writer = new RespWriter(out, 256);
        final CommandExecutor executor;
        long preAuthDeadlineNanos; // 0: none
        long resumeAtNanos;        // 0: not held back by the failed-AUTH delay
        boolean closing;           // close once everything queued has been written
        boolean peerClosed;        // the client sent EOF
        boolean stoppedForBacklog; // stopped running commands because replies are piling up
        boolean closed;
        boolean lingering;         // everything has been sent and end-of-output signalled; discarding input until EOF
        long lingerDeadlineNanos;
        int interest;

        Conn(SocketChannel channel) {
            this.channel = channel;
            this.executor = new CommandExecutor(cache, coordinator, authenticator, null);
            this.executor.deferAuthDelay();
            this.executor.setMetrics(metrics);
        }

        boolean timed() {
            return preAuthDeadlineNanos != 0 || resumeAtNanos != 0 || lingerDeadlineNanos != 0;
        }
    }

    private final class Loop implements Runnable {
        final Selector selector;
        final Thread thread;
        final Queue<SocketChannel> incoming = new ConcurrentLinkedQueue<>();
        final Set<Conn> timed = new HashSet<>();
        final ByteBuffer readBuf = ByteBuffer.allocateDirect(READ_CHUNK_BYTES);
        volatile boolean stop;

        Loop(int index) throws IOException {
            this.selector = Selector.open();
            this.thread = new Thread(this, "nio-loop-" + index);
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    selector.select(timed.isEmpty() ? 0 : SWEEP_MILLIS);
                    if (stop) {
                        break;
                    }
                    adopt();
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        Conn c = (Conn) key.attachment();
                        try {
                            if (key.isValid() && key.isReadable()) {
                                onReadable(c);
                            }
                            if (!c.closed && key.isValid() && key.isWritable()) {
                                onWritable(c);
                            }
                        } catch (CancelledKeyException | IOException e) {
                            close(c);
                        } catch (RuntimeException | Error e) {
                            LOG.log(System.Logger.Level.ERROR, "connection failed", e);
                            close(c);
                        }
                    }
                    if (!timed.isEmpty()) {
                        sweep();
                    }
                }
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.ERROR, "event loop stopped", e);
                }
            } finally {
                shutdownLoop();
            }
        }

        private void adopt() {
            SocketChannel ch;
            while ((ch = incoming.poll()) != null) {
                Conn c = new Conn(ch);
                try {
                    c.key = ch.register(selector, SelectionKey.OP_READ, c);
                    c.interest = SelectionKey.OP_READ;
                } catch (IOException e) {
                    open.decrementAndGet();
                    closeQuietly(ch);
                    continue;
                }
                if (authenticator != null) {
                    c.preAuthDeadlineNanos = System.nanoTime() + preAuthTimeoutMillis * 1_000_000L;
                    timed.add(c);
                }
            }
        }

        private void onReadable(Conn c) throws IOException {
            readBuf.clear();
            int n = c.channel.read(readBuf);
            if (c.lingering) {
                if (n < 0) {
                    close(c);
                }
                return; // input after a decision to close is discarded
            }
            if (n < 0) {
                c.peerClosed = true;
                drive(c);
                return;
            }
            if (n == 0) {
                return;
            }
            readBuf.flip();
            try {
                c.parser.feed(readBuf);
            } catch (RespProtocolException e) { // a single command bigger than anything the limits allow
                c.writer.error("ERR Protocol error: " + e.getMessage());
                c.writer.flush();
                c.closing = true;
            }
            drive(c);
        }

        private void onWritable(Conn c) throws IOException {
            if (c.resumeAtNanos == 0 && flush(c)) {
                drive(c);
            }
        }

        /** Runs buffered commands and writes replies until there is nothing more to do right now. */
        private void drive(Conn c) throws IOException {
            while (true) {
                processBuffered(c);
                if (c.resumeAtNanos != 0) {
                    break; // the failed-AUTH delay: nothing is sent until the sweep releases this connection
                }
                if (!flush(c)) {
                    break; // the socket's send buffer is full; OP_WRITE will call back
                }
                if (c.closing && !c.peerClosed) {
                    startLinger(c);
                    return;
                }
                if (c.closing || (c.peerClosed && !c.stoppedForBacklog)) {
                    close(c);
                    return;
                }
                if (!c.stoppedForBacklog) {
                    break;
                }
            }
            updateInterest(c);
        }

        private void processBuffered(Conn c) throws IOException {
            c.stoppedForBacklog = false;
            ServerMetrics m = metrics;
            while (!c.closing && c.resumeAtNanos == 0) {
                if (c.out.pending() >= OUTPUT_HIGH_WATER_BYTES) {
                    c.stoppedForBacklog = true;
                    return;
                }
                List<byte[]> command;
                try {
                    command = c.parser.next();
                } catch (RespProtocolException e) {
                    c.writer.error("ERR Protocol error: " + e.getMessage());
                    c.writer.flush();
                    c.closing = true;
                    return;
                }
                if (command == null) {
                    return;
                }
                if (command.isEmpty()) {
                    continue;
                }
                CommandExecutor.Result result;
                long startedAt = m != null ? System.nanoTime() : 0;
                try {
                    result = c.executor.execute(command, c.writer);
                    if (m != null) {
                        m.record(new String(command.get(0), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT),
                                System.nanoTime() - startedAt);
                    }
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.ERROR, "command failed", e);
                    c.writer.error("ERR internal error");
                    result = CommandExecutor.Result.CONTINUE;
                }
                c.writer.flush();
                if (c.preAuthDeadlineNanos != 0 && c.executor.isAuthenticated()) {
                    c.preAuthDeadlineNanos = 0;
                    if (!c.timed()) {
                        timed.remove(c);
                    }
                }
                long delay = c.executor.takeAuthDelayMillis();
                if (delay > 0) {
                    c.resumeAtNanos = System.nanoTime() + delay * 1_000_000L;
                    timed.add(c);
                }
                if (result == CommandExecutor.Result.CLOSE || result == CommandExecutor.Result.REPLICATE) {
                    c.closing = true;
                }
            }
        }

        /** Writes as much as the socket takes. True when nothing is left to write. */
        private boolean flush(Conn c) throws IOException {
            OutBuf o = c.out;
            while (o.sent < o.len) {
                int w = c.channel.write(ByteBuffer.wrap(o.data, o.sent, o.len - o.sent));
                if (w == 0) {
                    return false;
                }
                o.sent += w;
            }
            o.reset();
            return true;
        }

        /**
         * Everything is written. Signal end-of-output and keep reading (discarding) until the client closes or a
         * second passes: closing with unread input would send a reset, which on Windows can destroy the reply.
         */
        private void startLinger(Conn c) throws IOException {
            c.channel.shutdownOutput();
            c.lingering = true;
            c.lingerDeadlineNanos = System.nanoTime() + 1_000_000_000L;
            c.preAuthDeadlineNanos = 0;
            timed.add(c);
            c.interest = SelectionKey.OP_READ;
            c.key.interestOps(SelectionKey.OP_READ);
        }

        private void updateInterest(Conn c) {
            if (c.lingering) {
                return;
            }
            int ops = 0;
            if (c.out.pending() > 0) {
                ops |= SelectionKey.OP_WRITE;
            }
            if (c.resumeAtNanos == 0 && !c.closing && !c.peerClosed && c.out.pending() < OUTPUT_HIGH_WATER_BYTES) {
                ops |= SelectionKey.OP_READ;
            }
            if (ops != c.interest) {
                c.interest = ops;
                c.key.interestOps(ops);
            }
        }

        /** Releases connections whose failed-AUTH delay is over and closes ones that did not authenticate in time. */
        private void sweep() {
            long now = System.nanoTime();
            for (Conn c : new java.util.ArrayList<>(timed)) {
                if (c.closed) {
                    timed.remove(c);
                    continue;
                }
                try {
                    if (c.preAuthDeadlineNanos != 0 && now - c.preAuthDeadlineNanos >= 0) {
                        close(c);
                        continue;
                    }
                    if (c.lingering && now - c.lingerDeadlineNanos >= 0) {
                        close(c);
                        continue;
                    }
                    if (c.resumeAtNanos != 0 && now - c.resumeAtNanos >= 0) {
                        c.resumeAtNanos = 0;
                        if (!c.timed()) {
                            timed.remove(c);
                        }
                        drive(c);
                    }
                } catch (IOException | CancelledKeyException e) {
                    close(c);
                } catch (RuntimeException | Error e) {
                    LOG.log(System.Logger.Level.ERROR, "connection failed", e);
                    close(c);
                }
            }
        }

        private void close(Conn c) {
            if (c.closed) {
                return;
            }
            c.closed = true;
            timed.remove(c);
            if (c.key != null) {
                c.key.cancel();
            }
            closeQuietly(c.channel);
            open.decrementAndGet();
        }

        private void shutdownLoop() {
            SocketChannel ch;
            while ((ch = incoming.poll()) != null) {
                open.decrementAndGet();
                closeQuietly(ch);
            }
            try {
                for (SelectionKey key : new java.util.ArrayList<>(selector.keys())) {
                    if (key.attachment() instanceof Conn c) {
                        close(c);
                    }
                }
                selector.close();
            } catch (IOException | RuntimeException ignored) {
                // closing
            }
        }
    }

    /** Stops accepting, closes every connection and waits briefly for the threads. */
    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        closeQuietly(serverChannel);
        for (Loop loop : loops) {
            loop.stop = true;
            loop.selector.wakeup();
        }
        try {
            for (Loop loop : loops) {
                loop.thread.join(2000);
            }
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
