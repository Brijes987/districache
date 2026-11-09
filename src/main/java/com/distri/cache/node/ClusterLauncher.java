package com.distri.cache.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Starts replicated servers as separate operating-system processes on this machine, for the demo and the benchmark.
 * Killing one is {@code Process.destroyForcibly()}: a real SIGKILL (or TerminateProcess on Windows), so nothing is
 * flushed or announced, exactly like a crash.
 */
final class ClusterLauncher implements AutoCloseable {

    private final int n;
    private final Path root;
    private final boolean fsync;
    private final int[] peerPorts;
    private final int[] clientPorts;
    private final Process[] processes;
    private final byte[] clientSecret;
    private final Path peerSecretFile;
    private final Path clientSecretFile;
    private final String members;

    ClusterLauncher(int n, Path root, boolean fsync) throws IOException {
        this.n = n;
        this.root = root;
        this.fsync = fsync;
        this.peerPorts = new int[n];
        this.clientPorts = new int[n];
        this.processes = new Process[n];
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < n; i++) {
            peerPorts[i] = freePort();
            clientPorts[i] = freePort();
            if (i > 0) {
                list.append(',');
            }
            list.append(i).append("=127.0.0.1:").append(peerPorts[i]).append(':').append(clientPorts[i]);
        }
        this.members = list.toString();
        Files.createDirectories(root);
        this.peerSecretFile = root.resolve("peer.secret");
        this.clientSecretFile = root.resolve("client.secret");
        Files.writeString(peerSecretFile, randomSecret());
        String clientSecretText = randomSecret();
        Files.writeString(clientSecretFile, clientSecretText);
        this.clientSecret = clientSecretText.getBytes(StandardCharsets.UTF_8);
    }

    private static String randomSecret() {
        byte[] bytes = new byte[24];
        new SecureRandom().nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    void start(int i) throws IOException, InterruptedException {
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> command = new ArrayList<>(List.of(java, "-cp", System.getProperty("java.class.path"),
                ReplicatedServerMain.class.getName(), "--id", String.valueOf(i), "--members", members,
                "--data", root.resolve("node" + i).toString(), "--peer-secret-file", peerSecretFile.toString(),
                "--client-secret-file", clientSecretFile.toString()));
        if (!fsync) {
            command.add("--no-fsync");
        }
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve("node" + i + ".log").toFile()));
        processes[i] = builder.start();
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (System.nanoTime() < deadline) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", clientPorts[i]), 200);
                return; // the client port accepts connections: the server is up
            } catch (IOException notYet) {
                if (!processes[i].isAlive()) {
                    throw new IOException("server " + i + " exited at startup; see " + root.resolve("node" + i + ".log"));
                }
                Thread.sleep(50);
            }
        }
        throw new IOException("server " + i + " did not start within 15 seconds");
    }

    void startAll() throws IOException, InterruptedException {
        for (int i = 0; i < n; i++) {
            start(i);
        }
    }

    /** A real kill: no shutdown hooks run, no goodbye. */
    void kill(int i) throws InterruptedException {
        if (processes[i] != null) {
            processes[i].destroyForcibly();
            processes[i].waitFor();
            processes[i] = null;
        }
    }

    boolean isUp(int i) {
        return processes[i] != null && processes[i].isAlive();
    }

    int size() {
        return n;
    }

    /** Index of the server with this client address, or -1. */
    int indexOfClientAddress(InetSocketAddress address) {
        for (int i = 0; i < n; i++) {
            if (clientPorts[i] == address.getPort()) {
                return i;
            }
        }
        return -1;
    }

    List<InetSocketAddress> clientAddresses() {
        List<InetSocketAddress> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new InetSocketAddress("127.0.0.1", clientPorts[i]));
        }
        return out;
    }

    ReplicatedClient newClient(long operationTimeoutMillis) {
        return new ReplicatedClient(clientAddresses(), clientSecret, operationTimeoutMillis);
    }

    Path root() {
        return root;
    }

    @Override
    public void close() {
        for (int i = 0; i < n; i++) {
            try {
                kill(i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
