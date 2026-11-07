package com.distri.cache.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Connection protection for {@link CacheClient} and {@link ClusterClient}.
 *
 * @param password sent with {@code AUTH} each time a connection is opened, or null for none
 * @param tlsContext if not null, connections use TLS; the server's certificate must be trusted by this
 *     context and must match the host name used to connect
 */
public record ClientSecurity(String password, SSLContext tlsContext) {

    public static final ClientSecurity NONE = new ClientSecurity(null, null);

    public ClientSecurity {
        if (password != null && password.isEmpty()) {
            throw new IllegalArgumentException("password cannot be empty");
        }
    }

    /** Password only, no TLS. */
    public static ClientSecurity password(String password) {
        return new ClientSecurity(password, null);
    }

    /** TLS that trusts the certificates in a PKCS12 trust store, plus an optional password. */
    public static ClientSecurity tls(Path trustStore, char[] trustStorePassword, String password) throws IOException {
        try (InputStream in = Files.newInputStream(trustStore)) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(in, trustStorePassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return new ClientSecurity(password, context);
        } catch (GeneralSecurityException e) {
            throw new IOException("cannot load trust store " + trustStore + ": " + e.getMessage(), e);
        }
    }

    /**
     * Opens a connection to {@code host:port}: TCP connect, then (if TLS is configured) a handshake that checks the
     * certificate against the trust store and the host name. The password is NOT sent here.
     */
    public java.net.Socket open(String host, int port, int connectTimeoutMillis, int readTimeoutMillis)
            throws IOException {
        java.net.Socket s = new java.net.Socket();
        try {
            s.connect(new java.net.InetSocketAddress(host, port), connectTimeoutMillis);
            s.setSoTimeout(readTimeoutMillis);
            s.setTcpNoDelay(true);
            if (tlsContext != null) {
                javax.net.ssl.SSLSocket tls = (javax.net.ssl.SSLSocket) tlsContext.getSocketFactory()
                        .createSocket(s, host, port, true);
                s = tls; // closing it closes the underlying socket too
                javax.net.ssl.SSLParameters params = tls.getSSLParameters();
                params.setEndpointIdentificationAlgorithm("HTTPS"); // the certificate must match the host name
                params.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
                tls.setSSLParameters(params);
                tls.startHandshake();
            }
            return s;
        } catch (IOException | RuntimeException e) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already failing
            }
            throw e;
        }
    }

    @Override
    public String toString() { // never print the password
        return "ClientSecurity[password=" + (password == null ? "none" : "set") + ", tls=" + (tlsContext != null) + "]";
    }
}
