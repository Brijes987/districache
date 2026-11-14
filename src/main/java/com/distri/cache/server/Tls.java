package com.distri.cache.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Builds the server's TLS configuration from a PKCS12 keystore holding its certificate and private key. */
public final class Tls {

    private Tls() {}

    /**
     * @param keystore a PKCS12 file with the server's private key and certificate chain
     * @param password the keystore password (also used for the key)
     */
    public static SSLContext serverContext(Path keystore, char[] password) throws IOException {
        try (InputStream in = Files.newInputStream(keystore)) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(in, password);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, password);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("cannot load TLS keystore " + keystore + ": " + e.getMessage(), e);
        }
    }

    /**
     * For a replicated server: it presents the certificate in {@code keystore} and trusts exactly the certificates in
     * {@code truststore}. Used on the peer port, where both ends are servers and both must prove they hold a trusted
     * certificate, and on the client port, where only the server is authenticated.
     */
    public static SSLContext mutualContext(Path keystore, char[] keystorePassword, Path truststore,
            char[] truststorePassword) throws IOException {
        try (InputStream keys = Files.newInputStream(keystore); InputStream trust = Files.newInputStream(truststore)) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(keys, keystorePassword);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword);
            KeyStore ts = KeyStore.getInstance("PKCS12");
            ts.load(trust, truststorePassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("cannot load TLS stores " + keystore + " / " + truststore + ": " + e.getMessage(), e);
        }
    }

    /** For a client: trusts exactly the certificates in {@code truststore}; presents no certificate. */
    public static SSLContext clientContext(Path truststore, char[] truststorePassword) throws IOException {
        try (InputStream in = Files.newInputStream(truststore)) {
            KeyStore ts = KeyStore.getInstance("PKCS12");
            ts.load(in, truststorePassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("cannot load trust store " + truststore + ": " + e.getMessage(), e);
        }
    }

    /**
     * Wraps an accepted socket as the TLS server side and completes the handshake (the caller has already set a read
     * timeout, so a peer that stalls cannot hold the thread). {@code requireClientCertificate} is for the peer port.
     */
    public static java.net.Socket acceptTls(SSLContext context, java.net.Socket raw, boolean requireClientCertificate)
            throws IOException {
        javax.net.ssl.SSLSocket ssl = (javax.net.ssl.SSLSocket) context.getSocketFactory()
                .createSocket(raw, null, raw.getPort(), true);
        ssl.setUseClientMode(false);
        ssl.setEnabledProtocols(PROTOCOLS);
        ssl.setNeedClientAuth(requireClientCertificate);
        ssl.startHandshake();
        return ssl;
    }

    /**
     * Wraps a connected socket as the TLS client side and completes the handshake. The server's certificate must be
     * trusted AND match {@code host}.
     */
    public static java.net.Socket connectTls(SSLContext context, java.net.Socket raw, String host, int port)
            throws IOException {
        javax.net.ssl.SSLSocket ssl = (javax.net.ssl.SSLSocket) context.getSocketFactory()
                .createSocket(raw, host, port, true);
        javax.net.ssl.SSLParameters params = ssl.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        params.setProtocols(PROTOCOLS);
        ssl.setSSLParameters(params);
        ssl.startHandshake();
        return ssl;
    }

    /** TLS versions the server and client accept. */
    public static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};
}
