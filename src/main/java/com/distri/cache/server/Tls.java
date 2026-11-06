package com.distri.cache.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

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

    /** TLS versions the server and client accept. */
    static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};
}
