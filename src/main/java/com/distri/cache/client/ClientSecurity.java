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

    @Override
    public String toString() { // never print the password
        return "ClientSecurity[password=" + (password == null ? "none" : "set") + ", tls=" + (tlsContext != null) + "]";
    }
}
