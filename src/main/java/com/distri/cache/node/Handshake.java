package com.distri.cache.node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Challenge-response proof that the other side knows a shared secret, without sending the secret.
 *
 * <p>The accepting side sends a random 16-byte challenge; the connecting side answers with
 * {@code HMAC-SHA256(secret, challenge || role || id)}. A recorded answer cannot be replayed on a later connection
 * because the challenge differs. This authenticates who <em>opened</em> the connection; it does not encrypt or sign
 * the traffic that follows, so an attacker who can modify packets on the path could still tamper with an established
 * connection. Use it on a trusted network, or put the links inside a VPN or TLS tunnel.
 */
final class Handshake {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int CHALLENGE_BYTES = 16;

    private Handshake() {}

    /** Accepting side. Returns normally if the peer proved knowledge of the secret (or no secret is configured). */
    static void accept(DataInputStream in, DataOutputStream out, byte[] secret, String role, int claimedId)
            throws IOException {
        byte[] challenge = new byte[CHALLENGE_BYTES];
        RANDOM.nextBytes(challenge);
        out.writeBoolean(secret != null);
        out.write(challenge);
        out.flush();
        byte[] answer = Frames.read(in, 64);
        if (secret != null) {
            byte[] expected = mac(secret, challenge, role, claimedId);
            if (!MessageDigest.isEqual(expected, answer)) {
                throw new IOException("handshake failed: wrong or missing secret");
            }
        }
    }

    /** Connecting side. Fails if the server requires a secret that this side does not have. */
    static void connect(DataInputStream in, DataOutputStream out, byte[] secret, String role, int myId)
            throws IOException {
        boolean required = in.readBoolean();
        byte[] challenge = new byte[CHALLENGE_BYTES];
        in.readFully(challenge);
        if (required && secret == null) {
            throw new IOException("the server requires a secret and none is configured");
        }
        Frames.write(out, secret == null ? new byte[0] : mac(secret, challenge, role, myId));
        out.flush();
    }

    private static byte[] mac(byte[] secret, byte[] challenge, String role, int id) throws IOException {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update(challenge);
            mac.update(role.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) (id >>> 24));
            mac.update((byte) (id >>> 16));
            mac.update((byte) (id >>> 8));
            mac.update((byte) id);
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IOException("HMAC unavailable", e);
        }
    }
}
