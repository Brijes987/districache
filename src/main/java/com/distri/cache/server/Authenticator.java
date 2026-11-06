package com.distri.cache.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Checks a password without keeping it: only its SHA-256 digest is stored, and candidates are compared
 * digest-to-digest in constant time, so the comparison time does not depend on how many leading characters
 * match or on the length of the guess.
 *
 * <p>This is a single shared password, not user accounts.
 */
final class Authenticator {

    private final byte[] digest;

    Authenticator(String password) {
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("password cannot be empty");
        }
        this.digest = sha256(password.getBytes(StandardCharsets.UTF_8));
    }

    boolean matches(byte[] candidate) {
        return MessageDigest.isEqual(digest, sha256(candidate));
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }
}
