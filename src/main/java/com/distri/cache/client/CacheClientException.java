package com.distri.cache.client;

/** A request to a cache server failed: the connection broke, timed out, or the server replied with an error. */
public class CacheClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CacheClientException(String message) {
        super(message);
    }

    public CacheClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
