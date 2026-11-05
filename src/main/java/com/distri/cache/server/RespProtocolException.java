package com.distri.cache.server;

import java.io.IOException;

/** The client sent bytes that are not valid RESP; the connection cannot be resynchronised. */
final class RespProtocolException extends IOException {
    private static final long serialVersionUID = 1L;

    RespProtocolException(String message) {
        super(message);
    }
}
