package com.distri.cache.node;

import java.io.IOException;

/**
 * A request could not be completed. {@link #outcomeUnknown()} says whether it may nevertheless have taken effect: true
 * when the request was sent and no definite answer arrived (a write may have been applied; a retry with the same
 * request would be safe, a new request would not be a retry), false when it certainly did not run.
 */
public final class ReplicatedClientException extends IOException {

    private static final long serialVersionUID = 1L;

    private final boolean outcomeUnknown;

    ReplicatedClientException(String message, boolean outcomeUnknown, Throwable cause) {
        super(message, cause);
        this.outcomeUnknown = outcomeUnknown;
    }

    public boolean outcomeUnknown() {
        return outcomeUnknown;
    }
}
