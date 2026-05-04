package com.unhuman.managertools.rest.exceptions

class NeedsRetryException extends RESTException {
    private int retryAfter
    NeedsRetryException(int statusCode, String message, String url, int retryAfter) {
        super(statusCode, message, url)
        this.retryAfter = retryAfter
    }

    int getRetryAfter() {
        return retryAfter
    }

    @Override
    String toString() {
        return "NeedsRetryException{" +
                "statusCode=" + statusCode +
                ", message='" + message + '\'' +
                ", url='" + url + '\'' +
                ", retryAfter=" + retryAfter +
                '}'
    }
}
