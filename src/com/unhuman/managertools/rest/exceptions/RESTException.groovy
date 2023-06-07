package com.unhuman.managertools.rest.exceptions

class RESTException extends RuntimeException {
    int statusCode
    String url

    RESTException(int statusCode, String message, String url) {
        super(message)
        this.statusCode = statusCode
        this.url = url
    }

    @Override
    String toString() {
        return "{\"statusCode\": ${statusCode}, \"message\": \"${this.getMessage()}\", \"url\": \"${url}\"}"
    }
}
