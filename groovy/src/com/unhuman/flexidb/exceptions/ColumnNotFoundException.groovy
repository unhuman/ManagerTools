package com.unhuman.flexidb.exceptions

class ColumnNotFoundException extends RuntimeException {
    ColumnNotFoundException(String message) {
        super(message)
    }
}
