package com.unhuman.flexidb.output;

interface OutputFilter {
    Object apply(String columnName, Object value);
}
