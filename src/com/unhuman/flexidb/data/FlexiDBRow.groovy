package com.unhuman.flexidb.data

class FlexiDBRow extends LinkedHashMap<String, Object> {
    FlexiDBRow(int initialCapacity) {
        super(initialCapacity)
    }

    FlexiDBRow() {
    }
}
