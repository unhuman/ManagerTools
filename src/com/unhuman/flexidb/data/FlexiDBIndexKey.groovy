package com.unhuman.flexidb.data

class FlexiDBIndexKey {
    private String key
    private Object value

    FlexiDBIndexKey(String key, String value) {
        this.key = key
        this.value = value
    }

    // equals / hashcode needed for indexing - retainAll() functionality

    boolean equals(o) {
        if (this.is(o)) return true
        if (o == null || getClass() != o.class) return false

        FlexiDBIndexKey that = (FlexiDBIndexKey) o

        if (key != that.key) return false
        if (value != that.value) return false

        return true
    }

    int hashCode() {
        int result
        result = (key != null ? key.hashCode() : 0)
        result = 31 * result + (value != null ? value.hashCode() : 0)
        return result
    }
}
