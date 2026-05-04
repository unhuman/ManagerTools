package com.unhuman.flexidb.init

abstract class AbstractFlexiDBInitColumn {
    private final String name

    AbstractFlexiDBInitColumn(String name) {
        this.name = name
    }

    String getName() {
        return name
    }
}
