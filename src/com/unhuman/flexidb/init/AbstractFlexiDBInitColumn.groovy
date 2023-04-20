package com.unhuman.flexidb.init

abstract class AbstractFlexiDBInitColumn {
    private String name

    // This gets populated when added to the database
    private Integer column

    AbstractFlexiDBInitColumn(String name) {
        this.name = name
    }

    String getName() {
        return name
    }

    protected void setColumn(int column) {
        this.column = column
    }

    int getColumn() {
        return column
    }
}
