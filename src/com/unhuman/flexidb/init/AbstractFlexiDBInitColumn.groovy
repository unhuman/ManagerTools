package com.unhuman.flexidb.init

abstract class AbstractFlexiDBInitColumn {
    private String name
    private Class dataType

    // This gets populated when added to the database
    private Integer column

    AbstractFlexiDBInitColumn(String name, Class dataType) {
        this.name = name
        this.dataType = dataType
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
