package com.unhuman.flexidb

class FlexiDBQueryColumn {
    private String name
    private Object matchValue

    FlexiDBQueryColumn(String name, Object matchValue) {
        this.name = name
        this.matchValue = matchValue
    }

    String getName() {
        return name
    }

    Object getMatchValue() {
        return matchValue
    }


    @Override
    String toString() {
        String valueQuotation = (matchValue instanceof String) ? "'" : ""
        return "FlexiDBQueryColumn{'${name}':${valueQuotation}${matchValue}${valueQuotation}}"
    }
}
