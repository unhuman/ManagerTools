package com.unhuman.flexidb.init

class FlexiDBInitDataColumn extends AbstractFlexiDBInitColumn {
    private final Object defaultValue

    FlexiDBInitDataColumn(String name, Object defaultValue) {
        super(name)
        this.defaultValue = defaultValue
    }

    Object getDefaultValue() {
        return defaultValue
    }
}
