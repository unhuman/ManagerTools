package com.unhuman.flexidb.data

class FlexiDBRow extends LinkedHashMap<String, Object> {
    static final Map<String, Object> defaults = new HashMap<>()
    FlexiDBRow(int initialCapacity) {
        super(initialCapacity)
    }

    static void setDefault(String key, Object value) {
        defaults.put(key, value)
    }

    // TODO: Do we really want this?
    static Object getDefault(String key) {
        return defaults.get(key)
    }

    @Override
    Object get(Object key) {
        return (super.containsKey(key) ? super.get(key) : defaults.get(key))
    }
}
