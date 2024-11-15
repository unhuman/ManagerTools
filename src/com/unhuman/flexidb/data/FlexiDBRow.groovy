package com.unhuman.flexidb.data

import com.unhuman.flexidb.output.OutputFilter
@Grapes(
        @Grab(group='org.apache.commons', module='commons-text', version='1.11.0')
)

import org.apache.commons.text.StringEscapeUtils

class FlexiDBRow extends LinkedHashMap<String, Object> {
    static final Map<String, Object> defaults = new HashMap<>()

    static final char CSV_SEPARATOR = ','

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

    static String headingsToCSV(List<String> columnOrder) {
        StringBuilder sb = new StringBuilder(512)
        for (int i = 0; i < columnOrder.size(); i++) {
            String columnName = columnOrder.get(i)
            sb.append((i > 0) ? CSV_SEPARATOR : "")

            // if a columnName is null, that's an empty value
            if (columnName == null) {
                continue
            }

            sb.append(columnOrder.get(i))
        }
        return sb.toString()
    }

    String toCSV(List<String> columnOrder) {
        return toCSV(columnOrder, Collections.emptyList())
    }

    /**
     *
     * @param columnOrder
     * @param outputRules - applied in onder, stops when first one takes effect
     * @return
     */
    String toCSV(List<String> columnOrder, List<OutputFilter> outputRules) {
        StringBuilder sb = new StringBuilder(512)
        for (int i = 0; i < columnOrder.size(); i++) {
            String columnName = columnOrder.get(i)
            sb.append((i > 0) ? CSV_SEPARATOR : "")

            // if a columnName is null, that's an empty value
            if (columnName == null) {
                continue
            }

            Object value = get(columnName)

            // Check the output rules
            for (OutputFilter outputRule: outputRules) {
                Object checkValue = outputRule.apply(columnName, value)
                if (checkValue != value) {
                    value = checkValue
                    break
                }
            }

            if (value == null) {
                continue
            }

            // We have to fixup lists
            if (value instanceof List) {
                StringBuilder newValueBuilder = new StringBuilder(1024)
                for (int j = 0; j < ((List) value).size(); j++) {
                    newValueBuilder.append((j > 0) ? "\n" : "")
                    newValueBuilder.append(((List) value).get(j))
                }
                value = newValueBuilder.toString()
            }

            // escape the value to include in a CSV
            sb.append(StringEscapeUtils.escapeCsv(value.toString()))
        }
        return sb.toString()
    }
}
