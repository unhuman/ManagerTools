package com.unhuman.flexidb

import com.unhuman.flexidb.data.FlexiDBIndexKey
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.flexidb.exceptions.ColumnNotFoundException
import com.unhuman.flexidb.exceptions.DataNotFoundException
import com.unhuman.flexidb.exceptions.InvalidRequestException
import com.unhuman.flexidb.exceptions.UnexpectedSituationException
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn

/**
 * This is a dynamic in-memory database.
 * This database will not be high performance, but will make searching for data across fields easy
 *
 * There is no type checking
 * This is not thread safe
 */

class FlexiDB {
    private final List<FlexiDBRow> database;
    private final Map<String, AbstractFlexiDBInitColumn> columnFinder = new HashMap<>()
    private final List<String> originalColumnOrder
    private final int indexedColumnCount

    Map<FlexiDBIndexKey, LinkedHashSet<FlexiDBRow>> indexes

    /**
     * @param columnSignature
     */
    FlexiDB(List<AbstractFlexiDBInitColumn> columnSignature) {
        indexedColumnCount = 0
        originalColumnOrder = new ArrayList<>()
        // to optimize lookups, store a mapping of String to column
        for (int i = 0; i < columnSignature.size(); i++) {
            AbstractFlexiDBInitColumn columnDefinition = columnSignature.get(i)
            columnFinder.put(columnDefinition.getName(), columnDefinition)
            originalColumnOrder.add(columnDefinition.getName())

            if (columnDefinition instanceof FlexiDBInitIndexColumn) {
                ++indexedColumnCount
            } else if (columnDefinition instanceof FlexiDBInitDataColumn) {
                // Set defaults for the rows
                FlexiDBInitDataColumn initData = (FlexiDBInitDataColumn) columnDefinition
                FlexiDBRow.setDefault(initData.getName(), initData.getDefaultValue())
            }
        }

        database = new ArrayList<>()
        indexes = new HashMap<>()
        originalColumnOrder = Collections.unmodifiableList(originalColumnOrder)
    }

    /**
     *
     * @param columnFilters
     * @param desiredField
     * @return
     */
    Object getValue(Collection<FlexiDBQueryColumn> columnFilters, String desiredField) {
        validateColumn(desiredField)

        List<FlexiDBRow> rows = findRows(columnFilters, false)

        FlexiDBRow row = (rows.size() > 0) ? rows.get(0) : null

        if (row == null) {
            throw new DataNotFoundException("Row not found for ${columnFilters}")
        }
        return row.get(desiredField)
    }

    List<Object> getValues(Collection<FlexiDBQueryColumn> columnFilters, String desiredField) {
        validateColumn(desiredField)

        List<FlexiDBRow> rows = findRows(columnFilters, true)

        List<Object> data = new ArrayList<>()
        for (FlexiDBRow row: rows) {
            data.add(row.get(desiredField))
        }
        return data
    }

    List<Object> findUniqueValues(String columnName) {
        LinkedHashSet<Object> uniqueValues = new LinkedHashSet<>()
        database.each(row -> {
            if (row.containsKey(columnName) && row.get(columnName) != null && !uniqueValues.contains(row.get(columnName)))  {
                uniqueValues.add(row.get(columnName))
            }
        })
        return uniqueValues.toList()
    }

    /**
     *
     * @param columnFilters
     * @param incrementField
     * @return
     */
    int incrementField(Collection<FlexiDBQueryColumn> columnFilters, String incrementField) {
        return incrementField(columnFilters, incrementField, 1)
    }

    /**
     *
     * @param columnFilters
     * @param incrementField
     * @param increment
     * @return
     */
    int incrementField(Collection<FlexiDBQueryColumn> columnFilters, String incrementField, int increment) {
        validateColumn(incrementField)

        FlexiDBRow row = findOrCreateRow(columnFilters)

        Object defaultValue = ((columnFinder.get(incrementField) instanceof FlexiDBInitDataColumn) &&
                ((FlexiDBInitDataColumn) columnFinder.get(incrementField)).getDefaultValue() != null)
                ? ((FlexiDBInitDataColumn) columnFinder.get(incrementField)).getDefaultValue() : 0

        int newValue = ((row.get(incrementField) != null) ? row.get(incrementField) : defaultValue) + increment
        row.put(incrementField, newValue)
        return newValue
    }

    /**
     *
     * @param columnFilters
     * @param columnName
     * @param value
     */
    void setValue(Collection<FlexiDBQueryColumn> columnFilters, String columnName, Object value) {
        validateColumn(columnName)

        FlexiDBRow row = findOrCreateRow(columnFilters);
        row.setProperty(columnName, value)
    }

    /**
     *
     * @param columnFilters
     * @param appendField
     * @param appendData
     * @return
     */
    List append(Collection<FlexiDBQueryColumn> columnFilters, String appendField, Object appendData) {
        append(columnFilters, appendField, appendData, false)
    }

    /**
     *
     * @param columnFilters
     * @param appendField
     * @param appendData
     * @return
     */
    List append(Collection<FlexiDBQueryColumn> columnFilters, String appendField, Object appendData, boolean addLineNumber) {
        validateColumn(appendField)

        FlexiDBRow row = findOrCreateRow(columnFilters);

        List data = (!row.containsKey(appendField)) ? new ArrayList<>() : row.get(appendField)

        if (addLineNumber) {
            int lineNumber = data.size() + 1
            appendData = "${lineNumber}. ${appendData}"
        }

        data.add(appendData)
        row.put(appendField, data)
        return data
    }

    List<FlexiDBRow> findRows(Collection<FlexiDBQueryColumn> columnFilters, boolean allowMultipleReturn) {
        // check we provided correct details
        int foundCount = 0

        columnFilters.forEach {columnFilter -> {
            String desiredColumnName = columnFilter.getName()
            AbstractFlexiDBInitColumn foundColumn = columnFinder.get(desiredColumnName);

            if (foundColumn == null) {
                throw new ColumnNotFoundException("Could not find column: ${desiredColumnName}")
            }

            if (!(foundColumn instanceof FlexiDBInitIndexColumn)) {
                throw new InvalidRequestException("Requested column is not an index column: ${desiredColumnName}")
            }

            foundCount += (foundColumn instanceof FlexiDBInitIndexColumn) ? 1 : 0
        }}

        if (!allowMultipleReturn && foundCount != indexedColumnCount) {
            throw new InvalidRequestException("Found ${foundCount} of ${indexedColumnCount} required filters")
        }

        // now search for the row
        LinkedHashSet<FlexiDBRow> foundRows = new LinkedHashSet<>()
        for (FlexiDBQueryColumn columnFilter: columnFilters) { // cannot use each since can't break out
            String desiredColumnName = columnFilter.getName()
            Object desiredColumnValue = columnFilter.getMatchValue()
            LinkedHashSet<FlexiDBRow> filterRows = indexes.get(new FlexiDBIndexKey(desiredColumnName, desiredColumnValue))

            filterRows = (filterRows != null) ? filterRows.clone() : Collections.emptySet()

            // either use the data if we had none or determine the intersection
            if (foundRows.isEmpty()) {
                foundRows = filterRows
            } else {
                foundRows.retainAll(filterRows)
            }

            // nothing?  bail out
            if (foundRows.isEmpty()) {
                break
            }
        }

        switch (foundRows.size()) {
            case 0:
                return Collections.emptyList()
            case 1:
                return foundRows.toList()
            default:
                if (!allowMultipleReturn) {
                    throw new UnexpectedSituationException("Found too many rows ${foundRows.size()} for columnFilters ${columnFilters}")
                }
                return foundRows.toList()
        }
    }

    private FlexiDBRow findOrCreateRow(Collection<FlexiDBQueryColumn> columnFilters) {
        List<FlexiDBRow> foundRows = findRows(columnFilters, false)
        FlexiDBRow row = (foundRows.size() == 1) ? foundRows.get(0) : null
        if (row == null) {
            // create a row, set it to whatever is asked for and update indexesa
            row = new FlexiDBRow(columnFinder.size())
            columnFilters.forEach { columnFilter ->
                // put the data into the row
                row.put(columnFilter.getName(), columnFilter.getMatchValue())

                // index the data
                FlexiDBIndexKey indexKey = new FlexiDBIndexKey(columnFilter.getName(), columnFilter.getMatchValue())
                LinkedHashSet<FlexiDBRow> indexedRows = (indexes.containsKey(indexKey)) ? indexes.get(indexKey) : new LinkedHashSet<>()
                indexedRows.add(row)
                indexes.put(indexKey, indexedRows)
            }
            database.add(row)
        }

        return row
    }

    List<String> getOriginalColumnOrder() {
        return originalColumnOrder
    }

    String toCSV() {
        return toCSV(originalColumnOrder)
    }

    String toCSV(List<String> columnOrder) {
        StringBuilder sb = new StringBuilder(4096)

        // Render headings
        sb.append(FlexiDBRow.headingsToCSV(columnOrder))
        sb.append('\n')

        // Render rows
        database.each {row -> {
            sb.append(row.toCSV(columnOrder))
            sb.append('\n')
        }}

        return sb.toString()
    }

    private void validateColumn(String columnName) {
        if (!columnFinder.containsKey(columnName)) {
            throw new ColumnNotFoundException("Unknown column: ${columnName}")
        }
    }
}
