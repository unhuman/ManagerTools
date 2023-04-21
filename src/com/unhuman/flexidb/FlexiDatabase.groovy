package com.unhuman.flexidb

import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.flexidb.exceptions.ColumnNotFoundException
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

class FlexiDatabase {
    private List<FlexiDBRow> database;
    private Map<String, AbstractFlexiDBInitColumn> columnFinder = new HashMap<>()
    private int indexedColumnCount

    // TODO: Optimization - we could optimize searches by tracking the columnFinder lookup values in Maps.

    /**
     * @param columnSignature
     */
    FlexiDatabase(List<AbstractFlexiDBInitColumn> columnSignature) {
        indexedColumnCount = 0
        // to optimize lookups, store a mapping of String to column
        for (int i = 0; i < columnSignature.size(); i++) {
            AbstractFlexiDBInitColumn columnDefinition = columnSignature.get(i)
            columnFinder.put(columnDefinition.getName(), columnDefinition)

            if (columnDefinition instanceof FlexiDBInitIndexColumn) {
                ++indexedColumnCount
            }
        }

        database = new ArrayList<>();
    }

    /**
     *
     * @param columnFilters
     * @param desiredField
     * @return
     */
    Object getValue(List<FlexiDBQueryColumn> columnFilters, String desiredField) {
        validateColumn(desiredField)

        List<FlexiDBRow> rows = findRows(columnFilters, false)

        // Figure out the default value if needed
        Object defaultValue = (columnFinder.get(desiredField) instanceof FlexiDBInitDataColumn)
                ? ((FlexiDBInitDataColumn) columnFinder.get(desiredField)).getDefaultValue() : null

        FlexiDBRow row = (rows.size() > 0) ? rows.get(0) : null
        return (row != null && row.containsKey(desiredField)) ? row.get(desiredField) : defaultValue
    }

    List<Object> getValues(List<FlexiDBQueryColumn> columnFilters, String desiredField) {
        validateColumn(desiredField)

        List<FlexiDBRow> rows = findRows(columnFilters, true)

        // Figure out the default value if needed
        Object defaultValue = (columnFinder.get(desiredField) instanceof FlexiDBInitDataColumn)
                ? ((FlexiDBInitDataColumn) columnFinder.get(desiredField)).getDefaultValue() : null

        List<Object> data = new ArrayList<>()
        for (FlexiDBRow row: rows) {
            data.add((row.containsKey(desiredField)) ? row.get(desiredField) : defaultValue)
        }
        return data
    }


    /**
     *
     * @param columnFilters
     * @param incrementField
     * @return
     */
    int incrementField(List<FlexiDBQueryColumn> columnFilters, String incrementField) {
        validateColumn(incrementField)

        FlexiDBRow row = findOrCreateRow(columnFilters);
        Object defaultValue = (columnFinder.get(incrementField) instanceof FlexiDBInitDataColumn)
                ? ((FlexiDBInitDataColumn) columnFinder.get(incrementField)).getDefaultValue() : 0
        int newValue = (row.containsKey(incrementField) ? row.get(incrementField) : defaultValue) + 1
        row.put(incrementField, newValue)
        return newValue
    }

    /**
     *
     * @param columnFilters
     * @param appendField
     * @param appendData
     * @return
     */
    List append(List<FlexiDBQueryColumn> columnFilters, String appendField, Object appendData) {
        validateColumn(appendField)

        FlexiDBRow row = findOrCreateRow(columnFilters);

        List data = (!row.containsKey(appendField)) ? new ArrayList<>() : row.get(appendField)
        data.add(appendData)
        row.put(appendField, data)
        return data
    }

    private List<FlexiDBRow> findRows(List<FlexiDBQueryColumn> columnFilters, boolean allowMultipleReturn) {
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
        // TODO: This should be optimized to leverage the indexes
        List<FlexiDBRow> foundRows = new ArrayList<>()
        for (FlexiDBRow row: database) {
            columnFilters.forEach {columnFilter -> {
                String desiredColumnName = columnFilter.getName()
                Object desiredColumnValue = columnFilter.getMatchValue()

                if (row.get(desiredColumnName) != desiredColumnValue) {
                    row = null
                }
            }}
            if (row == null) {
                continue
            }
            foundRows.add(row)
        }

        switch (foundRows.size()) {
            case 0:
                return Collections.emptyList()
            case 1:
                return foundRows
            default:
                if (!allowMultipleReturn) {
                    throw new UnexpectedSituationException("Found too many rows ${foundRows.size()} for columnFilters ${columnFilters}")
                }
                return foundRows
        }
    }

    private FlexiDBRow findOrCreateRow(List<FlexiDBQueryColumn> columnFilters) {
        List<FlexiDBRow> foundRows = findRows(columnFilters, false)
        FlexiDBRow row = (foundRows.size() == 1) ? foundRows.get(0) : null
        if (row == null) {
            // create a row, set it to whatever is asked for, a`nd return it
            row = new FlexiDBRow(columnFinder.size())
            columnFilters.forEach { columnFilter ->
                row.put(columnFilter.getName(), columnFilter.getMatchValue())
            }
            database.add(row)
        }

        return row
    }

    private void validateColumn(String columnName) {
        if (!columnFinder.containsKey(columnName)) {
            throw new ColumnNotFoundException("Unknown column: ${columnName}")
        }
    }
}
