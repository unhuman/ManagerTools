package com.unhuman.flexidb

import com.unhuman.flexidb.data.FlexiDBIndexKey
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

    Map<FlexiDBIndexKey, LinkedHashSet<FlexiDBRow>> indexes

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

        database = new ArrayList<>()
        indexes = new HashMap<>()
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
        LinkedHashSet<FlexiDBRow> foundRows = new LinkedHashSet<>()
        columnFilters.forEach {columnFilter -> {
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
                return
            }
        }}

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

    private FlexiDBRow findOrCreateRow(List<FlexiDBQueryColumn> columnFilters) {
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

    private void validateColumn(String columnName) {
        if (!columnFinder.containsKey(columnName)) {
            throw new ColumnNotFoundException("Unknown column: ${columnName}")
        }
    }
}
