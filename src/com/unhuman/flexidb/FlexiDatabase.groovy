package com.unhuman.flexidb

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
    // TODO : maybe not needed
    private List<AbstractFlexiDBInitColumn> columnSignature
    private Map<String, AbstractFlexiDBInitColumn> columnFinder = new HashMap<>()
    private int indexedColumnCount

    private List<List<Object>> rows;

    // TODO: Optimization - we could optimize searches by tracking the columnFinder lookup values in Maps.

    /**
     * @param columnSignature
     */
    FlexiDatabase(List<AbstractFlexiDBInitColumn> columnSignature) {
        columnSignature = Collections.unmodifiableList(columnSignature)
        indexedColumnCount = 0
        // to optimize lookups, store a mapping of String to column
        for (int i = 0; i < columnSignature.size(); i++) {
            AbstractFlexiDBInitColumn columnDefinition = columnSignature.get(i)
            // We set the column in the definition for lookups
            columnDefinition.setColumn(i)
            columnFinder.put(columnDefinition.getName(), columnDefinition)

            if (columnDefinition instanceof FlexiDBInitIndexColumn) {
                ++indexedColumnCount
            }
        }

        rows = new ArrayList<>();
    }

    /**
     *
     * @param columnFilters
     * @param desiredField
     * @return
     */
    Object getValue(List<FlexiDBQueryColumn> columnFilters, String desiredField) {
        List<List<Object>> rows = findRows(columnFilters, false)

        Integer desiredColumnIndex = findColumn(desiredField)

        // Figure out the default value if needed
        Object defaultValue = (columnFinder.get(desiredField) instanceof FlexiDBInitDataColumn)
                ? ((FlexiDBInitDataColumn) columnFinder.get(desiredField)).getDefaultValue() : null

        List<Object> row = (rows.size() > 0) ? rows.get(0) : null
        return (row != null && desiredColumnIndex < row.size()) ? row.get(desiredColumnIndex) : defaultValue
    }

    List<Object> getValues(List<FlexiDBQueryColumn> columnFilters, String desiredField) {
        List<List<Object>> rows = findRows(columnFilters, true)

        Integer desiredColumnIndex = findColumn(desiredField)

        // Figure out the default value if needed
        Object defaultValue = (columnFinder.get(desiredField) instanceof FlexiDBInitDataColumn)
                ? ((FlexiDBInitDataColumn) columnFinder.get(desiredField)).getDefaultValue() : null

        List<Object> data = new ArrayList<>()
        for (List<Object> row: rows) {
            data.add((desiredColumnIndex < row.size()) ? row.get(desiredColumnIndex) : defaultValue)
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
        List<Object> row = findOrCreateRow(columnFilters);
        return updateRow(row, incrementField,
                (int columnIndex, boolean columnExists) -> {
                    Object defaultValue = (columnFinder.get(incrementField) instanceof FlexiDBInitDataColumn)
                            ? ((FlexiDBInitDataColumn) columnFinder.get(incrementField)).getDefaultValue() : 0
                    return ((!columnExists || row.get(columnIndex) == null)
                            ? defaultValue : row.get(columnIndex)) + 1
                }
        )
    }

    /**
     *
     * @param columnFilters
     * @param textField
     * @param appendData
     * @return
     */
    List append(List<FlexiDBQueryColumn> columnFilters, String textField, Object appendData) {
        List<Object> row = findOrCreateRow(columnFilters);

        return updateRow(row, textField,
                (int columnIndex, boolean columnExists) -> {
                    List data = (!columnExists || row.get(columnIndex) == null)
                            ? new ArrayList<>() : row.get(columnIndex)
                    data.add(appendData)
                    return data
                }
        )
    }

    private List<List<Object>> findRows(List<FlexiDBQueryColumn> columnFilters, boolean allowMultipleReturn) {
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
        List<List<Object>> foundRows = new ArrayList<>()
        for (List<Object> row: rows) {
            columnFilters.forEach {columnFilter -> {
                String desiredColumnName = columnFilter.getName()
                Object desiredColumnValue = columnFilter.getMatchValue()

                Integer desiredColumnIndex = findColumn(desiredColumnName)
                if (row.get(desiredColumnIndex) != desiredColumnValue) {
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

    private List<Object> findOrCreateRow(List<FlexiDBQueryColumn> columnFilters) {
        List<List<Object>> foundRows = findRows(columnFilters, false)
        List<Object> row = (foundRows.size() == 1) ? foundRows.get(0) : null
        if (row == null) {
            // create a row, set it to whatever is asked for, and return it
            row = new ArrayList<>(columnFinder.size())
            columnFilters.forEach { columnFilter ->
                updateRow(row, columnFilter.getName(),
                        (int columnIndex, boolean columnExists) -> columnFilter.getMatchValue())
            }
            rows.add(row)
        }

        return row
    }

    /**
     * Updates a row
     * @param row
     * @param desiredColumnName
     * @param mutation used to alter data
     * @return
     */
    private Object updateRow(List<Object> row, String desiredColumnName, Closure mutation) {
        Integer desiredColumnIndex = findColumn(desiredColumnName)
        boolean columnExists = (desiredColumnIndex < row.size())

        // callback to the mutation to do any data adjustments
        Object desiredColumnValue = mutation(desiredColumnIndex, columnExists)

        if (columnExists) {
            row.set(desiredColumnIndex, desiredColumnValue)
        } else {
            row.add(desiredColumnIndex, desiredColumnValue)
        }

        return desiredColumnValue
    }

    private Integer findColumn(String columnName) {
        AbstractFlexiDBInitColumn desiredColumn = columnFinder.get(columnName)
        if (desiredColumn == null) {
            throw new ColumnNotFoundException("Unknown column: ${columnName}")
        }
        return desiredColumn.getColumn()
    }
}
