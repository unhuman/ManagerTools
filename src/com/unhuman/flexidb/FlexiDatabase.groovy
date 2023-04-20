package com.unhuman.flexidb

import com.unhuman.flexidb.exceptions.ColumnNotFoundException
import com.unhuman.flexidb.exceptions.InvalidRequestException
import com.unhuman.flexidb.exceptions.UnexpectedSituationException
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn

/**
 * This is a dynamic in-memory database.
 * This database will not be high performance, but will make searching for data across fields easy
 *
 * This is not thread safe
 */

class FlexiDatabase {
    private List<AbstractFlexiDBInitColumn> columnSignature
    private Map<String, AbstractFlexiDBInitColumn> columnFinder = new HashMap<>()
    private int requiredColumnsCount

    private List<List<Object>> rows;

    // TODO: Optimization - we could optimize searches by tracking the columnFinder lookup values in Maps.

    /**
     * @param columnSignature
     */
    FlexiDatabase(List<AbstractFlexiDBInitColumn> columnSignature) {
        columnSignature = Collections.unmodifiableList(columnSignature)
        requiredColumnsCount = 0
        // to optimize lookups, store a mapping of String to column
        for (int i = 0; i < columnSignature.size(); i++) {
            AbstractFlexiDBInitColumn columnDefinition = columnSignature.get(i)
            // We set the column in the definition for lookups
            columnDefinition.setColumn(i)
            columnFinder.put(columnDefinition.getName(), columnDefinition)

            if (columnDefinition instanceof FlexiDBInitIndexColumn) {
                ++requiredColumnsCount
            }
        }

        rows = new ArrayList<>();
    }

    Object getValue(List<FlexiDBQueryColumn> columnFilters, String desiredField) {
        Integer desiredColumnIndex = findColumn(desiredField)
        if (desiredColumnIndex == null) {
            throw new ColumnNotFoundException("Could not find column: ${desiredField}")
        }

        List<Object> row = findRow(columnFilters)

        if (row == null) {
            return null
        }

        return (desiredColumnIndex < row.size()) ? row.get(desiredColumnIndex) : null
    }

    /**
     *
     * @param columnFilters
     * @param incrementField
     * @return
     */
    int incrementField(List<FlexiDBQueryColumn> columnFilters, String incrementField) {
        int desiredColumnIndex = findColumn(incrementField)
        List<Object> row = findOrCreateRow(columnFilters);

        boolean columnExists = (desiredColumnIndex < row.size())

        int newValue = (!columnExists || row.get(desiredColumnIndex) == null)
                ? 1 : row.get(desiredColumnIndex) + 1

        if (columnExists) {
            row.set(desiredColumnIndex, newValue)
        } else {
            row.add(desiredColumnIndex, newValue)
        }

        return newValue
    }

    List append(List<FlexiDBQueryColumn> columnFilters, String textField, text) {
        int desiredColumnIndex = findColumn(textField)
        List<Object> row = findOrCreateRow(columnFilters);

        boolean columnExists = (desiredColumnIndex < row.size())

        List data = (!columnExists || row.get(desiredColumnIndex) == null)
                ? new ArrayList<>() : row.get(desiredColumnIndex)
        data.add(text)

        if (columnExists) {
            row.set(desiredColumnIndex, data)
        } else {
            row.add(desiredColumnIndex, data)
        }

        return data
    }

    List<Object> findRow(List<FlexiDBQueryColumn> columnFilters) {
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
        if (foundCount != columnFilters.size()) {
            throw new InvalidRequestException("Found ${foundCount} of ${columnFilters.size()} requested filters")
        }
        if (foundCount != requiredColumnsCount) {
            throw new InvalidRequestException("Found only ${foundCount} of ${requiredColumnsCount} required filters")
        }

        // now search for the row
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
                return null
            case 1:
                return foundRows.get(0)
            default:
                throw new UnexpectedSituationException("Found too many rows ${foundRows.size()} for columnFilters ${columnFilters}")
        }

    }

    List<Object> findOrCreateRow(List<FlexiDBQueryColumn> columnFilters) {
        List<Object> row = findRow(columnFilters)
        if (row == null) {
            // create a row, set it to whatever is asked for, and return it
            row = new ArrayList<>(columnFinder.size())
            columnFilters.forEach { columnFilter ->
                {
                    String desiredColumnName = columnFilter.getName()
                    Object desiredColumnValue = columnFilter.getMatchValue()

                    Integer desiredColumnIndex = findColumn(desiredColumnName)

                    boolean columnExists = (desiredColumnIndex < row.size())

                    if (columnExists) {
                        row.set(desiredColumnIndex, desiredColumnValue)
                    } else {
                        row.add(desiredColumnIndex, desiredColumnValue)
                    }
                }
            }
            rows.add(row)
        }

        return row
    }

    private Integer findColumn(String columnName) {
        AbstractFlexiDBInitColumn desiredColumn = columnFinder.get(columnName)
        if (desiredColumn == null) {
            throw new ColumnNotFoundException("Unknown column: ${columnName}")
        }
        return desiredColumn.getColumn()
    }
}
