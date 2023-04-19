import org.codehaus.groovy.util.StringUtil

/**
 * This is a dynamic in-memory database.
 * This database will not be high performance, but will make searching for data across fields easy
 *
 * This is not thread safe
 */

class FlexiDatabase {
    private List<Tuple3<String, Class, Boolean>> columnSignature;
    private Map<String, Tuple2<Integer, Boolean>> columnFinder = new HashMap<>()
    private int requiredColumnsCount

    private List<List<Object>> rows;

    // TODO: Optimization - we could optimize searches by tracking the columnFinder lookup values in Maps.

    /**
     * @param columnSignature containing (name, class, and requirement for insert (flag))
     */
    FlexiDatabase(List<Tuple3<String, Class, Boolean>> columnSignature) {
        columnSignature = Collections.unmodifiableList(columnSignature)
        requiredColumnsCount = 0
        // to optimize lookups, store a mapping of String to column
        for (int i = 0; i < columnSignature.size(); i++) {
            columnFinder.put(columnSignature.get(i).getV1(), Tuple2.tuple(i, columnSignature.get(i).getV3()))
            if (columnSignature.get(i).getV3()) {
                ++requiredColumnsCount
            }
        }

        rows = new ArrayList<>();
    }

    Object getValue(List<Tuple2<String, Object>> columnFilters, String desiredField) {
        Integer desiredColumnIndex = findColumn(desiredField)
        if (desiredColumnIndex == null) {
            throw new RuntimeException("Could not find column for desiredField: ${desiredField}")
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
    int incrementField(List<Tuple2<String, Object>> columnFilters, String incrementField) {
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

    List append(List<Tuple2<String, Object>> columnFilters, String textField, text) {
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

    List<Object> findRow(List<Tuple2<String, Object>> columnFilters) {
        // check we provided correct details
        int foundCount = 0

        columnFilters.forEach {columnFilter -> {
            String desiredColumnName = columnFilter.getV1()
            Tuple2<Integer, Boolean> foundColumn = columnFinder.get(desiredColumnName);
            if (foundColumn == null) {
                throw new RuntimeException("Could not find column: ${desiredColumnName}")
            }
            foundCount += (foundColumn.getV2()) ? 1 : 0
        }}
        if (foundCount != requiredColumnsCount) {
            throw new RuntimeException("Provided ${foundCount} of ${requiredColumnsCount}")
        }

        // now search for the row
        List<List<Object>> foundRows = new ArrayList<>()
        for (List<Object> row: rows) {
            columnFilters.forEach {columnFilter -> {
                String desiredColumnName = columnFilter.getV1()
                Object desiredColumnValue = columnFilter.getV2()

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
                throw new RuntimeException("Found too many rows ${foundRows.size()} for columnFilters ${columnFilters}")
        }

    }

    List<Object> findOrCreateRow(List<Tuple2<String, Object>> columnFilters) {
        List<Object> row = findRow(columnFilters)
        if (row == null) {
            // create a row, set it to whatever is asked for, and return it
            row = new ArrayList<>(columnFinder.size())
            columnFilters.forEach { columnFilter ->
                {
                    String desiredColumnName = columnFilter.getV1()
                    Object desiredColumnValue = columnFilter.getV2()

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
        Tuple2<Integer, Boolean> desiredColumnIndex = columnFinder.get(columnName)
        if (desiredColumnIndex == null) {
            throw new RuntimeException("Unknown column: ${columnName}")
        }
        return desiredColumnIndex.getV1()
    }
}
