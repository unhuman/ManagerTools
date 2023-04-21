package com.unhuman.flexidb

import com.unhuman.flexidb.exceptions.ColumnNotFoundException
import com.unhuman.flexidb.exceptions.InvalidRequestException
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import groovy.test.GroovyTestCase
import org.junit.Assert

class FlexiDatabaseTests extends GroovyTestCase {
    private final String INDEX_KEY_1 = "StringIdx1"
    private final String INDEX_KEY_2 = "StringIdx2"

    private final String SIMPLE_COUNTER_KEY = "Counter"
    private final String SIMPLE_COMMENTS_KEY = "Comments"

    private final String NO_DATA_COLUMN_KEY = "DoNotPutDataInThisColumn"
    private final String INVALID_COLUMN_KEY = "ColumnDNE"

    void testFlexiDatabaseSimple() {
        FlexiDBInitIndexColumn indexField = new FlexiDBInitIndexColumn(INDEX_KEY_1)
        FlexiDBInitDataColumn dataField = new FlexiDBInitDataColumn(SIMPLE_COUNTER_KEY, 0)
        FlexiDBInitDataColumn commentsField = new FlexiDBInitDataColumn(SIMPLE_COMMENTS_KEY, Collections.emptyList())
        List<AbstractFlexiDBInitColumn> definition = List.of(indexField, dataField, commentsField)

        FlexiDatabase simpleFlexiDb = new FlexiDatabase(definition)
        List<FlexiDBQueryColumn> simpleIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "value1"))

        // Ensure we get the default back
        Assert.assertEquals(0, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        // Simple test for requested column not existing
        Assert.assertThrows(ColumnNotFoundException.class,
                () -> simpleFlexiDb.getValue(simpleIndexQuery, INVALID_COLUMN_KEY))

        // Track values
        Assert.assertEquals(1, simpleFlexiDb.incrementField(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        Assert.assertEquals(Collections.emptyList(), simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY))
        Assert.assertEquals(1, simpleFlexiDb.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment1").size())
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        Assert.assertEquals(2, simpleFlexiDb.incrementField(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(2, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        Assert.assertEquals(2, simpleFlexiDb.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment2").size())
        Assert.assertEquals(2, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        // Test not-found index scenario
        List<FlexiDBQueryColumn> notFoundIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "valueDNE"))
        Assert.assertEquals(0, simpleFlexiDb.getValue(notFoundIndexQuery, SIMPLE_COUNTER_KEY))

        // querying 2 of the same thing won't match
        List<FlexiDBQueryColumn> dupeIndexQuery =
                List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                        new FlexiDBQueryColumn(INDEX_KEY_1, "value1"))
        Assert.assertThrows(InvalidRequestException.class,
                () -> simpleFlexiDb.getValue(dupeIndexQuery, SIMPLE_COUNTER_KEY))

        // Do a test of non-index field
        List<FlexiDBQueryColumn> complexIndexQuery1 = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "value2"))

        Assert.assertThrows(com.unhuman.flexidb.exceptions.ColumnNotFoundException.class,
                () -> simpleFlexiDb.getValue(complexIndexQuery1, SIMPLE_COUNTER_KEY))

        // Do a test of too many filters provided
        List<FlexiDBQueryColumn> complexIndexQuery2 = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(SIMPLE_COUNTER_KEY, "value2"))

        Assert.assertThrows(com.unhuman.flexidb.exceptions.InvalidRequestException.class,
                () -> simpleFlexiDb.getValue(complexIndexQuery2, SIMPLE_COUNTER_KEY))
    }

    void testFlexiDatabaseComplex() {
        FlexiDBInitIndexColumn indexField1 = new FlexiDBInitIndexColumn(INDEX_KEY_1)
        FlexiDBInitIndexColumn indexField2 = new FlexiDBInitIndexColumn(INDEX_KEY_2)
        FlexiDBInitDataColumn dataField = new FlexiDBInitDataColumn(SIMPLE_COUNTER_KEY, 10)
        FlexiDBInitDataColumn commentsField = new FlexiDBInitDataColumn(SIMPLE_COMMENTS_KEY, Collections.emptyList())
        FlexiDBInitDataColumn notFoundField = new FlexiDBInitDataColumn(NO_DATA_COLUMN_KEY, null)

        List<AbstractFlexiDBInitColumn> definition = List.of(indexField1, indexField2, dataField, commentsField, notFoundField)

        FlexiDatabase complexFlexiDB = new FlexiDatabase(definition)
        List<FlexiDBQueryColumn> simpleIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "value1"))

        // Ensure we get nothing back for the first request
        Assert.assertThrows(InvalidRequestException.class,
                () -> complexFlexiDB.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        List<FlexiDBQueryColumn> complexIndexQuery = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "value2"))

        // Track values
        Assert.assertEquals(11, complexFlexiDB.incrementField(complexIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(11, complexFlexiDB.getValue(complexIndexQuery, SIMPLE_COUNTER_KEY))

        Assert.assertThrows(com.unhuman.flexidb.exceptions.InvalidRequestException.class,
                () -> complexFlexiDB.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY))
        Assert.assertThrows(com.unhuman.flexidb.exceptions.InvalidRequestException.class,
                () -> complexFlexiDB.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment1").size())

        Assert.assertEquals(12, complexFlexiDB.incrementField(complexIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(12, complexFlexiDB.getValue(complexIndexQuery, SIMPLE_COUNTER_KEY))

        List<FlexiDBQueryColumn> complexNotFoundIndexQuery1 = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "valueDNE"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "value2"))
        Assert.assertNull(complexFlexiDB.getValue(complexIndexQuery, NO_DATA_COLUMN_KEY))

        List<FlexiDBQueryColumn> complexNotFoundIndexQuery2 = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "valueDNE"))
        Assert.assertNull(complexFlexiDB.getValue(complexIndexQuery, NO_DATA_COLUMN_KEY))

        // add a second element to the database
        List<FlexiDBQueryColumn> complexIndexQuery2 = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "value3"))
        Assert.assertEquals(11, complexFlexiDB.incrementField(complexIndexQuery2, SIMPLE_COUNTER_KEY))

        // Retrieve all the rows for INDEX_KEY_1 (should be 2)
        List<Object> counters = complexFlexiDB.getValues(simpleIndexQuery, SIMPLE_COUNTER_KEY)
        Assert.assertEquals(2, counters.size())
    }
}
