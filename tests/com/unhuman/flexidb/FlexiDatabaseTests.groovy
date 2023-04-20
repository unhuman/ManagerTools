package com.unhuman.flexidb

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

    void testFlexiDatabaseSimple() {
        FlexiDBInitIndexColumn indexField = new FlexiDBInitIndexColumn(INDEX_KEY_1)
        FlexiDBInitDataColumn dataField = new FlexiDBInitDataColumn(SIMPLE_COUNTER_KEY)
        FlexiDBInitDataColumn commentsField = new FlexiDBInitDataColumn(SIMPLE_COMMENTS_KEY)
        List<AbstractFlexiDBInitColumn> definition = List.of(indexField, dataField, commentsField)

        FlexiDatabase simpleFlexiDb = new FlexiDatabase(definition)
        List<FlexiDBQueryColumn> simpleIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "value1"))

        // Ensure we get nothing back for the first request
        Assert.assertNull(simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        // Track values

        Assert.assertEquals(1, simpleFlexiDb.incrementField(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        Assert.assertNull(simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY))
        Assert.assertEquals(1, simpleFlexiDb.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment1").size())
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        Assert.assertEquals(2, simpleFlexiDb.incrementField(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(2, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(1, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        Assert.assertEquals(2, simpleFlexiDb.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment2").size())
        Assert.assertEquals(2, simpleFlexiDb.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY).size())

        // Test not-found index scenario
        List<FlexiDBQueryColumn> notFoundIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "valueDNE"))
        Assert.assertNull(simpleFlexiDb.getValue(notFoundIndexQuery, SIMPLE_COUNTER_KEY))

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
        FlexiDBInitDataColumn dataField = new FlexiDBInitDataColumn(SIMPLE_COUNTER_KEY)
        List<AbstractFlexiDBInitColumn> definition = List.of(indexField1, indexField2, dataField)

        FlexiDatabase complexFlexiDB = new FlexiDatabase(definition)
        List<FlexiDBQueryColumn> simpleIndexQuery = List.of(new FlexiDBQueryColumn(INDEX_KEY_1, "value1"))

        // Ensure we get nothing back for the first request
        Assert.assertThrows(InvalidRequestException.class,
                () -> complexFlexiDB.getValue(simpleIndexQuery, SIMPLE_COUNTER_KEY))

        List<FlexiDBQueryColumn> complexIndexQuery = List.of(
                new FlexiDBQueryColumn(INDEX_KEY_1, "value1"),
                new FlexiDBQueryColumn(INDEX_KEY_2, "value2"))

        // Track values
        Assert.assertEquals(1, complexFlexiDB.incrementField(complexIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(1, complexFlexiDB.getValue(complexIndexQuery, SIMPLE_COUNTER_KEY))

        Assert.assertThrows(com.unhuman.flexidb.exceptions.ColumnNotFoundException.class,
                () -> complexFlexiDB.getValue(simpleIndexQuery, SIMPLE_COMMENTS_KEY))
        Assert.assertThrows(com.unhuman.flexidb.exceptions.ColumnNotFoundException.class,
                () -> complexFlexiDB.append(simpleIndexQuery, SIMPLE_COMMENTS_KEY, "Comment1").size())

        Assert.assertEquals(2, complexFlexiDB.incrementField(complexIndexQuery, SIMPLE_COUNTER_KEY))
        Assert.assertEquals(2, complexFlexiDB.getValue(complexIndexQuery, SIMPLE_COUNTER_KEY))
    }
}
