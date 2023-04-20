package com.unhuman.flexidb

import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import groovy.test.GroovyTestCase
import org.junit.Assert

class FlexiDatabaseTests extends GroovyTestCase {
    private final String SIMPLE_INDEX_KEY = "StringIdx"
    private final String SIMPLE_COUNTER_KEY = "Counter"
    private final String SIMPLE_COMMENTS_KEY = "Comments"

    void testFlexiDatabaseSimple() {
        FlexiDBInitIndexColumn indexField = new FlexiDBInitIndexColumn(SIMPLE_INDEX_KEY, String.class)
        FlexiDBInitDataColumn dataField = new FlexiDBInitDataColumn(SIMPLE_COUNTER_KEY, Integer.class)
        FlexiDBInitDataColumn commentsField = new FlexiDBInitDataColumn(SIMPLE_COMMENTS_KEY, List.class)
        List<AbstractFlexiDBInitColumn> definition = List.of(indexField, dataField, commentsField)

        FlexiDatabase simpleFlexiDb = new FlexiDatabase(definition)
        List<FlexiDBQueryColumn> simpleIndexQuery = List.of(new FlexiDBQueryColumn(SIMPLE_INDEX_KEY, "value1"))

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
    }
}
