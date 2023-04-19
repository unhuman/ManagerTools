import groovy.test.GroovyTestCase
import org.junit.Assert

class FlexiDatabaseTests extends GroovyTestCase {
    private final String SIMPLE_INDEX_KEY = "StringIdx"
    private final String SIMPLE_COUNTER_KEY = "Counter"
    private final String SIMPLE_COMMENTS_KEY = "Comments"

    void testFlexiDatabaseSimple() {
        Tuple3<String, Class, Boolean> indexField = new Tuple3<>(SIMPLE_INDEX_KEY, String.class, true)
        Tuple3<String, Class, Boolean> dataField = new Tuple3<>(SIMPLE_COUNTER_KEY, Integer.class, false)
        Tuple3<String, Class, Boolean> commentsField = new Tuple3<>(SIMPLE_COMMENTS_KEY, List.class, false)
        List<Tuple3<String, Class, Boolean>> definition = List.of(indexField, dataField, commentsField)

        FlexiDatabase simpleFlexiDb = new FlexiDatabase(definition)
        List<Tuple2<String, Object>> simpleIndexQuery = List.of(new Tuple2(SIMPLE_INDEX_KEY, "value1"))

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
