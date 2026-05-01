import pytest
from managertools.flexidb.flexidb import FlexiDB
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.flexidb.init.flexidb_init_index_column import FlexiDBInitIndexColumn
from managertools.flexidb.init.flexidb_init_data_column import FlexiDBInitDataColumn
from managertools.flexidb.exceptions import (
    ColumnNotFoundException, DataNotFoundException,
    InvalidRequestException, UnexpectedSituationException,
)
from managertools.flexidb.data.flexidb_row import FlexiDBRow


class TestFlexiDB:
    def setup_method(self):
        self.index_key_1 = "StringIdx1"
        self.index_key_2 = "StringIdx2"
        self.simple_counter_key = "Counter"
        self.simple_comments_key = "Comments"
        self.simple_value_key = "Value"
        self.no_data_column_key = "DoNotPutDataInThisColumn"
        self.invalid_column_key = "ColumnDNE"
        self.value_one = "one"
        self.value_two = "two"

    def create_simple_db(self):
        index_field = FlexiDBInitIndexColumn(self.index_key_1)
        value_field = FlexiDBInitDataColumn(self.simple_value_key, None)
        data_field = FlexiDBInitDataColumn(self.simple_counter_key, 0)
        comments_field = FlexiDBInitDataColumn(self.simple_comments_key, [])
        definition = [index_field, value_field, data_field, comments_field]
        return FlexiDB(definition)

    def test_flexi_database_simple(self):
        db = self.create_simple_db()
        query = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key)]

        with pytest.raises(DataNotFoundException):
            db.get_value(query, self.simple_counter_key)

        with pytest.raises(ColumnNotFoundException):
            db.get_value(query, self.invalid_column_key)

        with pytest.raises(DataNotFoundException):
            db.get_value(query, self.simple_value_key)

        db.set_value(query, self.simple_value_key, self.value_one)
        assert db.get_value(query, self.simple_value_key) == self.value_one

        lowercase_key = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key.lower())]
        with pytest.raises(DataNotFoundException):
            db.get_value(lowercase_key, self.simple_value_key)

        uppercase_key = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key.upper())]
        with pytest.raises(DataNotFoundException):
            db.get_value(uppercase_key, self.simple_value_key)

        db.set_value(query, self.simple_value_key, self.value_two)
        assert db.get_value(query, self.simple_value_key) == self.value_two

        assert db.increment_field(query, self.simple_counter_key) == 1
        assert db.get_value(query, self.simple_counter_key) == 1

        assert db.get_value(query, self.simple_comments_key) == []
        assert len(db.append(query, self.simple_comments_key, "Comment1")) == 1
        assert len(db.get_value(query, self.simple_comments_key)) == 1

        assert db.increment_field(query, self.simple_counter_key) == 2
        assert db.get_value(query, self.simple_counter_key) == 2
        assert len(db.get_value(query, self.simple_comments_key)) == 1

        assert len(db.append(query, self.simple_comments_key, "Comment2")) == 2
        assert len(db.get_value(query, self.simple_comments_key)) == 2

        not_found_query = [FlexiDBQueryColumn(self.index_key_1, "valueDNE")]
        with pytest.raises(DataNotFoundException):
            db.get_value(not_found_query, self.simple_counter_key)

        dupe_query = [
            FlexiDBQueryColumn(self.index_key_1, "Value1"),
            FlexiDBQueryColumn(self.index_key_1, "Value1")
        ]
        with pytest.raises(InvalidRequestException):
            db.get_value(dupe_query, self.simple_counter_key)

        complex_query_1 = [
            FlexiDBQueryColumn(self.index_key_1, "Value1"),
            FlexiDBQueryColumn(self.index_key_2, "Value2")
        ]
        with pytest.raises(ColumnNotFoundException):
            db.get_value(complex_query_1, self.simple_counter_key)

        complex_query_2 = [
            FlexiDBQueryColumn(self.index_key_1, "Value1"),
            FlexiDBQueryColumn(self.simple_counter_key, "Value2")
        ]
        with pytest.raises(InvalidRequestException):
            db.get_value(complex_query_2, self.simple_counter_key)

        expected_csv = "StringIdx1,Value,Counter,Comments\nValue,two,2,\"Comment1\nComment2\"\n"
        assert db.to_csv() == expected_csv

        expected_csv_counter = "Counter\n2\n"
        assert db.to_csv([self.simple_counter_key]) == expected_csv_counter

    def test_flexi_database_simple_case_insensitive(self):
        index_field = FlexiDBInitIndexColumn(self.index_key_1)
        value_field = FlexiDBInitDataColumn(self.simple_value_key, None)
        data_field = FlexiDBInitDataColumn(self.simple_counter_key, 0)
        comments_field = FlexiDBInitDataColumn(self.simple_comments_key, [])
        definition = [index_field, value_field, data_field, comments_field]
        db = FlexiDB(definition, True)

        query = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key)]
        db.set_value(query, self.simple_value_key, self.value_one)

        assert db.get_value(query, self.simple_value_key) == self.value_one

        lowercase_key = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key.lower())]
        assert db.get_value(lowercase_key, self.simple_value_key) == self.value_one

        uppercase_key = [FlexiDBQueryColumn(self.index_key_1, self.simple_value_key.upper())]
        assert db.get_value(uppercase_key, self.simple_value_key) == self.value_one

    def test_flexi_database_complex(self):
        index_field1 = FlexiDBInitIndexColumn(self.index_key_1)
        index_field2 = FlexiDBInitIndexColumn(self.index_key_2)
        data_field = FlexiDBInitDataColumn(self.simple_counter_key, 10)
        comments_field = FlexiDBInitDataColumn(self.simple_comments_key, [])
        not_found_field = FlexiDBInitDataColumn(self.no_data_column_key, None)
        definition = [index_field1, index_field2, data_field, comments_field, not_found_field]
        db = FlexiDB(definition)

        simple_query = [FlexiDBQueryColumn(self.index_key_1, "value1")]
        with pytest.raises(InvalidRequestException):
            db.get_value(simple_query, self.simple_counter_key)

        complex_query = [
            FlexiDBQueryColumn(self.index_key_1, "value1"),
            FlexiDBQueryColumn(self.index_key_2, "value2")
        ]

        assert db.increment_field(complex_query, self.simple_counter_key) == 11
        assert db.get_value(complex_query, self.simple_counter_key) == 11

        with pytest.raises(InvalidRequestException):
            db.get_value(simple_query, self.simple_comments_key)

        with pytest.raises(InvalidRequestException):
            db.append(simple_query, self.simple_comments_key, "Comment1")

        assert db.increment_field(complex_query, self.simple_counter_key) == 12
        assert db.get_value(complex_query, self.simple_counter_key) == 12

        assert db.get_value(complex_query, self.no_data_column_key) is None

        complex_query_2 = [
            FlexiDBQueryColumn(self.index_key_1, "value1"),
            FlexiDBQueryColumn(self.index_key_2, "value3")
        ]
        assert db.increment_field(complex_query_2, self.simple_counter_key) == 11

        counters = db.get_values(simple_query, self.simple_counter_key)
        assert len(counters) == 2

        expected_csv = ("StringIdx1,StringIdx2,Counter,Comments,DoNotPutDataInThisColumn\n"
                       "value1,value2,12,,\n"
                       "value1,value3,11,,\n")
        assert db.to_csv() == expected_csv
