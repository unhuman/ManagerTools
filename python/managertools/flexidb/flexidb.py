import threading
from typing import Any, Collection, List, Optional

from managertools.flexidb.flexidb_index_key import FlexiDBIndexKey
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.flexidb.data.flexidb_row import FlexiDBRow
from managertools.flexidb.exceptions import (
    ColumnNotFoundException, DataNotFoundException, InvalidRequestException, UnexpectedSituationException
)
from managertools.flexidb.init.abstract_flexidb_init_column import AbstractFlexiDBInitColumn
from managertools.flexidb.init.flexidb_init_data_column import FlexiDBInitDataColumn
from managertools.flexidb.init.flexidb_init_index_column import FlexiDBInitIndexColumn


class FlexiDB:
    EMPTY_INCREMENTOR = " "

    def __init__(self, column_signature: List[AbstractFlexiDBInitColumn], case_insensitive_index: bool = False):
        self._lock = threading.Lock()

        self.case_insensitive_index = case_insensitive_index
        self.indexed_column_count = 0
        self.column_finder = {}
        self.original_column_order = []
        self.database = []
        self.indexes = {}

        for column_definition in column_signature:
            self.column_finder[column_definition.get_name()] = column_definition
            self.original_column_order.append(column_definition.get_name())

            if isinstance(column_definition, FlexiDBInitIndexColumn):
                self.indexed_column_count += 1
            elif isinstance(column_definition, FlexiDBInitDataColumn):
                FlexiDBRow.set_default(column_definition.get_name(), column_definition.get_default_value())

        self.original_column_order = tuple(self.original_column_order)

    def get_value(self, column_filters: Collection[FlexiDBQueryColumn], desired_field: str) -> Any:
        with self._lock:
            self._validate_column(desired_field)
            rows = self._find_rows(column_filters, allow_multiple=False)

            if not rows:
                raise DataNotFoundException(f"Row not found for {column_filters}")

            return rows[0].get(desired_field)

    def get_values(self, column_filters: Collection[FlexiDBQueryColumn], desired_field: str) -> List[Any]:
        with self._lock:
            self._validate_column(desired_field)
            rows = self._find_rows(column_filters, allow_multiple=True)

            return [row.get(desired_field) for row in rows]

    def find_unique_values(self, column_name: str) -> List[Any]:
        with self._lock:
            unique_values = {}
            for row in self.database:
                if column_name in row and row[column_name] is not None:
                    value = row[column_name]
                    if value not in unique_values:
                        unique_values[value] = True

            return list(unique_values.keys())

    def find_rows(self, column_filters: Collection[FlexiDBQueryColumn], allow_multiple: bool) -> List[FlexiDBRow]:
        with self._lock:
            return self._find_rows(column_filters, allow_multiple)

    def increment_field(self, column_filters: Collection[FlexiDBQueryColumn], increment_field: str, increment: int = 1) -> int:
        with self._lock:
            self._validate_column(increment_field)

            row = self._find_or_create_row(column_filters)

            column_def = self.column_finder.get(increment_field)
            if isinstance(column_def, FlexiDBInitDataColumn):
                default_val = column_def.get_default_value()
                if default_val is not None and default_val != self.EMPTY_INCREMENTOR:
                    default_value = default_val
                else:
                    default_value = 0
            else:
                default_value = 0

            current = row.get(increment_field)
            if current is None or current == self.EMPTY_INCREMENTOR:
                current = default_value

            new_value = current + increment
            row[increment_field] = new_value
            return new_value

    def set_value(self, column_filters: Collection[FlexiDBQueryColumn], column_name: str, value: Any) -> None:
        with self._lock:
            self._validate_column(column_name)
            row = self._find_or_create_row(column_filters)
            row[column_name] = value

    def append(self, column_filters: Collection[FlexiDBQueryColumn], append_field: str, append_data: Any, add_line_number: bool = False) -> List[Any]:
        with self._lock:
            self._validate_column(append_field)

            row = self._find_or_create_row(column_filters)

            data = row.get(append_field) if append_field in row else []
            if not isinstance(data, list):
                data = []

            if add_line_number:
                line_number = len(data) + 1
                append_data = f"{line_number}. {append_data}"

            data.append(append_data)
            row[append_field] = data
            return data

    def get_original_column_order(self) -> List[str]:
        with self._lock:
            return list(self.original_column_order)

    def to_csv(self, column_order: List[str] = None) -> str:
        with self._lock:
            if column_order is None:
                column_order = self.original_column_order

            lines = []

            lines.append(FlexiDBRow.headings_to_csv(column_order))

            for row in self.database:
                lines.append(row.to_csv(column_order))

            return '\n'.join(lines)

    def _find_rows(self, column_filters: Collection[FlexiDBQueryColumn], allow_multiple: bool) -> List[FlexiDBRow]:
        found_count = 0
        for column_filter in column_filters:
            desired_column_name = column_filter.get_name()
            found_column = self.column_finder.get(desired_column_name)

            if found_column is None:
                raise ColumnNotFoundException(f"Could not find column: {desired_column_name}")

            if not isinstance(found_column, FlexiDBInitIndexColumn):
                raise InvalidRequestException(f"Requested column is not an index column: {desired_column_name}")

            found_count += 1

        if not allow_multiple and found_count != self.indexed_column_count:
            raise InvalidRequestException(f"Found {found_count} of {self.indexed_column_count} required filters")

        found_rows = None
        for column_filter in column_filters:
            desired_column_name = column_filter.get_name()
            desired_column_value = column_filter.get_match_value()

            index_key = self._create_flexidb_index_key(desired_column_name, desired_column_value)
            filter_rows = self.indexes.get(index_key, [])

            if found_rows is None:
                found_rows = list(filter_rows)
            else:
                filter_set = set(filter_rows)
                found_rows = [r for r in found_rows if r in filter_set]

            if not found_rows:
                break

        if found_rows is None:
            found_rows = []

        if len(found_rows) == 0:
            return []
        elif len(found_rows) == 1:
            return list(found_rows)
        else:
            if not allow_multiple:
                raise UnexpectedSituationException(f"Found too many rows {len(found_rows)} for columnFilters {column_filters}")
            return list(found_rows)

    def _find_or_create_row(self, column_filters: Collection[FlexiDBQueryColumn]) -> FlexiDBRow:
        found_rows = self._find_rows(column_filters, allow_multiple=False)

        if found_rows:
            return found_rows[0]

        row = FlexiDBRow()
        for column_filter in column_filters:
            column_name = column_filter.get_name()
            match_value = column_filter.get_match_value()

            row[column_name] = match_value

            index_key = self._create_flexidb_index_key(column_name, match_value)

            if index_key not in self.indexes:
                self.indexes[index_key] = []

            self.indexes[index_key].append(row)

        self.database.append(row)

        return row

    def _create_flexidb_index_key(self, column_name: str, value: Any) -> FlexiDBIndexKey:
        if self.case_insensitive_index and isinstance(value, str):
            value = value.lower()

        return FlexiDBIndexKey(column_name, str(value))

    def _validate_column(self, column_name: str) -> None:
        if column_name not in self.column_finder:
            raise ColumnNotFoundException(f"Unknown column: {column_name}")
