import threading
from collections import OrderedDict
from typing import List, Any, Collection

from .data.flexi_db_index_key import FlexiDBIndexKey
from .data.flexi_db_row import FlexiDBRow
from .exceptions import (
    ColumnNotFoundException, DataNotFoundException,
    InvalidRequestException, UnexpectedSituationException,
)
from .flexi_db_query_column import FlexiDBQueryColumn
from .init.abstract_flexi_db_init_column import AbstractFlexiDBInitColumn
from .init.flexi_db_init_data_column import FlexiDBInitDataColumn
from .init.flexi_db_init_index_column import FlexiDBInitIndexColumn


class FlexiDB:
    EMPTY_INCREMENTOR = " "

    def __init__(self, column_signature: List[AbstractFlexiDBInitColumn], case_insensitive_index: bool = False):
        self._lock = threading.Lock()
        self._case_insensitive_index = case_insensitive_index
        self._database: List[FlexiDBRow] = []
        self._indexes: dict[FlexiDBIndexKey, set[int]] = {}
        self._column_finder: dict[str, AbstractFlexiDBInitColumn] = {}
        self._original_column_order: List[str] = []
        self._indexed_column_count = 0

        for col in column_signature:
            self._column_finder[col.get_name()] = col
            self._original_column_order.append(col.get_name())
            if isinstance(col, FlexiDBInitIndexColumn):
                self._indexed_column_count += 1
            elif isinstance(col, FlexiDBInitDataColumn):
                FlexiDBRow.set_default(col.get_name(), col.get_default_value())

        self._original_column_order = tuple(self._original_column_order)

    def get_value(self, column_filters: Collection[FlexiDBQueryColumn], desired_field: str) -> Any:
        with self._lock:
            self._validate_column(desired_field)
            rows = self._find_rows(column_filters, False)
            if not rows:
                raise DataNotFoundException(f"Row not found for {list(column_filters)}")
            return rows[0].get(desired_field)

    def get_values(self, column_filters: Collection[FlexiDBQueryColumn], desired_field: str) -> List[Any]:
        with self._lock:
            self._validate_column(desired_field)
            rows = self._find_rows(column_filters, True)
            return [row.get(desired_field) for row in rows]

    def find_unique_values(self, column_name: str) -> List[Any]:
        with self._lock:
            seen = []
            seen_set = set()
            for row in self._database:
                val = row.get(column_name)
                if val is not None and val not in seen_set:
                    seen_set.add(val)
                    seen.append(val)
            return seen

    def increment_field(self, column_filters: Collection[FlexiDBQueryColumn], increment_field: str, increment: int = 1) -> int:
        with self._lock:
            self._validate_column(increment_field)
            row = self._find_or_create_row(column_filters)

            col_def = self._column_finder.get(increment_field)
            default_value = 0
            if isinstance(col_def, FlexiDBInitDataColumn):
                dv = col_def.get_default_value()
                if dv is not None and dv != self.EMPTY_INCREMENTOR:
                    default_value = dv

            current = row.get(increment_field)
            if current is None or current == self.EMPTY_INCREMENTOR:
                current = default_value
            new_value = current + increment
            row[increment_field] = new_value
            return new_value

    def set_value(self, column_filters: Collection[FlexiDBQueryColumn], column_name: str, value: Any):
        with self._lock:
            self._validate_column(column_name)
            row = self._find_or_create_row(column_filters)
            row[column_name] = value

    def append(self, column_filters: Collection[FlexiDBQueryColumn], append_field: str,
               append_data: Any, add_line_number: bool = False) -> list:
        with self._lock:
            self._validate_column(append_field)
            row = self._find_or_create_row(column_filters)

            data = row.get(append_field)
            if data is None or append_field not in row:
                data = []

            if add_line_number:
                line_number = len(data) + 1
                append_data = f"{line_number}. {append_data}"

            data = list(data)
            data.append(append_data)
            row[append_field] = data
            return data

    def find_rows(self, column_filters: Collection[FlexiDBQueryColumn], allow_multiple_return: bool) -> List[FlexiDBRow]:
        with self._lock:
            return self._find_rows(column_filters, allow_multiple_return)

    def _find_rows(self, column_filters: Collection[FlexiDBQueryColumn], allow_multiple_return: bool) -> List[FlexiDBRow]:
        found_count = 0
        for cf in column_filters:
            col_name = cf.get_name()
            found_col = self._column_finder.get(col_name)
            if found_col is None:
                raise ColumnNotFoundException(f"Could not find column: {col_name}")
            if not isinstance(found_col, FlexiDBInitIndexColumn):
                raise InvalidRequestException(f"Requested column is not an index column: {col_name}")
            found_count += 1

        if not allow_multiple_return and found_count != self._indexed_column_count:
            raise InvalidRequestException(f"Found {found_count} of {self._indexed_column_count} required filters")

        found_row_ids: set = set()
        first = True
        for cf in column_filters:
            key = self._create_index_key(cf.get_name(), cf.get_match_value())
            filter_row_ids = set(self._indexes.get(key, set()))
            if first:
                found_row_ids = filter_row_ids
                first = False
            else:
                found_row_ids &= filter_row_ids
            if not found_row_ids:
                break

        count = len(found_row_ids)
        if count == 0:
            return []
        if count == 1:
            return [self._database[list(found_row_ids)[0]]]
        if not allow_multiple_return:
            raise UnexpectedSituationException(f"Found too many rows {count} for columnFilters {list(column_filters)}")
        # preserve insertion order from database list
        return [self._database[row_id] for row_id in sorted(found_row_ids)]

    def _find_or_create_row(self, column_filters: Collection[FlexiDBQueryColumn]) -> FlexiDBRow:
        rows = self._find_rows(column_filters, False)
        if rows:
            return rows[0]

        row = FlexiDBRow(len(self._column_finder))
        row_id = len(self._database)
        for cf in column_filters:
            row[cf.get_name()] = cf.get_match_value()
            key = self._create_index_key(cf.get_name(), cf.get_match_value())
            if key not in self._indexes:
                self._indexes[key] = set()
            self._indexes[key].add(row_id)

        self._database.append(row)
        return row

    def _create_index_key(self, column_name: str, value) -> FlexiDBIndexKey:
        if self._case_insensitive_index and isinstance(value, str):
            value = value.lower()
        return FlexiDBIndexKey(column_name, str(value))

    def get_original_column_order(self) -> tuple:
        return self._original_column_order

    def to_csv(self, column_order: List[str] = None) -> str:
        with self._lock:
            if column_order is None:
                column_order = list(self._original_column_order)
            lines = [FlexiDBRow.headings_to_csv(column_order)]
            for row in self._database:
                lines.append(row.to_csv(column_order))
            return "\n".join(lines) + "\n"

    def _validate_column(self, column_name: str):
        if column_name not in self._column_finder:
            raise ColumnNotFoundException(f"Unknown column: {column_name}")
