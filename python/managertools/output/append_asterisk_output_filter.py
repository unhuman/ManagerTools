from managertools.flexidb.output.output_filter import OutputFilter


class AppendAsteriskOutputFilter(OutputFilter):
    """Append a trailing '*' to specific columns' values to flag that they were adjusted at
    report time (e.g. a PR line count capped at the authored-commit total).

    Instantiated per row with the set of columns that were actually changed. Only non-zero
    numeric values are flagged; zero/None/strings are returned unchanged so the standard
    zero->empty rendering still applies. Note FlexiDBRow.to_csv applies the first rule that
    changes a cell and then stops, so this filter must be placed BEFORE the standard rules.
    """

    def __init__(self, columns):
        self._columns = set(columns)

    def apply(self, column_name, value):
        if column_name in self._columns and isinstance(value, (int, float)) and value != 0:
            return f"{value}*"
        return value
