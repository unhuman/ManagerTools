from managertools.data.db_data import DBData
from managertools.flexidb.output.output_filter import OutputFilter


class FormatCommitDataOutputFilter(OutputFilter):
    """Render the COMMIT_DATA column (list of per-commit dicts) as readable lines.

    Each entry {"message", "additions", "deletions"} becomes "message (+additions/-deletions)".
    Returns a list of strings so FlexiDBRow.to_csv joins them with newlines like any other list.
    """

    def apply(self, column_name, value):
        if column_name != DBData.COMMIT_DATA.name or not isinstance(value, list):
            return value
        return [self._format_entry(entry) for entry in value]

    @staticmethod
    def _format_entry(entry):
        if not isinstance(entry, dict):
            return str(entry)
        message = entry.get("message", "")
        additions = entry.get("additions", 0) or 0
        deletions = entry.get("deletions", 0) or 0
        prefix = "[merge] " if entry.get("type") == "merge" else ""
        return f"{prefix}{message} (+{additions}/-{deletions})"
