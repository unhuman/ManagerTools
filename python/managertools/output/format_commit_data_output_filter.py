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
        entry_type = entry.get("type")
        if entry_type == "skipped":
            # Collection-time skip marker (e.g. down-merge PR); message already self-describing.
            return f"[skipped] {message}"
        prefix = ""
        if entry_type == "merge":
            prefix = "[merge] "
        elif entry_type == "brought-in":
            prefix = "[brought-in] "  # merged in from another branch; not counted by default
        return f"{prefix}{message} (+{additions}/-{deletions})"
