from managertools.output.append_asterisk_output_filter import AppendAsteriskOutputFilter
from managertools.sprint_report_visualizer import safe_int


class TestAppendAsteriskOutputFilter:
    def test_capped_nonzero_gets_asterisk(self):
        f = AppendAsteriskOutputFilter(["PR_ADDED"])
        assert f.apply("PR_ADDED", 120) == "120*"

    def test_zero_untouched(self):
        f = AppendAsteriskOutputFilter(["PR_ADDED"])
        assert f.apply("PR_ADDED", 0) == 0

    def test_none_untouched(self):
        f = AppendAsteriskOutputFilter(["PR_ADDED"])
        assert f.apply("PR_ADDED", None) is None

    def test_non_capped_column_unchanged(self):
        f = AppendAsteriskOutputFilter(["PR_ADDED"])
        assert f.apply("PR_REMOVED", 50) == 50

    def test_commits_column_flagged(self):
        f = AppendAsteriskOutputFilter(["PR_ADDED", "PR_REMOVED", "COMMITS"])
        assert f.apply("COMMITS", 7) == "7*"

    def test_string_value_unchanged(self):
        # Only numeric values are flagged.
        f = AppendAsteriskOutputFilter(["PR_ADDED"])
        assert f.apply("PR_ADDED", "x") == "x"


class TestSafeIntStripsAsterisk:
    def test_capped_value(self):
        assert safe_int("120*") == 120

    def test_capped_value_with_spaces(self):
        assert safe_int(" 45* ") == 45

    def test_bare_asterisk_is_zero(self):
        assert safe_int("*") == 0

    def test_empty_is_zero(self):
        assert safe_int("") == 0

    def test_plain_values(self):
        assert safe_int("30") == 30
        assert safe_int(30) == 30
