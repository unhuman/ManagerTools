from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.output.format_commit_data_output_filter import FormatCommitDataOutputFilter
from managertools.data.db_data import DBData


def _analysis():
    # Bypass __init__ — _is_merge_commit only depends on the class-level MERGE_COMMIT_REGEX.
    return SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)


class TestIsMergeCommit:
    def test_parent_count_two_is_merge(self):
        a = _analysis()
        assert a._is_merge_commit({"parents_count": 2}, "anything") is True

    def test_parent_count_one_is_normal(self):
        a = _analysis()
        # Message says "merge" but parent count is authoritative -> not a merge (no false positive).
        assert a._is_merge_commit({"parents_count": 1}, "Fix merge conflict in parser") is False

    def test_parent_count_zero_is_normal(self):
        a = _analysis()
        assert a._is_merge_commit({"parents_count": 0}, "initial commit") is False

    def test_bitbucket_parents_list_two_is_merge(self):
        a = _analysis()
        assert a._is_merge_commit({"parents": [{"id": "a"}, {"id": "b"}]}, "x") is True

    def test_bitbucket_parents_list_one_is_normal(self):
        a = _analysis()
        assert a._is_merge_commit({"parents": [{"id": "a"}]}, "x") is False

    def test_regex_fallback_when_no_parent_data(self):
        a = _analysis()
        assert a._is_merge_commit({}, "Merge branch 'main' into feature") is True
        assert a._is_merge_commit({}, "Implement feature X") is False


class TestFormatCommitDataMergeRendering:
    def test_merge_entry_prefixed(self):
        f = FormatCommitDataOutputFilter()
        data = [
            {"message": "real work", "additions": 10, "deletions": 2, "type": "normal"},
            {"message": "Merge branch 'main'", "additions": 0, "deletions": 0, "type": "merge"},
        ]
        result = f.apply(DBData.COMMIT_DATA.name, data)
        assert result == ["real work (+10/-2)", "[merge] Merge branch 'main' (+0/-0)"]

    def test_non_commit_data_column_unchanged(self):
        f = FormatCommitDataOutputFilter()
        assert f.apply("SOME_OTHER_COLUMN", [1, 2, 3]) == [1, 2, 3]
