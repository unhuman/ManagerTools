from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.output.format_commit_data_output_filter import FormatCommitDataOutputFilter
from managertools.data.db_data import DBData


def _analysis(title_patterns=None, trunk_branches=None):
    # Bypass __init__; the down-merge helpers only read these two attributes.
    a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
    a.down_merge_title_patterns = (title_patterns
                                   if title_patterns is not None
                                   else SprintReportTeamAnalysis.DEFAULT_DOWN_MERGE_TITLE_PATTERNS)
    a.down_merge_trunk_branches = (trunk_branches
                                   if trunk_branches is not None
                                   else SprintReportTeamAnalysis.DEFAULT_DOWN_MERGE_TRUNK_BRANCHES)
    return a


class TestPrTitleIsDownMerge:
    def test_downmerge_no_space(self):
        assert _analysis()._pr_title_is_down_merge("downmerge main to release") is True

    def test_down_merge_with_space(self):
        assert _analysis()._pr_title_is_down_merge("Down Merge of main") is True

    def test_plain_merge_is_not_down_merge(self):
        # Bare "merge" must NOT match the default down-merge pattern (avoids false positives).
        assert _analysis()._pr_title_is_down_merge("Fix merge conflict in parser") is False

    def test_empty_title(self):
        assert _analysis()._pr_title_is_down_merge("") is False

    def test_disabled_when_no_patterns(self):
        assert _analysis(title_patterns=[])._pr_title_is_down_merge("downmerge") is False

    def test_invalid_regex_is_ignored(self):
        # A bad pattern is skipped, not raised; the good pattern still matches.
        a = _analysis(title_patterns=["(", r"(?i).*down\s*merge.*"])
        assert a._pr_title_is_down_merge("downmerge") is True


class TestHeadBranchIsTrunk:
    def test_main_matches(self):
        assert _analysis()._head_branch_is_trunk("main") is True

    def test_release_glob_matches(self):
        assert _analysis()._head_branch_is_trunk("release/2026.6") is True

    def test_feature_does_not_match(self):
        assert _analysis()._head_branch_is_trunk("feature/ERPT-829-thing") is False

    def test_fullmatch_avoids_substring_false_positive(self):
        # 'main' must not match 'maintenance' (fullmatch, not search).
        assert _analysis()._head_branch_is_trunk("maintenance") is False

    def test_hotfix_excluded_by_default(self):
        assert _analysis()._head_branch_is_trunk("hotfix/urgent") is False

    def test_none_head_ref(self):
        assert _analysis()._head_branch_is_trunk(None) is False

    def test_disabled_when_no_branches(self):
        assert _analysis(trunk_branches=[])._head_branch_is_trunk("main") is False


class TestSkippedMarkerRendering:
    def test_skipped_entry_rendered(self):
        f = FormatCommitDataOutputFilter()
        data = [
            {"message": "real work", "additions": 10, "deletions": 2, "type": "normal"},
            {"message": "[skipped: down-merge PR (head=main)]", "additions": 0,
             "deletions": 0, "type": "skipped"},
        ]
        result = f.apply(DBData.COMMIT_DATA.name, data)
        assert result == ["real work (+10/-2)", "[skipped] [skipped: down-merge PR (head=main)]"]
