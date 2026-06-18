from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.output.format_commit_data_output_filter import FormatCommitDataOutputFilter
from managertools.data.db_data import DBData


def _commit(oid, *parents):
    return {"id": oid, "parent_oids": list(parents)}


class TestBroughtInOids:
    def test_sync_merge_marks_brought_in(self):
        # Feature line c1->c2, then merge M of main (m1->m2) into feature, then c3.
        #   head=c3 -> M(parents [c2, m2]) -> c2 -> c1
        #   main side: m2 -> m1
        commits = [
            _commit("c1"),
            _commit("c2", "c1"),
            _commit("m1"),
            _commit("m2", "m1"),
            _commit("M", "c2", "m2"),   # first parent c2 (feature), second parent m2 (main)
            _commit("c3", "M"),
        ]
        brought_in = SprintReportTeamAnalysis._brought_in_oids(commits, "c3")
        assert brought_in == {"m1", "m2"}

    def test_no_merge_nothing_brought_in(self):
        commits = [_commit("c1"), _commit("c2", "c1"), _commit("c3", "c2")]
        assert SprintReportTeamAnalysis._brought_in_oids(commits, "c3") == set()

    def test_missing_head_is_noop(self):
        commits = [_commit("c1"), _commit("c2", "c1")]
        assert SprintReportTeamAnalysis._brought_in_oids(commits, None) == set()
        assert SprintReportTeamAnalysis._brought_in_oids(commits, "nope") == set()

    def test_octopus_merge_all_extra_parents_brought_in(self):
        commits = [
            _commit("a"),
            _commit("b"),
            _commit("c"),
            _commit("M", "a", "b", "c"),  # first parent a; b, c brought in
        ]
        assert SprintReportTeamAnalysis._brought_in_oids(commits, "M") == {"b", "c"}

    def test_empty_commits(self):
        assert SprintReportTeamAnalysis._brought_in_oids([], "x") == set()


class TestBroughtInRendering:
    def test_brought_in_prefixed(self):
        f = FormatCommitDataOutputFilter()
        data = [
            {"message": "real work", "additions": 10, "deletions": 2, "type": "normal"},
            {"message": "old main commit", "additions": 5, "deletions": 1, "type": "brought-in"},
        ]
        result = f.apply(DBData.COMMIT_DATA.name, data)
        assert result == ["real work (+10/-2)", "[brought-in] old main commit (+5/-1)"]
