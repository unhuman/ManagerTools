import csv
import io
import types
from unittest.mock import patch

from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.abstract_sprint_report import Mode
from managertools.data import DBData, DBIndexData, UserActivity, WorkSource
from managertools.flexidb.flexidb import FlexiDB
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.flexidb.data.flexidb_row import FlexiDBRow
from managertools.rest.github_rest import GithubREST
from managertools.rest.bitbucket_rest import BitbucketREST


class FakeDB:
    """Minimal in-memory stand-in for FlexiDB supporting the methods the commit pass uses."""

    def __init__(self):
        self.rows = {}

    @staticmethod
    def _key(index_lookup):
        return tuple((c.get_name(), c.get_match_value()) for c in index_lookup)

    def _row(self, index_lookup):
        key = self._key(index_lookup)
        row = self.rows.get(key)
        if row is None:
            row = dict(key)  # seed with index columns so find_rows can match
            self.rows[key] = row
        return row

    def set_value(self, index_lookup, field, value):
        self._row(index_lookup)[field] = value

    def get_value(self, index_lookup, field):
        return self._row(index_lookup).get(field)

    def increment_field(self, index_lookup, field, increment=1):
        row = self._row(index_lookup)
        row[field] = (row.get(field) or 0) + increment
        return row[field]

    def append(self, index_lookup, field, value):
        self._row(index_lookup).setdefault(field, []).append(value)

    def find_rows(self, filt, _flag=True):
        out = []
        for row in self.rows.values():
            if all(row.get(c.get_name()) == c.get_match_value() for c in filt):
                out.append(row)
        return out


def _analysis(scm=None):
    a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
    a.database = FakeDB()
    a.github_rest = scm
    a.bitbucket_rest = scm
    a.work_source = WorkSource.COMMIT
    a.max_commit_size = None
    a.ignore_commit_message_patterns = []
    return a


class _StubSCM:
    """Returns a fixed GitHub-style stats diff for every commit."""

    def __init__(self, additions=7, deletions=3):
        self.calls = []
        self._resp = {"stats": {"additions": additions, "deletions": deletions}}

    def get_repo_commit_diffs(self, repo_url, sha):
        self.calls.append((repo_url, sha))
        return self._resp


def _commit(sha, name="Dev One", ts=1500, message="real work", merge=False, repo="https://github.com/org/repo"):
    return {"id": sha, "author": {"name": name}, "authorTimestamp": ts,
            "message": message, "merge": merge, "_repository": {"url": repo, "name": "repo"}}


WINDOW = dict(sprint_start_ms=1000, sprint_end_ms=2000)


class TestProcessTicketCommits:
    def _run(self, a, commits, mode=Mode.SCRUM):
        a.process_ticket_commits("TIK-1", commits, "Sprint 1", "s", "e",
                                 WINDOW["sprint_start_ms"], WINDOW["sprint_end_ms"], mode)

    def test_normal_commit_recorded_under_synthetic_pr_with_sha(self):
        scm = _StubSCM(additions=10, deletions=4)
        a = _analysis(scm)
        self._run(a, [_commit("sha1")])

        rows = [r for r in a.database.rows.values()
                if r.get(DBIndexData.PR_ID.name) == SprintReportTeamAnalysis.COMMITS_PR_ID]
        assert len(rows) == 1
        row = rows[0]
        assert row[DBIndexData.PR_STATUS.name] == ""
        assert row[DBIndexData.USER.name] == "Dev One"
        assert row[UserActivity.COMMITS.name] == 1
        entry = row[DBData.COMMIT_DATA.name][0]
        assert entry == {"message": "real work", "additions": 10, "deletions": 4,
                         "type": "normal", "sha": "sha1"}
        assert scm.calls == [("https://github.com/org/repo", "sha1")]

    def test_commit_outside_window_skipped(self):
        scm = _StubSCM()
        a = _analysis(scm)
        self._run(a, [_commit("late", ts=9999), _commit("early", ts=10)])
        assert scm.calls == []  # both out of [1000, 2000)
        assert a.database.rows == {}

    def test_kanban_ignores_window(self):
        scm = _StubSCM()
        a = _analysis(scm)
        self._run(a, [_commit("late", ts=9999)], mode=Mode.KANBAN)
        assert scm.calls == [("https://github.com/org/repo", "late")]

    def test_ignored_user_skipped(self):
        # IGNORE_USERS is matched case-insensitively (same as the PR path's user_name.lower()).
        scm = _StubSCM()
        a = _analysis(scm)
        self._run(a, [_commit("s", name="CodeOwners")])
        assert scm.calls == []
        assert a.database.rows == {}

    def test_merge_commit_not_diff_fetched(self):
        scm = _StubSCM()
        a = _analysis(scm)
        self._run(a, [_commit("m", merge=True, message="Merge branch main")])
        assert scm.calls == []  # merges use inline values, never fetched
        row = next(iter(a.database.rows.values()))
        entry = row[DBData.COMMIT_DATA.name][0]
        assert entry["type"] == "merge"
        assert entry["additions"] == 0 and entry["deletions"] == 0

    def test_stores_full_set_no_collection_dedupe(self):
        # The commit pass no longer de-dupes at collection time (that moved to render); it always
        # stores the full set so the cached commit rows are mode-independent.
        scm = _StubSCM()
        a = _analysis(scm)
        self._run(a, [_commit("a"), _commit("b")])
        assert scm.calls == [("https://github.com/org/repo", "a"),
                             ("https://github.com/org/repo", "b")]
        row = next(iter(a.database.rows.values()))
        assert [e["sha"] for e in row[DBData.COMMIT_DATA.name]] == ["a", "b"]

    def test_string_timestamp_in_window_processed(self):
        # dev-status repository detail returns authorTimestamp as a string; it must be coerced
        # before the numeric window comparison (regression: float > str TypeError).
        scm = _StubSCM()
        a = _analysis(scm)
        c = _commit("s")
        c["authorTimestamp"] = "1500"  # string epoch ms, inside [1000, 2000)
        self._run(a, [c])
        assert scm.calls == [("https://github.com/org/repo", "s")]

    def test_iso_timestamp_out_of_window_skipped(self):
        scm = _StubSCM()
        a = _analysis(scm)
        c = _commit("s")
        c["authorTimestamp"] = "2030-01-01T00:00:00.000+0000"  # far future -> out of window
        self._run(a, [c])
        assert scm.calls == []


class TestCoerceEpochMs:
    def test_int_passthrough(self):
        assert SprintReportTeamAnalysis._coerce_epoch_ms(1500) == 1500

    def test_numeric_string(self):
        assert SprintReportTeamAnalysis._coerce_epoch_ms("1500") == 1500

    def test_iso_string_with_colonless_offset(self):
        from datetime import datetime
        expected = int(datetime.fromisoformat("2026-02-13T18:08:08.000+00:00").timestamp() * 1000)
        assert SprintReportTeamAnalysis._coerce_epoch_ms("2026-02-13T18:08:08.000+0000") == expected

    def test_iso_string_with_z(self):
        from datetime import datetime
        expected = int(datetime.fromisoformat("2026-02-13T18:08:08+00:00").timestamp() * 1000)
        assert SprintReportTeamAnalysis._coerce_epoch_ms("2026-02-13T18:08:08Z") == expected

    def test_unparseable_and_none(self):
        assert SprintReportTeamAnalysis._coerce_epoch_ms("not-a-date") is None
        assert SprintReportTeamAnalysis._coerce_epoch_ms(None) is None
        assert SprintReportTeamAnalysis._coerce_epoch_ms("") is None


class TestSourcesForAndRowSource:
    def test_sources_for(self):
        assert SprintReportTeamAnalysis._sources_for(WorkSource.PR) == {"pr"}
        assert SprintReportTeamAnalysis._sources_for(WorkSource.COMMIT) == {"commit"}
        assert SprintReportTeamAnalysis._sources_for(WorkSource.BOTH) == {"pr", "commit"}

    def test_row_source(self):
        a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
        assert a._row_source({DBIndexData.PR_ID.name: SprintReportTeamAnalysis.COMMITS_PR_ID}) == "commit"
        assert a._row_source({DBIndexData.PR_ID.name: "1234"}) == "pr"


class TestRenderModeFilteringAndDedupe:
    """End-to-end render: one PR row + one (commits) row sharing ticket T-1 and SHA 'X'.
    PR row: commit X (10/5). Commit row: X (dup, 10/5) + Y (3/2)."""

    def _analysis_with_db(self):
        a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
        a.database = FlexiDB(a.generate_db_signature(), True)
        a.command_line_options = types.SimpleNamespace(
            includeMergeCommits=False, includeBroughtInCommits=False)
        a.max_commit_size = None
        a.ignore_pr_title_patterns = []
        a._overall_capped_cols = set()
        # PR row
        pr = a.create_index_lookup("Sprint 1", "T-1", "1", "UserA", "MERGED")
        a.populate_baseline_db_info(pr, "s", "e", "UserA")
        a.database.append(pr, DBData.COMMIT_DATA.name,
                          {"message": "x", "additions": 10, "deletions": 5, "type": "normal", "sha": "X"})
        # Commit row (full set incl. the dup X)
        cm = a.create_index_lookup("Sprint 1", "T-1", SprintReportTeamAnalysis.COMMITS_PR_ID, "UserA", "")
        a.populate_baseline_db_info(cm, "s", "e", "UserA")
        a.database.append(cm, DBData.COMMIT_DATA.name,
                          {"message": "x", "additions": 10, "deletions": 5, "type": "normal", "sha": "X"})
        a.database.append(cm, DBData.COMMIT_DATA.name,
                          {"message": "y", "additions": 3, "deletions": 2, "type": "normal", "sha": "Y"})
        return a

    def _render(self, work_source):
        a = self._analysis_with_db()
        a.work_source = work_source
        sb = []
        a.find_rows_and_append_csv_data(
            [FlexiDBQueryColumn(DBIndexData.SPRINT.name, "Sprint 1")], sb, FlexiDBRow({}))
        # find_rows_and_append_csv_data emits data rows + the Sprint Totals line (no header — that's
        # added by generate_output). Build the header from the column order.
        header = a.generate_columns_order()
        rows = list(csv.reader(io.StringIO("\n".join(l for l in sb if l))))
        data = [dict(zip(header, r)) for r in rows]
        # Keep only the two data rows (by PR_ID), drop the Sprint Totals line.
        return {d[DBIndexData.PR_ID.name]: d for d in data
                if d.get(DBIndexData.PR_ID.name) in ("1", SprintReportTeamAnalysis.COMMITS_PR_ID)}

    def test_pr_mode_renders_only_pr_row(self):
        rendered = self._render(WorkSource.PR)
        assert set(rendered) == {"1"}
        assert rendered["1"][UserActivity.COMMITS.name] == "1"

    def test_commit_mode_renders_only_commit_row_full_set(self):
        rendered = self._render(WorkSource.COMMIT)
        assert set(rendered) == {SprintReportTeamAnalysis.COMMITS_PR_ID}
        cm = rendered[SprintReportTeamAnalysis.COMMITS_PR_ID]
        assert cm[UserActivity.COMMITS.name] == "2"  # X and Y both counted (no dedupe)
        assert cm[UserActivity.COMMIT_ADDED.name] == "13"

    def test_both_mode_renders_all_with_render_dedupe(self):
        rendered = self._render(WorkSource.BOTH)
        assert set(rendered) == {"1", SprintReportTeamAnalysis.COMMITS_PR_ID}
        cm = rendered[SprintReportTeamAnalysis.COMMITS_PR_ID]
        # X is already counted by the PR row -> only Y survives in the commit row.
        assert cm[UserActivity.COMMITS.name] == "1"
        assert cm[UserActivity.COMMIT_ADDED.name] == "3"
        assert cm[UserActivity.COMMIT_REMOVED.name] == "2"


class TestRepoCommitDiffUrlDerivation:
    def test_github_builds_api_commit_url(self):
        gh = GithubREST("token")
        with patch.object(gh, "get_request", return_value={"stats": {}}) as gr:
            gh.get_repo_commit_diffs("https://github.com/org/repo", "abc123")
        gr.assert_called_once_with("https://api.github.com/repos/org/repo/commits/abc123")

    def test_bitbucket_builds_commit_diff_url(self):
        bb = BitbucketREST("bitbucket.example.com", "token")
        with patch.object(bb, "get_request", return_value={"diffs": []}) as gr:
            bb.get_repo_commit_diffs("https://bb/projects/P/repos/R", "abc123")
        args, kwargs = gr.call_args
        assert args[0] == "https://bb/projects/P/repos/R/commits/abc123/diff"
        assert kwargs["contextLines"] == "0" and kwargs["whitespace"] == "ignore-all"


class TestWorkSourceEnum:
    def test_values(self):
        assert WorkSource("pr") is WorkSource.PR
        assert WorkSource("commit") is WorkSource.COMMIT
        assert WorkSource("both") is WorkSource.BOTH


class TestWorkSourceCli:
    def _parser(self):
        import argparse
        a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
        p = argparse.ArgumentParser()
        a.add_custom_command_line_options(p)
        return p

    def test_default_is_none_so_config_can_apply(self):
        # default=None lets aggregate_data tell "unset" (use config/default) from an explicit choice.
        assert self._parser().parse_args(["-o", "x.csv"]).workSource is None

    def test_explicit_choice_parsed(self):
        assert self._parser().parse_args(["-o", "x.csv", "--workSource", "both"]).workSource == "both"

    def test_invalid_choice_rejected(self):
        import pytest
        with pytest.raises(SystemExit):
            self._parser().parse_args(["-o", "x.csv", "--workSource", "bogus"])
