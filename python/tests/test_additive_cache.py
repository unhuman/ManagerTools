"""Orchestration tests for the additive, source-aware cache: a mode switch tops up only the
missing source and never reprocesses a source already present-and-complete."""
import pytest

from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.abstract_sprint_report import Mode
from managertools.data import DBData, DBIndexData, WorkSource
from managertools.flexidb.flexidb import FlexiDB
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.util.sprint_data_cache import SprintDataCache


TEAM, NAME, START, END = "Replock", "Sprint 1", "2026/02/03", "2026/03/03"
ALL_ISSUES = [{"key": "T-1", "id": "100"}]


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(SprintDataCache, "CACHE_DIR", str(tmp_path / "cacheData"))


def _make_analysis(work_source):
    """Analysis with a real DB and a stubbed get_issue_category_information that records each call
    and populates one row per processed source (so extract/cache captures it)."""
    a = SprintReportTeamAnalysis.__new__(SprintReportTeamAnalysis)
    a.database = FlexiDB(a.generate_db_signature(), True)
    a.work_source = work_source
    a.calls = []

    def fake_gici(thread_count, sprint, mode, issue_list, pr_id_filter=None, sources=None):
        sources = set(sources) if sources is not None else a._sources_for(a.work_source)
        a.calls.append({"sources": sources, "issues": [i.get("key") for i in issue_list]})
        recs = {}
        name = sprint.get("name")
        for s in sources:
            if s == WorkSource.PR.value:
                idx = a.create_index_lookup(name, "T-1", "1", "UserA", "MERGED")
            else:
                idx = a.create_index_lookup(name, "T-1", SprintReportTeamAnalysis.COMMITS_PR_ID, "UserA", "")
            a.populate_baseline_db_info(idx, "s", "e", "UserA")
            a.database.append(idx, DBData.COMMIT_DATA.name,
                              {"sha": s.upper(), "type": "normal", "additions": 1, "deletions": 0, "message": s})
            recs[s] = {"complete": True, "failed_issues": [], "failed_prs": []}
        return recs

    a.get_issue_category_information = fake_gici
    return a


def _run(a):
    a._process_sprint_with_source_aware_cache(
        1, TEAM, NAME, START, END,
        {"name": NAME, "startDate": START, "endDate": END}, Mode.SCRUM,
        cacheable=True, get_all_issues=lambda: ALL_ISSUES)


def _rows():
    return SprintDataCache.load_cached_data(TEAM, NAME, START, END)["rows"]


def _pr_ids():
    return {r.get(DBIndexData.PR_ID.name) for r in _rows()}


class TestAdditiveTopUp:
    def test_pr_then_commit_tops_up_without_reprocessing_pr(self, cache_dir):
        # Run 1: PR only.
        a1 = _make_analysis(WorkSource.PR)
        _run(a1)
        assert a1.calls == [{"sources": {"pr"}, "issues": ["T-1"]}]
        meta = SprintDataCache.load_cache_meta(TEAM, NAME, START, END)
        assert set(meta["sources"]) == {"pr"}
        assert _pr_ids() == {"1"}

        # Run 2: COMMIT — must fetch ONLY commit, preserve the PR row.
        a2 = _make_analysis(WorkSource.COMMIT)
        _run(a2)
        assert a2.calls == [{"sources": {"commit"}, "issues": ["T-1"]}]  # PR not reprocessed
        meta = SprintDataCache.load_cache_meta(TEAM, NAME, START, END)
        assert set(meta["sources"]) == {"pr", "commit"}  # additive
        assert _pr_ids() == {"1", SprintReportTeamAnalysis.COMMITS_PR_ID}  # PR row preserved

        # Run 3: BOTH — both sources present & complete => pure cache hit, no processing.
        a3 = _make_analysis(WorkSource.BOTH)
        _run(a3)
        assert a3.calls == []  # fast path
        # DB was loaded from the union.
        loaded = a3.database.find_rows([FlexiDBQueryColumn(DBIndexData.SPRINT.name, NAME)], True)
        assert {r.get(DBIndexData.PR_ID.name) for r in loaded} == {"1", SprintReportTeamAnalysis.COMMITS_PR_ID}

    def test_both_fresh_processes_both_in_one_pass(self, cache_dir):
        a = _make_analysis(WorkSource.BOTH)
        _run(a)
        assert len(a.calls) == 1
        assert a.calls[0]["sources"] == {"pr", "commit"}  # single combined pass
        assert set(SprintDataCache.load_cache_meta(TEAM, NAME, START, END)["sources"]) == {"pr", "commit"}

    def test_pr_fast_path_after_pr_complete(self, cache_dir):
        _run(_make_analysis(WorkSource.PR))
        a2 = _make_analysis(WorkSource.PR)
        _run(a2)
        assert a2.calls == []  # already present & complete -> no processing
