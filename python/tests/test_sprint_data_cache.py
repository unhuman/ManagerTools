import json

import pytest

from managertools.util.sprint_data_cache import SprintDataCache


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    d = tmp_path / "cacheData"
    monkeypatch.setattr(SprintDataCache, "CACHE_DIR", str(d))
    return d


TEAM, SPRINT, START, END = "Replock", "Sprint 1", "2026/02/03", "2026/03/03"


def _sources(pr_complete=True, commit_complete=None):
    s = {"pr": {"complete": pr_complete, "failed_issues": [], "failed_prs": []}}
    if commit_complete is not None:
        s["commit"] = {"complete": commit_complete, "failed_issues": [], "failed_prs": []}
    return s


class TestSourceAwareRoundTrip:
    def test_save_then_load_meta(self, cache_dir):
        data = {"rows": [{"SPRINT": SPRINT, "TICKET": "T-1"}]}
        srcs = _sources(pr_complete=True, commit_complete=False)
        SprintDataCache.save_to_cache(TEAM, SPRINT, START, END, data, sources=srcs)

        meta = SprintDataCache.load_cache_meta(TEAM, SPRINT, START, END)
        assert meta is not None
        assert meta["version"] == SprintDataCache.CACHE_VERSION
        assert meta["sources"] == srcs
        assert meta["data"] == data
        # load_cached_data returns just the rows payload
        assert SprintDataCache.load_cached_data(TEAM, SPRINT, START, END) == data

    def test_load_meta_absent_is_none(self, cache_dir):
        assert SprintDataCache.load_cache_meta(TEAM, SPRINT, START, END) is None
        assert SprintDataCache.has_cached_data(TEAM, SPRINT, START, END) is False


class TestVersionGate:
    def test_old_version_ignored(self, cache_dir):
        cache_dir.mkdir(parents=True, exist_ok=True)
        key = SprintDataCache.generate_cache_key(TEAM, SPRINT, START, END)
        # Hand-write a 2.7 (old-shape) file.
        (cache_dir / f"{key}.json").write_text(json.dumps({
            "version": "2.7", "complete": True, "data": {"rows": []}}))
        assert SprintDataCache.has_cached_data(TEAM, SPRINT, START, END) is False
        assert SprintDataCache.load_cache_meta(TEAM, SPRINT, START, END) is None


class TestIsCacheCompleteRequiredSources:
    def test_missing_file_is_complete(self, cache_dir):
        assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, {"pr"}) is True

    def test_pr_only_complete(self, cache_dir):
        SprintDataCache.save_to_cache(TEAM, SPRINT, START, END, {"rows": []},
                                      sources=_sources(pr_complete=True))
        assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, {"pr"}) is True
        # commit source absent -> not complete for a both-run
        assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, {"pr", "commit"}) is False
        assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, {"commit"}) is False

    def test_both_present_complete(self, cache_dir):
        SprintDataCache.save_to_cache(TEAM, SPRINT, START, END, {"rows": []},
                                      sources=_sources(pr_complete=True, commit_complete=True))
        for req in ({"pr"}, {"commit"}, {"pr", "commit"}):
            assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, req) is True

    def test_present_but_incomplete(self, cache_dir):
        SprintDataCache.save_to_cache(TEAM, SPRINT, START, END, {"rows": []},
                                      sources=_sources(pr_complete=False))
        assert SprintDataCache.is_cache_complete(TEAM, SPRINT, START, END, {"pr"}) is False
