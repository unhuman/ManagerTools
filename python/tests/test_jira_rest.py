from unittest.mock import patch

from managertools.rest.jira_rest import JiraREST


class TestDedupePullRequests:
    def test_dedupes_by_url_preserving_order(self):
        prs = [
            {"id": "1", "url": "https://gh/c4/pull/10"},
            {"id": "1", "url": "https://gh/c4/pull/10"},   # dup of #10
            {"id": "2", "url": "https://gh/c4/pull/11"},
            {"id": "1", "url": "https://gh/c4/pull/10"},   # dup again
        ]
        unique = JiraREST._dedupe_pull_requests(prs)
        assert [p["url"] for p in unique] == ["https://gh/c4/pull/10", "https://gh/c4/pull/11"]

    def test_falls_back_to_id_when_no_url(self):
        prs = [{"id": "1"}, {"id": "1"}, {"id": "2"}]
        unique = JiraREST._dedupe_pull_requests(prs)
        assert [p["id"] for p in unique] == ["1", "2"]

    def test_keeps_unkeyable_entries(self):
        prs = [{"title": "no id or url"}, {"title": "also none"}]
        unique = JiraREST._dedupe_pull_requests(prs)
        assert len(unique) == 2

    def test_skips_non_dicts(self):
        prs = [{"url": "u1"}, "garbage", {"url": "u1"}]
        unique = JiraREST._dedupe_pull_requests(prs)
        assert unique == [{"url": "u1"}]


class TestGetTicketPullRequestInfo:
    def _jira(self):
        return JiraREST("jira.example.com", "bearer_token")

    def test_returns_deduped_pull_requests(self):
        jira = self._jira()
        stash_resp = {"detail": []}
        # github returns the same PR three times (dev-status repeats per branch/commit)
        dup = {"id": "10", "url": "https://github.com/cvent-internal/c4/pull/28596", "status": "MERGED"}
        other = {"id": "11", "url": "https://github.com/cvent-internal/c4/pull/28600", "status": "OPEN"}
        github_resp = {"detail": [{"pullRequests": [dup, dup, dup, other]}]}

        with patch.object(jira, "get_request", side_effect=[stash_resp, github_resp]):
            result = jira.get_ticket_pull_request_info("6474231")

        urls = [p["url"] for p in result]
        assert urls == [
            "https://github.com/cvent-internal/c4/pull/28596",
            "https://github.com/cvent-internal/c4/pull/28600",
        ]
        assert len(result) == 2
