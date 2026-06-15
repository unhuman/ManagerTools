import json
import pytest
from unittest.mock import Mock, patch

from managertools.rest.github_rest import GithubREST

URL = "https://api.github.com/repos/owner/repo/pulls/1"


def _response(status, text, content_type="application/json", json_value=None):
    r = Mock()
    r.status_code = status
    r.reason = "OK"
    r.headers = {"Content-Type": content_type}
    r.text = text
    if json_value is not None:
        r.json.return_value = json_value
    else:
        r.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
    return r


def _run(responses):
    """Drive _execute_request with a sequence of mocked HTTP responses."""
    github = GithubREST("test_token")
    session = Mock()
    session.request.side_effect = responses
    with patch.object(github, "_get_session", return_value=session), \
         patch("managertools.rest.rest_service.time.sleep"):
        result = github._execute_request("GET", URL, None, {})
    return github, session, result


class TestEmptyOrNonJsonRetry:
    def test_empty_body_retries_then_succeeds(self):
        responses = [
            _response(200, ""),                                  # empty -> retry
            _response(200, "   "),                               # whitespace -> retry
            _response(200, "[]", json_value=[]),                 # valid JSON -> return
        ]
        _, session, result = _run(responses)
        assert result == []
        assert session.request.call_count == 3

    def test_html_body_retries_then_succeeds(self):
        responses = [
            _response(200, "<html>502 Bad Gateway</html>", content_type="text/html"),
            _response(200, '{"ok": true}', json_value={"ok": True}),
        ]
        _, session, result = _run(responses)
        assert result == {"ok": True}
        assert session.request.call_count == 2

    def test_persistent_empty_body_eventually_raises(self):
        # GithubREST default _max_transient_retries is 3 -> 1 initial + 3 retries = 4 attempts.
        responses = [_response(200, "") for _ in range(5)]
        with pytest.raises(json.JSONDecodeError):
            _run(responses)

    def test_legit_empty_json_array_not_retried(self):
        # A valid empty JSON array must pass straight through (not treated as a failure).
        _, session, result = _run([_response(200, "[]", json_value=[])])
        assert result == []
        assert session.request.call_count == 1

    def test_204_no_content_returns_none(self):
        _, session, result = _run([_response(204, "")])
        assert result is None
        assert session.request.call_count == 1
