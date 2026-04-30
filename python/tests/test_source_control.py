import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from http import HTTPStatus

from managertools.rest.github_rest import GithubREST
from managertools.rest.bitbucket_rest import BitbucketREST
from managertools.rest.exceptions import RESTException
from managertools.data.user_activity import UserActivity


class TestGithubREST:
    def test_api_convert(self):
        github = GithubREST("test_token")
        result = github.api_convert("https://github.com/owner/repo/pull/123")
        assert result == "https://api.github.com/repos/owner/repo/pulls/123"

    def test_api_convert_no_change(self):
        github = GithubREST("test_token")
        result = github.api_convert("https://api.github.com/repos/owner/repo/pulls/123")
        assert result == "https://api.github.com/repos/owner/repo/pulls/123"

    @patch.object(GithubREST, 'get_comments')
    @patch.object(GithubREST, 'get_reviews')
    def test_get_activities_success(self, mock_reviews, mock_comments):
        mock_comments.return_value = [{'id': 1, 'action': 'COMMENTED'}]
        mock_reviews.return_value = [{'id': 2, 'action': 'APPROVED'}]

        github = GithubREST("test_token")
        result = github.get_activities("https://api.github.com/repos/owner/repo/pulls/123")

        assert len(result) == 2
        assert result[0]['id'] == 1
        assert result[1]['id'] == 2

    @patch.object(GithubREST, 'get_comments')
    @patch.object(GithubREST, 'get_reviews')
    def test_get_activities_handles_404(self, mock_reviews, mock_comments):
        mock_comments.side_effect = RESTException(HTTPStatus.NOT_FOUND, "Not found", "url")
        mock_reviews.return_value = []

        github = GithubREST("test_token")
        result = github.get_activities("https://api.github.com/repos/owner/repo/pulls/123")

        assert len(result) == 0

    @patch.object(GithubREST, 'get_request')
    def test_get_comments(self, mock_request):
        mock_request.return_value = {
            'values': [
                {
                    'id': 1,
                    'user': {'login': 'user1'},
                    'created_at': '2024-01-15T10:30:00Z',
                    'author_association': 'MEMBER',
                    'body': 'Great work'
                }
            ]
        }

        github = GithubREST("test_token")
        result = github.get_comments("https://api.github.com/repos/owner/repo/pulls/123/comments")

        assert len(result) == 1
        assert result[0]['action'] == UserActivity.COMMENTED.name
        assert result[0]['comment']['text'] == 'Great work'
        assert 'createdDate' in result[0]

    @patch.object(GithubREST, 'get_request')
    def test_get_comments_filters_non_members(self, mock_request):
        mock_request.return_value = {
            'values': [
                {
                    'id': 1,
                    'user': {'login': 'user1'},
                    'created_at': '2024-01-15T10:30:00Z',
                    'author_association': 'NONE',
                    'body': 'Comment'
                }
            ]
        }

        github = GithubREST("test_token")
        result = github.get_comments("https://api.github.com/repos/owner/repo/pulls/123/comments")

        assert len(result) == 0

    @patch.object(GithubREST, 'get_request')
    def test_get_reviews(self, mock_request):
        mock_request.return_value = {
            'values': [
                {
                    'id': 1,
                    'user': {'login': 'user1'},
                    'submitted_at': '2024-01-15T10:30:00Z',
                    'author_association': 'MEMBER',
                    'state': 'APPROVED',
                    'body': 'Approved'
                }
            ]
        }

        github = GithubREST("test_token")
        result = github.get_reviews("https://api.github.com/repos/owner/repo/pulls/123/reviews")

        assert len(result) == 1
        assert result[0]['action'] == 'APPROVED'
        assert 'createdDate' in result[0]

    @patch.object(GithubREST, 'get_request')
    def test_get_commits(self, mock_request):
        mock_request.return_value = {
            'values': [
                {
                    'sha': 'abc123',
                    'author': {'login': 'user1'},
                    'committer': {'name': 'User One'},
                    'commit': {
                        'message': 'Fix bug',
                        'committer': {'date': '2024-01-15T10:30:00Z'},
                        'author': {'name': 'User One'}
                    }
                }
            ]
        }

        github = GithubREST("test_token")
        result = github.get_commits("https://api.github.com/repos/owner/repo/pulls/123/commits")

        assert len(result['values']) == 1
        assert result['values'][0]['id'] == 'abc123'
        assert result['values'][0]['message'] == 'Fix bug'

    @patch.object(GithubREST, 'get_request')
    def test_get_commits_handles_error(self, mock_request):
        mock_request.side_effect = RESTException(HTTPStatus.FORBIDDEN, "Forbidden", "url")

        github = GithubREST("test_token")
        result = github.get_commits("https://api.github.com/repos/owner/repo/pulls/123/commits")

        assert result is None

    @patch.object(GithubREST, 'get_request')
    def test_get_diffs(self, mock_request):
        mock_request.return_value = {'diff': 'data'}

        github = GithubREST("test_token")
        result = github.get_diffs("https://api.github.com/repos/owner/repo/pulls/123")

        assert result == {'diff': 'data'}

    @patch.object(GithubREST, 'get_request')
    def test_get_diffs_handles_error(self, mock_request):
        mock_request.side_effect = RESTException(HTTPStatus.NOT_FOUND, "Not found", "url")

        github = GithubREST("test_token")
        result = github.get_diffs("https://api.github.com/repos/owner/repo/pulls/123")

        assert result is None

    def test_get_commit_diffs_invalid_url(self):
        github = GithubREST("test_token")
        with pytest.raises(RuntimeError):
            github.get_commit_diffs("https://api.github.com/repos/owner/repo/invalid", "abc123")

    @patch.object(GithubREST, 'get_request')
    def test_get_commit_diffs(self, mock_request):
        mock_request.return_value = {'diff': 'data'}

        github = GithubREST("test_token")
        result = github.get_commit_diffs("https://api.github.com/repos/owner/repo/commits/abc123", "abc123")

        assert result == {'diff': 'data'}

    def test_map_user_to_jira_name_none(self):
        github = GithubREST("test_token")
        assert github.map_user_to_jira_name(None) is None

    def test_map_user_to_jira_name_string(self):
        github = GithubREST("test_token")
        assert github.map_user_to_jira_name("user1") == "user1"

    def test_map_user_to_jira_name_dict(self):
        github = GithubREST("test_token")
        assert github.map_user_to_jira_name({'login': 'user1'}) == "user1"

    def test_map_user_to_jira_name_from_url(self):
        github = GithubREST("test_token")
        result = github.map_user_to_jira_name({'url': 'https://api.github.com/users/USER1'})
        assert result == "user1"

    def test_map_user_to_jira_name_strips_enterprise_suffix(self):
        github = GithubREST("test_token")
        result = github.map_user_to_jira_name({'login': 'user1_enterprise'})
        assert result == "user1"

    def test_map_user_to_jira_name_long_string_without_underscore(self):
        github = GithubREST("test_token")
        long_name = "a" * 39
        result = github.map_user_to_jira_name({'login': long_name})
        assert result is None


class TestBitbucketREST:
    def test_constructor_basic_auth(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "user1", "pass1")
        assert bitbucket.bitbucket_server == "http://bitbucket.example.com"
        assert bitbucket.auth_info.auth_type.name == "Basic"

    def test_constructor_bearer_auth(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        assert bitbucket.bitbucket_server == "http://bitbucket.example.com"
        assert bitbucket.auth_info.auth_type.name == "Bearer"

    def test_constructor_cookie_auth(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "session=abc123; path=/")
        assert bitbucket.bitbucket_server == "http://bitbucket.example.com"
        assert bitbucket.auth_info.auth_type.name == "Cookies"

    @patch.object(BitbucketREST, 'get_request')
    def test_get_activities(self, mock_request):
        mock_request.return_value = {'values': [{'id': 1}]}

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_activities("http://bitbucket.example.com/project/repo/pull-requests/123")

        mock_request.assert_called_once()
        assert result['values'][0]['id'] == 1

    @patch.object(BitbucketREST, 'get_request')
    def test_get_commits(self, mock_request):
        mock_request.return_value = {'values': [{'id': 'abc123'}]}

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_commits("http://bitbucket.example.com/project/repo/pull-requests/123")

        assert result['values'][0]['id'] == 'abc123'

    @patch.object(BitbucketREST, 'get_request')
    def test_get_commits_handles_404(self, mock_request):
        mock_request.side_effect = RESTException(HTTPStatus.NOT_FOUND, "Not found", "url")

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_commits("http://bitbucket.example.com/project/repo/pull-requests/123")

        assert result is None

    @patch.object(BitbucketREST, 'get_request')
    def test_get_diffs(self, mock_request):
        mock_request.return_value = {'diffs': []}

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_diffs("http://bitbucket.example.com/project/repo/pull-requests/123")

        assert 'diffs' in result

    @patch.object(BitbucketREST, 'get_request')
    def test_get_diffs_handles_500(self, mock_request):
        mock_request.side_effect = RESTException(HTTPStatus.INTERNAL_SERVER_ERROR, "Server error", "url")

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_diffs("http://bitbucket.example.com/project/repo/pull-requests/123")

        assert result is None

    def test_get_commit_diffs_invalid_url(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        with pytest.raises(RuntimeError):
            bitbucket.get_commit_diffs("http://bitbucket.example.com/project/repo/invalid", "abc123")

    @patch.object(BitbucketREST, 'get_request')
    def test_get_commit_diffs(self, mock_request):
        mock_request.return_value = {'diffs': []}

        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        result = bitbucket.get_commit_diffs(
            "http://bitbucket.example.com/project/repo/pull-requests/123",
            "abc123"
        )

        assert 'diffs' in result

    def test_map_user_to_jira_name_none(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        assert bitbucket.map_user_to_jira_name(None) is None

    def test_map_user_to_jira_name_dict(self):
        bitbucket = BitbucketREST("http://bitbucket.example.com", "token123")
        assert bitbucket.map_user_to_jira_name({'name': 'user1'}) == "user1"
