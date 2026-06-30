from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional
from urllib.parse import urlencode, quote
from http import HTTPStatus

from .rest_service import RestService
from .auth_info import AuthInfo, AuthType
from .exceptions import RESTException


class JiraREST(RestService):
    JQL_LIMIT = 250

    def __init__(self, jira_server: str, username_or_auth: str, password: str = None):
        if password is not None:
            auth_info = AuthInfo(username_or_auth, password)
        else:
            auth_type = AuthType.Cookies if '=' in username_or_auth else AuthType.Bearer
            auth_info = AuthInfo(auth_type, username_or_auth)

        super().__init__(auth_info)
        self.jira_server = jira_server

    def get_sprints(self, board_id: str, fetch_tail: Optional[int] = None) -> List[Any]:
        uri = f"https://{self.jira_server}/rest/agile/1.0/board/{board_id}/sprint"
        now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        start_at = 0

        if fetch_tail is not None:
            # Probe with a 1-item request to read 'total' without pulling all data.
            # If the API returns total (Jira Cloud), jump to near the tail so we only fetch
            # the last fetch_tail * 2 sprints (2× buffer absorbs originBoardId-filtered entries).
            # If total is absent (some Jira Server versions), fall through to a full fetch.
            probe = self.get_request(uri, state="active,closed,future",
                                     startAt="0", maxResults="1", _=now_ms)
            total = probe.get('total')
            if total is not None:
                start_at = max(0, total - fetch_tail * 2)

        values = []
        while True:
            response = self.get_request(uri,
                                       state="active,closed,future",
                                       startAt=str(start_at),
                                       maxResults="250",
                                       _=now_ms)

            values.extend(response.get('values', []))
            if response.get('isLast', True):
                break
            start_at += response.get('maxResults', 0)

        # Filter: remove items without start/end date or future sprints that haven't started
        filtered = []
        for sprint in values:
            if sprint.get('startDate') is None or sprint.get('endDate') is None:
                continue
            if sprint.get('state') == 'future':
                start_dt = datetime.fromisoformat(sprint['startDate'].replace('Z', '+00:00'))
                if start_dt.timestamp() * 1000 > datetime.now(timezone.utc).timestamp() * 1000:
                    continue
            filtered.append(sprint)

        # Sort by end date
        filtered.sort(key=lambda x: x.get('endDate', ''))
        return filtered

    def get_sprint_report(self, board_id: str, sprint_id: str) -> Any:
        uri = f"https://{self.jira_server}/rest/greenhopper/1.0/rapid/charts/sprintreport"
        return self.get_request(uri,
                               rapidViewId=board_id,
                               sprintId=sprint_id,
                               _=str(int(datetime.now(timezone.utc).timestamp() * 1000)))

    def get_kanban_cycle(self, team: str, cycle: int, cycles: int, cycle_length: int) -> Any:
        # Calculate start and end dates
        today = datetime.now().date()
        start_date = today - timedelta(weeks=(cycles - cycle) * cycle_length)
        # Find Monday of that week
        start_date = start_date - timedelta(days=start_date.weekday())
        end_date = start_date + timedelta(days=7 * cycle_length - 1)

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        jql = (f'"Sprint Team" = "{team}" '
               f'AND issueType NOT IN subTaskIssueTypes() '
               f'AND ((resolutiondate >= {start_date_str} AND resolutiondate <= {end_date_str}) '
               f'OR (resolved >= {start_date_str} AND resolved <= {end_date_str}) '
               f'OR ("Resolved Date" >= {start_date_str} AND "Resolved Date" <= {end_date_str}))')

        response = self.jql_summary_query(jql)

        response['name'] = f"{team} Week {cycle}"
        start_fmt = start_date.strftime("%d/%b/%y")
        end_fmt = end_date.strftime("%d/%b/%y")
        response['startDate'] = f"{start_fmt} 12:00 AM"
        response['endDate'] = f"{end_fmt} 11:59 PM"
        return response

    def get_ticket(self, ticket_id: str) -> Any:
        uri = f"https://{self.jira_server}/rest/api/latest/issue/{ticket_id}"
        return self.get_request(uri, _=str(int(datetime.now(timezone.utc).timestamp() * 1000)))

    @staticmethod
    def _dedupe_pull_requests(pull_requests: List[Any]) -> List[Any]:
        """Collapse duplicate PR entries (Jira dev-status can repeat a PR once per
        associated branch/commit). Dedupe by url, falling back to id, preserving order."""
        seen = set()
        unique = []
        for pr in pull_requests:
            if not isinstance(pr, dict):
                continue
            key = pr.get('url') or pr.get('id')
            if key is None:
                unique.append(pr)  # can't key it; keep it rather than drop data
                continue
            if key in seen:
                continue
            seen.add(key)
            unique.append(pr)
        return unique

    def get_ticket_pull_request_info(self, issue_id: str) -> List[Any]:
        uri = f"https://{self.jira_server}/rest/dev-status/1.0/issue/detail"
        now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))

        pull_requests = []
        stash_count = 0
        github_count = 0

        # Try Stash/Bitbucket data
        try:
            stash_data = self.get_request(uri,
                                         issueId=issue_id,
                                         dataType="pullrequest",
                                         applicationType="stash",
                                         _=now_ms)
            if isinstance(stash_data, dict):
                if stash_data.get('errors'):
                    import sys
                    sys.stderr.write(f"Error in response: {stash_data.get('errors')}\n")
                detail = stash_data.get('detail')
                if isinstance(detail, list) and len(detail) > 0 and isinstance(detail[0], dict):
                    if detail[0].get('pullRequests'):
                        stash_count = len(detail[0]['pullRequests'])
                        pull_requests.extend(detail[0]['pullRequests'])
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            import sys
            sys.stderr.write(f"Unable to retrieve requested url {str(re)}\n")

        # Try GitHub data
        try:
            github_data = self.get_request(uri,
                                          issueId=issue_id,
                                          dataType="pullrequest",
                                          applicationType="githube",
                                          _=now_ms)
            if isinstance(github_data, dict):
                detail = github_data.get('detail')
                if isinstance(detail, list) and len(detail) > 0 and isinstance(detail[0], dict):
                    if detail[0].get('pullRequests'):
                        github_count = len(detail[0]['pullRequests'])
                        pull_requests.extend(detail[0]['pullRequests'])
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            import sys
            sys.stderr.write(f"Unable to retrieve requested url {str(re)}\n")

        unique = self._dedupe_pull_requests(pull_requests)
        raw_count = len(pull_requests)
        if raw_count != len(unique):
            import sys
            sys.stderr.write(
                f"Issue {issue_id}: dev-status PRs — stash={stash_count}, github={github_count}, "
                f"{raw_count} raw → {len(unique)} unique (collapsed {raw_count - len(unique)} duplicates)\n"
            )
        return unique

    @staticmethod
    def _dedupe_commits(commits: List[Any]) -> List[Any]:
        """Collapse duplicate commit entries (the same commit can appear under multiple
        branches/repos in dev-status). Dedupe by id, falling back to displayId then url,
        preserving order."""
        seen = set()
        unique = []
        for commit in commits:
            if not isinstance(commit, dict):
                continue
            key = commit.get('id') or commit.get('displayId') or commit.get('url')
            if key is None:
                unique.append(commit)  # can't key it; keep it rather than drop data
                continue
            if key in seen:
                continue
            seen.add(key)
            unique.append(commit)
        return unique

    def get_ticket_commit_info(self, issue_id: str) -> List[Any]:
        """Fetch commits linked to a ticket via the dev-status "Commits" panel.

        The dev-status detail endpoint has no "commit" dataType (GitHub Enterprise rejects it
        with "Unsupported type: commit"); commits are carried by the "repository" detail, which
        groups them under detail[0]['repositories'][*]['commits']. Each commit is tagged with
        its parent repository ({url, name}) so the per-commit diff can later be fetched by
        repository url (loose commits have no PR url)."""
        uri = f"https://{self.jira_server}/rest/dev-status/1.0/issue/detail"
        now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))

        commits = []
        for app_type in ("stash", "githube"):
            try:
                data = self.get_request(uri,
                                        issueId=issue_id,
                                        dataType="repository",
                                        applicationType=app_type,
                                        _=now_ms)
                if not isinstance(data, dict):
                    continue
                if data.get('errors'):
                    import sys
                    sys.stderr.write(f"Error in response: {data.get('errors')}\n")
                detail = data.get('detail')
                if isinstance(detail, list) and len(detail) > 0 and isinstance(detail[0], dict):
                    for repo in detail[0].get('repositories') or []:
                        if not isinstance(repo, dict):
                            continue
                        repo_ref = {'url': repo.get('url'), 'name': repo.get('name')}
                        for commit in repo.get('commits') or []:
                            if isinstance(commit, dict):
                                commit['_repository'] = repo_ref
                                commits.append(commit)
            except RESTException as re:
                if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                    raise
                import sys
                sys.stderr.write(f"Unable to retrieve requested url {str(re)}\n")

        unique = self._dedupe_commits(commits)
        raw_count = len(commits)
        if raw_count != len(unique):
            import sys
            sys.stderr.write(
                f"Issue {issue_id}: dev-status commits — {raw_count} raw → {len(unique)} unique "
                f"(collapsed {raw_count - len(unique)} duplicates)\n"
            )
        return unique

    def jql_summary_query(self, jql: str) -> Any:
        uri = f"https://{self.jira_server}/rest/api/2/search"
        return self.get_request(uri,
                               startAt="0",
                               maxResults=str(self.JQL_LIMIT),
                               jql=jql)

    def update_original_estimate(self, ticket_id: str, board_id: str, estimate_in_seconds: int) -> Any:
        uri = f"https://{self.jira_server}/rest/agile/1.0/issue/{ticket_id}/estimation"
        content = f'{{"value": "{estimate_in_seconds // 60}m"}}'
        return self.put_request(uri, content, boardId=board_id)
