from datetime import datetime, timedelta, timezone
from typing import List, Any
from urllib.parse import urlencode, quote
from http import HTTPStatus

from .rest_service import RestService
from .auth_info import AuthInfo
from .exceptions import RESTException


class JiraREST(RestService):
    JQL_LIMIT = 250

    def __init__(self, jira_server: str, username_or_auth: str, password: str = None):
        if password is not None:
            auth_info = AuthInfo(username_or_auth, password)
        else:
            auth_type = AuthInfo.AuthType.Cookies if '=' in username_or_auth else AuthInfo.AuthType.Bearer
            from .auth_info import AuthType
            auth_info = AuthInfo(auth_type, username_or_auth)

        super().__init__(auth_info)
        self.jira_server = jira_server

    def get_sprints(self, board_id: str) -> List[Any]:
        start_at = 0
        values = []

        while True:
            uri = f"https://{self.jira_server}/rest/agile/1.0/board/{board_id}/sprint"
            response = self.get_request(uri,
                                       state="active,closed,future",
                                       startAt=str(start_at),
                                       _=str(int(datetime.now(timezone.utc).timestamp() * 1000)))

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
        response['startDate'] = f"{start_fmt} 00:00 AM"
        response['endDate'] = f"{end_fmt} 11:59 PM"
        return response

    def get_ticket(self, ticket_id: str) -> Any:
        uri = f"https://{self.jira_server}/rest/api/latest/issue/{ticket_id}"
        return self.get_request(uri, _=str(int(datetime.now(timezone.utc).timestamp() * 1000)))

    def get_ticket_pull_request_info(self, issue_id: str) -> List[Any]:
        uri = f"https://{self.jira_server}/rest/dev-status/1.0/issue/detail"
        now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))

        pull_requests = []

        # Try Stash/Bitbucket data
        try:
            stash_data = self.get_request(uri,
                                         issueId=issue_id,
                                         dataType="pullrequest",
                                         applicationType="stash",
                                         _=now_ms)
            if stash_data.get('errors'):
                import sys
                sys.stderr.write(f"Error in response: {stash_data.get('errors')}\n")
            elif stash_data.get('detail', {}).get('pullRequests'):
                pull_requests.extend(stash_data['detail']['pullRequests'][0])
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
            if github_data.get('detail', {}).get('pullRequests'):
                pull_requests.extend(github_data['detail']['pullRequests'][0])
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            import sys
            sys.stderr.write(f"Unable to retrieve requested url {str(re)}\n")

        return pull_requests

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
