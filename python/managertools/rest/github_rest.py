import re
import sys
from datetime import datetime
from http import HTTPStatus
from typing import Any, List, Optional

from .source_control_rest import SourceControlREST
from .auth_info import AuthInfo, AuthType
from .exceptions import RESTException
from ..data.user_activity import UserActivity


class GithubREST(SourceControlREST):
    STARTING_PAGE = "0"
    PAGE_SIZE_LIMIT = "100"
    JIRA_NAME_PATTERN = re.compile(r'\w.*')

    def __init__(self, bearer_token: str):
        super().__init__(AuthInfo(AuthType.Bearer, bearer_token))

    def api_convert(self, pr_url: str) -> str:
        return (pr_url
                .replace("://github.com/", "://api.github.com/repos/")
                .replace("/pull/", "/pulls/"))

    def get_activities(self, pr_url: str) -> List[Any]:
        errors = []
        activities_list = []

        try:
            activities_list.extend(self.get_comments(pr_url))
        except RESTException as re:
            if re.status_code not in [HTTPStatus.NOT_FOUND, HTTPStatus.FORBIDDEN]:
                raise
            errors.append(f"Comments ({re.status_code})")

        try:
            activities_list.extend(self.get_reviews(pr_url))
        except RESTException as re:
            if re.status_code not in [HTTPStatus.NOT_FOUND, HTTPStatus.FORBIDDEN]:
                raise
            errors.append(f"Reviews ({re.status_code})")

        if errors:
            error_message = f"Unable to retrieve activities: {', '.join(errors)}"

        return activities_list

    def get_comments(self, pr_url: str) -> List[Any]:
        uri = f"{pr_url}/comments"
        all_values = []
        page = 1

        while True:
            activities = self.get_request(uri,
                                         per_page=self.PAGE_SIZE_LIMIT,
                                         page=str(page))

            values = activities.get('values', []) if isinstance(activities, dict) else activities
            if not isinstance(values, list):
                values = list(values.values()) if hasattr(values, 'values') else []

            if not values:
                break

            all_values.extend(values)

            if len(values) < int(self.PAGE_SIZE_LIMIT):
                break

            page += 1

        values = list(reversed(all_values))

        comments = []
        for activity in values:
            if activity.get('user') is None:
                activity['user'] = {'name': 'unknown', 'displayName': 'unknown'}
            else:
                activity['user']['name'] = self.map_user_to_jira_name(activity['user'])

            created_at = activity.get('created_at')
            if created_at:
                activity['createdDate'] = int(datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp() * 1000)

            author_association = activity.get('author_association', '')
            if (author_association in ['CONTRIBUTOR', 'COLLABORATOR', 'FIRST_TIMER',
                                       'FIRST_TIME_CONTRIBUTOR', 'MEMBER', 'OWNER']
                    and activity.get('body') is not None):
                activity['action'] = UserActivity.COMMENTED.name
                activity['comment'] = {'text': activity['body']}
                comments.append(activity)

        return comments

    def get_reviews(self, pr_url: str) -> List[Any]:
        uri = f"{pr_url}/reviews"
        all_values = []
        page = 1

        while True:
            activities = self.get_request(uri,
                                         per_page=self.PAGE_SIZE_LIMIT,
                                         page=str(page))

            values = activities.get('values', []) if isinstance(activities, dict) else activities
            if not isinstance(values, list):
                values = list(values.values()) if hasattr(values, 'values') else []

            if not values:
                break

            all_values.extend(values)

            if len(values) < int(self.PAGE_SIZE_LIMIT):
                break

            page += 1

        values = list(reversed(all_values))

        reviews = []
        for activity in values:
            if activity.get('user') is None:
                activity['user'] = {'name': 'unknown', 'displayName': 'unknown'}
            else:
                activity['user']['name'] = self.map_user_to_jira_name(activity['user'])

            if activity.get('submitted_at') is None:
                continue

            submitted_at = activity.get('submitted_at')
            if submitted_at:
                activity['createdDate'] = int(datetime.fromisoformat(submitted_at.replace('Z', '+00:00')).timestamp() * 1000)

            author_association = activity.get('author_association', '')
            if (author_association in ['CONTRIBUTOR', 'COLLABORATOR', 'FIRST_TIMER',
                                       'FIRST_TIME_CONTRIBUTOR', 'MEMBER', 'OWNER']
                    and activity.get('body') is not None):
                state = activity.get('state', '')
                if state in ['APPROVED', 'DISMISSED']:
                    activity['action'] = UserActivity.DECLINED.name if state == 'DISMISSED' else state
                    reviews.append(activity)

        return reviews

    def get_commits(self, pr_url: str) -> Optional[Any]:
        uri = f"{pr_url}/commits"
        try:
            all_values = []
            page = 1

            while True:
                commits = self.get_request(uri,
                                          per_page=self.PAGE_SIZE_LIMIT,
                                          page=str(page))

                values = commits.get('values', []) if isinstance(commits, dict) else commits
                if not isinstance(values, list):
                    values = list(values.values()) if hasattr(values, 'values') else []

                if not values:
                    break

                all_values.extend(values)

                if len(values) < int(self.PAGE_SIZE_LIMIT):
                    break

                page += 1

            values = list(reversed(all_values))

            for commit in values:
                commit['id'] = commit.get('sha')

                commit_date = commit.get('commit', {}).get('committer', {}).get('date')
                if commit_date:
                    commit['committerTimestamp'] = int(datetime.fromisoformat(commit_date.replace('Z', '+00:00')).timestamp() * 1000)

                commit['message'] = commit.get('commit', {}).get('message')

                user_name = self.map_user_to_jira_name(commit.get('author'))
                if user_name is None:
                    committer = commit.get('committer')
                    user_name = committer.get('name') if committer else None

                if user_name is None:
                    nested_commit = commit.get('commit')
                    if nested_commit:
                        user_name = nested_commit.get('author', {}).get('name')

                try:
                    if commit.get('committer') is None:
                        commit['committer'] = {}
                    commit['committer']['name'] = self.map_user_to_jira_name(user_name)
                except Exception as e:
                    sys.stderr.write(f"{e}\n")

            return values
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            sys.stderr.write(f"Unable to retrieve commits {str(re)}\n")
            return None

    def get_diffs(self, pr_url: str) -> Optional[Any]:
        try:
            return self.get_request(pr_url)
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND, HTTPStatus.INTERNAL_SERVER_ERROR]:
                raise
            sys.stderr.write(f"Unable to retrieve diffs {str(re)}\n")
            return None

    def get_commit_diffs(self, commit_url: str, commit_sha: str) -> Optional[Any]:
        commit_ending = f"/commits/{commit_sha}"
        if not commit_url.endswith(commit_ending):
            raise RuntimeError(f"Invalid commitUrl {commit_url} not matching SHA: {commit_sha}")

        try:
            return self.get_request(commit_url)
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            sys.stderr.write(f"Unable to retrieve commit diffs {str(re)}\n")
            return None

    def map_user_to_jira_name(self, user_data: Any) -> Optional[str]:
        if user_data is None:
            return None

        if isinstance(user_data, str):
            return user_data

        jira_name = user_data.get('login') if isinstance(user_data, dict) else getattr(user_data, 'login', None)

        if jira_name is None:
            url = user_data.get('url') if isinstance(user_data, dict) else getattr(user_data, 'url', None)
            if url:
                jira_name = url.lower().split('/')[-1]

        if jira_name is None:
            return None

        if '_' not in jira_name:
            if len(jira_name) >= 39:
                user_type = user_data.get('type') if isinstance(user_data, dict) else getattr(user_data, 'type', None)
                sys.stderr.write(f"Could not identify user - expected '_' separator missing from {jira_name} (type: {user_type})\n")
                return None
        else:
            jira_name = jira_name[:jira_name.rfind('_')]

        return jira_name
