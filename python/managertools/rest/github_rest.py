import re
import sys
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List, Optional

from .source_control_rest import SourceControlREST
from .auth_info import AuthInfo, AuthType
from .exceptions import RESTException
from .github_graphql_client import GithubGraphQLClient
from ..data.user_activity import UserActivity


def _iso_to_ms(iso_str: Optional[str]) -> int:
    if not iso_str:
        return 0
    return int(datetime.fromisoformat(iso_str.replace('Z', '+00:00')).timestamp() * 1000)


class GithubREST(SourceControlREST):
    STARTING_PAGE = "0"
    PAGE_SIZE_LIMIT = "100"
    JIRA_NAME_PATTERN = re.compile(r'\w.*')

    def __init__(self, bearer_token: str, graphql_points_reserved: int = 5):
        super().__init__(AuthInfo(AuthType.Bearer, bearer_token))
        self._graphql_client = GithubGraphQLClient(bearer_token, graphql_points_reserved)

    def set_pr_progress(self, index: int, total: int) -> None:
        """Set current PR progress for debug logging."""
        self._graphql_client.set_pr_progress(index, total)

    def set_commit_page_size_retry_mode(self, enabled: bool) -> None:
        """Switch the GraphQL commit page-size ladder to the small retry strategy."""
        self._graphql_client.set_retry_mode(enabled)

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

            if activity.get('body') is not None:
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

            if activity.get('body') is not None:
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
                    user_name = self.map_user_to_jira_name(committer) if committer else None

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


    def get_pull_request_metadata(self, pr_url: str) -> Dict[str, Any]:
        """Cheap PR metadata for collection-time down-merge detection, avoiding the full
        commit pagination. Returns {title, baseRefName, headRefName, commits_total, merged}."""
        m = re.search(r'/repos/([^/]+)/([^/]+)/pulls/(\d+)', pr_url)
        if not m:
            raise ValueError(f"Cannot parse owner/repo/number from PR URL: {pr_url}")
        owner, repo, pr_number = m.group(1), m.group(2), int(m.group(3))
        return self._graphql_client.get_pull_request_metadata(owner, repo, pr_number)

    def get_pull_request_full(self, pr_url: str) -> Dict[str, Any]:
        """Fetch all PR data via a single paginated GraphQL query.

        Returns a dict with keys:
          - commits: list of normalized commit dicts (keys: id, committerTimestamp, message,
                     committer.name, url, additions, deletions, changedFilesIfAvailable)
          - activities: list of normalized activity dicts (keys: user.name, action,
                        comment.text, createdDate)
          - created_ms: PR created-at as int milliseconds
          - merged_ms:  PR merged-at as int milliseconds (0 if not merged)
        """
        m = re.search(r'/repos/([^/]+)/([^/]+)/pulls/(\d+)', pr_url)
        if not m:
            raise ValueError(f"Cannot parse owner/repo/number from PR URL: {pr_url}")
        owner, repo, pr_number = m.group(1), m.group(2), int(m.group(3))

        raw = self._graphql_client.get_pull_request_data(owner, repo, pr_number)
        pr_meta = raw["pr"]

        # PR timestamps
        created_ms = _iso_to_ms(pr_meta.get("createdAt"))
        merged_ms = _iso_to_ms(pr_meta.get("mergedAt"))

        # Normalize commits (GraphQL returns newest-first; reverse to match get_commits())
        commit_base = f"https://api.github.com/repos/{owner}/{repo}/commits"
        commits = []
        for c in reversed(raw["commits"]):
            sha = c.get("oid", "")
            committer_login = ((c.get("committer") or {}).get("user") or {}).get("login")
            author_login = ((c.get("author") or {}).get("user") or {}).get("login")
            user_name = (self.map_user_to_jira_name({"login": committer_login}) if committer_login else None
                         or self.map_user_to_jira_name({"login": author_login}) if author_login else None
                         or (c.get("author") or {}).get("name"))
            commits.append({
                "id": sha,
                "committerTimestamp": _iso_to_ms(c.get("committedDate")),
                "message": c.get("message", ""),
                "committer": {"name": user_name},
                "url": f"{commit_base}/{sha}",
                # GraphQL may return additions/deletions as null (SERVICE_UNAVAILABLE for large
                # commits); `or 0` coerces both explicit null and missing keys to 0.
                "additions": c.get("additions") or 0,
                "deletions": c.get("deletions") or 0,
                "changedFilesIfAvailable": c.get("changedFilesIfAvailable", 0),
                # Parent count drives merge detection (a merge commit has 2+ parents).
                "parents_count": (c.get("parents") or {}).get("totalCount"),
            })

        # Normalize activities: general comments + inline review-thread comments + reviews
        activities = []

        for node in raw["comments"] + raw["review_thread_comments"]:
            if node.get("body") is None:
                continue
            login = (node.get("author") or {}).get("login")
            jira_name = self.map_user_to_jira_name({"login": login}) if login else None
            activities.append({
                "user": {"name": jira_name or "unknown"},
                "action": UserActivity.COMMENTED.name,
                "comment": {"text": node.get("body", "")},
                "createdDate": _iso_to_ms(node.get("createdAt")),
            })

        for review in pr_meta.get("reviews", []):
            state = review.get("state", "")
            if state not in ("APPROVED", "DISMISSED"):
                continue
            if review.get("submittedAt") is None or review.get("body") is None:
                continue
            login = (review.get("author") or {}).get("login")
            jira_name = self.map_user_to_jira_name({"login": login}) if login else None
            activities.append({
                "user": {"name": jira_name or "unknown"},
                "action": "APPROVED" if state == "APPROVED" else UserActivity.DECLINED.name,
                "createdDate": _iso_to_ms(review.get("submittedAt")),
            })

        # Sort most-recent-first to match the ordering returned by get_activities() REST
        activities.sort(key=lambda a: a.get("createdDate", 0), reverse=True)

        return {
            "commits": commits,
            "activities": activities,
            "created_ms": created_ms,
            "merged_ms": merged_ms,
        }

    def get_pr_created_ms(self, pr_url: str) -> int:
        try:
            pr = self.get_request(pr_url)
            if not isinstance(pr, dict):
                return 0
            created_at = pr.get('created_at', '')
            if created_at:
                return int(datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp() * 1000)
            return 0
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            sys.stderr.write(f"Unable to retrieve PR created date {str(re)}\n")
            return 0

    def get_pr_merged_ms(self, pr_url: str) -> int:
        try:
            pr = self.get_request(pr_url)
            if not isinstance(pr, dict):
                return 0
            merged_at = pr.get('merged_at', '')
            if merged_at:
                return int(datetime.fromisoformat(merged_at.replace('Z', '+00:00')).timestamp() * 1000)
            return 0
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            sys.stderr.write(f"Unable to retrieve PR merged date {str(re)}\n")
            return 0

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
