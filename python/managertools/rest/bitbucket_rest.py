import re
import sys
from http import HTTPStatus
from typing import Any, Optional

from .source_control_rest import SourceControlREST
from .auth_info import AuthInfo, AuthType
from .exceptions import RESTException


class BitbucketREST(SourceControlREST):
    STARTING_PAGE = "0"
    PAGE_SIZE_LIMIT = "100"
    FIND_PROJECT_URL = re.compile(r"(.*)/pull-requests/[\d]+")

    def __init__(self, bitbucket_server: str, username_or_auth: str, password: str = None):
        if password is not None:
            auth_info = AuthInfo(username_or_auth, password)
        else:
            auth_type = AuthType.Cookies if '=' in username_or_auth else AuthType.Bearer
            auth_info = AuthInfo(auth_type, username_or_auth)

        super().__init__(auth_info)
        self.bitbucket_server = bitbucket_server

    def get_activities(self, pr_url: str) -> Any:
        uri = f"{pr_url}/activities"
        all_values = []
        start = self.STARTING_PAGE

        while True:
            response = self.get_request(uri,
                                       start=start,
                                       limit=self.PAGE_SIZE_LIMIT,
                                       markup="true")

            values = response.get('values', []) if isinstance(response, dict) else response
            if isinstance(values, list):
                all_values.extend(values)

            if isinstance(response, dict) and not response.get('isLastPage', True):
                start = str(response.get('nextPageStart', int(start) + len(values)))
            else:
                break

        return all_values

    def get_commits(self, pr_url: str) -> Optional[Any]:
        uri = f"{pr_url}/commits"
        try:
            all_values = []
            start = self.STARTING_PAGE

            while True:
                response = self.get_request(uri,
                                           start=start,
                                           limit=self.PAGE_SIZE_LIMIT)

                values = response.get('values', []) if isinstance(response, dict) else response
                if isinstance(values, list):
                    all_values.extend(values)

                if isinstance(response, dict) and not response.get('isLastPage', True):
                    start = str(response.get('nextPageStart', int(start) + len(values)))
                else:
                    break

            return all_values
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND]:
                raise
            sys.stderr.write(f"Unable to retrieve commits {str(re)}\n")
            return None

    def get_diffs(self, pr_url: str) -> Optional[Any]:
        uri = f"{pr_url}/diff"
        try:
            return self.get_request(uri,
                                   start=self.STARTING_PAGE,
                                   limit=self.PAGE_SIZE_LIMIT,
                                   contextLines="0",
                                   whitespace="ignore-all",
                                   ignoreComments="true")
        except RESTException as re:
            if re.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND, HTTPStatus.INTERNAL_SERVER_ERROR]:
                raise
            sys.stderr.write(f"Unable to retrieve diffs {str(re)}\n")
            return None

    def get_commit_diffs(self, pr_url: str, commit_sha: str) -> Any:
        matcher = self.FIND_PROJECT_URL.match(pr_url)
        if not matcher:
            raise RuntimeError(f"Couldn't extract Project URL from: {pr_url}")

        uri = f"{matcher.group(1)}/commits/{commit_sha}/diff"

        return self.get_request(uri,
                               start=self.STARTING_PAGE,
                               limit=self.PAGE_SIZE_LIMIT,
                               contextLines="0",
                               whitespace="ignore-all",
                               ignoreComments="true")

    def map_user_to_jira_name(self, user_data: Any) -> Optional[str]:
        if user_data is None:
            return None
        return user_data.get('name') if isinstance(user_data, dict) else getattr(user_data, 'name', None)
