from abc import ABC, abstractmethod

from .rest_service import RestService
from .auth_info import AuthInfo


class SourceControlREST(RestService, ABC):
    def __init__(self, auth_info: AuthInfo):
        super().__init__(auth_info)

    @abstractmethod
    def get_activities(self, pr_url: str):
        pass

    @abstractmethod
    def get_commits(self, pr_url: str):
        pass

    @abstractmethod
    def get_diffs(self, pr_url: str):
        pass

    @abstractmethod
    def get_commit_diffs(self, pr_url: str, commit_sha: str):
        pass

    def api_convert(self, pr_url: str) -> str:
        return pr_url

    @abstractmethod
    def map_user_to_jira_name(self, user_data) -> str:
        pass
