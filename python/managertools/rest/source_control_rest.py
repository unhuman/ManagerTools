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

    @abstractmethod
    def get_repo_commit_diffs(self, repo_url: str, commit_sha: str):
        """Fetch a single commit's diffs given the REPOSITORY url (not a PR url).

        Needed for commits sourced from the Jira dev-status commit view, which are not
        associated with any PR. Implementations should degrade gracefully (return None)
        on 403/404 rather than raising."""
        pass

    @abstractmethod
    def get_pr_created_ms(self, pr_url: str) -> int:
        pass

    @abstractmethod
    def get_pr_merged_ms(self, pr_url: str) -> int:
        pass

    def api_convert(self, pr_url: str) -> str:
        return pr_url

    @abstractmethod
    def map_user_to_jira_name(self, user_data) -> str:
        pass
