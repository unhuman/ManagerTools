import sys
from .source_control_rest import SourceControlREST


class NullREST(SourceControlREST):
    def __init__(self, source_control_type: str):
        super().__init__(None)
        sys.stderr.write(f"No {source_control_type} available - using empty responses.\n")
        sys.stderr.flush()

    def get_activities(self, pr_url: str):
        return []

    def get_commits(self, pr_url: str):
        return []

    def get_diffs(self, pr_url: str):
        return []

    def get_commit_diffs(self, pr_url: str, commit_sha: str):
        return []

    def map_user_to_jira_name(self, user_data):
        return None
