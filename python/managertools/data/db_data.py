from enum import Enum
from typing import Any, Optional


class DBData(Enum):
    START_DATE = ("startDate", None, 0)
    END_DATE = ("endDate", None, 1)
    AUTHOR = ("author", None, 2)
    COMMENTS = ("comments", [], 3)
    OTHERS_COMMENTS = (None, [], 4)
    # Per-commit data: list of dicts, each {"message": str, "additions": int, "deletions": int,
    # "type": str, "sha": str}. "sha" is the commit id, used to de-dupe commit-view commits
    # against PR commits in WorkSource.BOTH mode. Future candidate fields: "committerTimestamp", "url".
    COMMIT_DATA = (None, [], 5)
    PR_TITLE = (None, None, 6)
    PR_TITLE_FOR_FILTER = (None, None, 7)

    def __init__(self, jira_field: Optional[str], default_value: Any, _unique_id: int):
        self.jira_field = jira_field
        self.default_value = default_value

    def get_jira_field(self) -> Optional[str]:
        return self.jira_field

    def get_default_value(self) -> Any:
        return self.default_value
