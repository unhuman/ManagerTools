from enum import Enum
from typing import Any, Optional


class DBData(Enum):
    START_DATE = ("startDate", None)
    END_DATE = ("endDate", None)
    AUTHOR = ("author", None)
    COMMENTS = ("comments", [])
    OTHERS_COMMENTS = (None, [])
    COMMIT_MESSAGES = (None, [])

    def __init__(self, jira_field: Optional[str], default_value: Any):
        self.jira_field = jira_field
        self.default_value = default_value

    def get_jira_field(self) -> Optional[str]:
        return self.jira_field

    def get_default_value(self) -> Any:
        return self.default_value
