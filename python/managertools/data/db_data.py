from enum import Enum
from typing import Any, Optional


class DBData(Enum):
    START_DATE = ("startDate", None, 0)
    END_DATE = ("endDate", None, 1)
    AUTHOR = ("author", None, 2)
    COMMENTS = ("comments", [], 3)
    OTHERS_COMMENTS = (None, [], 4)
    COMMIT_MESSAGES = (None, [], 5)

    def __init__(self, jira_field: Optional[str], default_value: Any, _unique_id: int):
        self.jira_field = jira_field
        self.default_value = default_value

    def get_jira_field(self) -> Optional[str]:
        return self.jira_field

    def get_default_value(self) -> Any:
        return self.default_value
