from enum import Enum
from typing import Optional


class DBIndexData(Enum):
    SPRINT = "sprint"
    TICKET = "ticket"
    PR_ID = "prId"
    PR_STATUS = "prStatus"
    USER = "user"

    def __init__(self, jira_field: str):
        self.jira_field = jira_field

    def get_jira_field(self) -> str:
        return self.jira_field
