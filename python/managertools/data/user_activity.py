from enum import Enum
from typing import Any, Optional

from managertools.flexidb.flexidb import FlexiDB


class UserActivity(Enum):
    # Code Changes (values must be unique for all enum members to be distinct)
    PR_ADDED = (0, 0)
    PR_REMOVED = (0, 1)

    COMMITS = (0, 2)
    COMMIT_ADDED = (0, 3)
    COMMIT_REMOVED = (0, 4)

    # Activities on PRs
    APPROVED = (0, 5)
    COMMENTED = (0, 6)
    COMMENTED_ON_SELF = (0, 7)
    COMMENTED_ON_OTHERS = (0, 8)
    OTHERS_COMMENTED = (FlexiDB.EMPTY_INCREMENTOR, 9)
    DECLINED = (0, 10)
    MERGED = (0, 11)
    OPENED = (0, 12)
    RESCOPED = (0, 13)
    UNAPPROVED = (0, 14)
    UPDATED = (0, 15)

    def __init__(self, default_value: Any, _unused: int):
        self.default_value = default_value

    def get_default_value(self) -> Any:
        return self.default_value

    @classmethod
    def get_resolved_value(cls, desired_action: str) -> Optional['UserActivity']:
        try:
            return cls[desired_action]
        except (KeyError, Exception):
            import sys
            sys.stderr.write(f"Unknown action: {desired_action}\n")
            return None
