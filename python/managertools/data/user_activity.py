from enum import Enum
from typing import Any, Optional

from managertools.flexidb.flexidb import FlexiDB


class UserActivity(Enum):
    # Code Changes
    PR_ADDED = 0
    PR_REMOVED = 0

    COMMITS = 0
    COMMIT_ADDED = 0
    COMMIT_REMOVED = 0

    # Activities on PRs
    APPROVED = 0
    COMMENTED = 0
    COMMENTED_ON_SELF = 0
    COMMENTED_ON_OTHERS = 0
    OTHERS_COMMENTED = FlexiDB.EMPTY_INCREMENTOR
    DECLINED = 0
    MERGED = 0
    OPENED = 0
    RESCOPED = 0
    UNAPPROVED = 0
    UPDATED = 0

    def __init__(self, default_value: Any):
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
