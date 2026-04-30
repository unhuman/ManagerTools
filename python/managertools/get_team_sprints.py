from datetime import datetime, timezone
from typing import List, Optional


class GetTeamSprints:
    def __init__(self, jira_rest=None):
        self.jira_rest = jira_rest

    @staticmethod
    def get_recent_sprints(
        jira_rest,
        include_active_sprint: bool,
        board_id: str,
        limit_count: Optional[int] = None
    ) -> List:
        data = jira_rest.get_sprints(board_id)

        # Filter out sprints from other boards
        data = [s for s in data if str(s.get('originBoardId')) == board_id]

        # Reverse to get most recent first
        data.reverse()

        # Remove active sprints if not included
        if not include_active_sprint:
            i = len(data) - 1
            while i >= 0:
                sprint = data[i]
                sprint_end_date = datetime.fromisoformat(
                    sprint.get('endDate', '').replace('Z', '+00:00')
                )
                sprint_active = sprint_end_date.timestamp() * 1000 > datetime.now(timezone.utc).timestamp() * 1000

                if sprint.get('state', '').upper() != 'CLOSED' and sprint_active:
                    data.pop(i)

                i -= 1

        # Apply limit
        if limit_count is not None:
            data = data[:min(limit_count, len(data))]

        # Reverse back to proper ordering
        data.reverse()

        return data
