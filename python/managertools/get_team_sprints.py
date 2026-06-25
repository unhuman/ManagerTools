import sys
from datetime import datetime, timezone
from typing import List, Optional

from managertools.util.command_line_helper import CommandLineHelper
from managertools.util.log_util import debug_print
from managertools.rest.jira_rest import JiraREST


class GetTeamSprints:
    def __init__(self, jira_rest: Optional[JiraREST] = None):
        self.jira_rest = jira_rest

    def _fetch_and_filter_sprints(self, include_active_sprint: bool, board_id: str) -> List[dict]:
        data = self.jira_rest.get_sprints(board_id)

        # Filter out sprints not from this board
        data = [sprint for sprint in data if str(sprint.get('originBoardId', '')) == board_id]

        # Reverse to get most recent first
        data.reverse()

        # Filter out active sprints if not desired
        filtered_data = []
        for sprint in data:
            end_date_str = sprint.get('endDate')
            if end_date_str:
                # Parse ISO 8601 datetime
                end_datetime = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                sprint_active = end_datetime.timestamp() * 1000 > datetime.now(timezone.utc).timestamp() * 1000

                if include_active_sprint or sprint.get('state', '').upper() == 'CLOSED' or not sprint_active:
                    filtered_data.append(sprint)
            else:
                filtered_data.append(sprint)

        return filtered_data

    def get_effective_start_map(self, include_active_sprint: bool, board_id: str) -> dict:
        """Map each board sprint id -> predecessor sprint's end time in epoch ms.

        Computed over the full board-wide sprint sequence (every sprint, not a -l N slice), so a
        given sprint's predecessor — and therefore its back-filled window start — is identical
        regardless of how many sprints a run requested (deterministic / cache-safe). Used to close
        the ~1-day gaps Jira leaves between a sprint's start and the previous sprint's end, where
        commits/activities would otherwise fall outside every window.

        The absolute first sprint of the board has no predecessor and gets no entry (callers fall
        back to start-of-day). _fetch_and_filter_sprints already fetches all board sprints, so the
        predecessor of even the first reported sprint is available without an extra fetch.
        """
        data = self._fetch_and_filter_sprints(include_active_sprint, board_id)
        # _fetch_and_filter_sprints returns most-recent-first; sort ascending by startDate.
        ascending = sorted((s for s in data if s.get('startDate') and s.get('endDate')),
                           key=lambda x: x.get('startDate', ''))
        effective_start_by_id = {}
        for i in range(1, len(ascending)):
            prev = ascending[i - 1]
            curr = ascending[i]
            prev_end_str = prev.get('endDate')
            prev_end = datetime.fromisoformat(prev_end_str.replace('Z', '+00:00'))
            sprint_id = curr.get('id')
            if sprint_id is not None:
                effective_start_by_id[str(sprint_id)] = prev_end.timestamp() * 1000
                # Log the predecessor that supplies each window start, including the gap being
                # closed. For the earliest sprint in a -l N run this predecessor is an "extra"
                # sprint outside the reported range, pulled in only for its end timestamp.
                gap = datetime.fromisoformat(curr.get('startDate').replace('Z', '+00:00')) - prev_end
                debug_print(f"sprint window back-fill: '{curr.get('name')}' (id {sprint_id}) start "
                            f"<- predecessor '{prev.get('name')}' (id {prev.get('id')}) end {prev_end_str} "
                            f"[closing {gap} gap]")
        return effective_start_by_id

    def get_recent_sprints(self, include_active_sprint: bool, board_id: str, limit_count: Optional[int]) -> List[dict]:
        data = self._fetch_and_filter_sprints(include_active_sprint, board_id)

        if limit_count is not None:
            data = data[:min(limit_count, len(data))]

        # Flip back to original order
        data.reverse()

        return data

    def get_sprints_by_date_range(self, include_active_sprint: bool, board_id: str,
                                   start_after: datetime,
                                   end_before: Optional[datetime]) -> List[dict]:
        data = self._fetch_and_filter_sprints(include_active_sprint, board_id)

        result = []
        for sprint in data:
            start_str = sprint.get('startDate')
            if not start_str:
                continue
            sprint_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            if sprint_start < start_after:
                continue
            if end_before is not None and sprint_start >= end_before:
                continue
            result.append(sprint)

        # Sort by startDate (ascending)
        result.sort(key=lambda x: x.get('startDate', ''))
        return result

    def run(self, args: List[str]):
        import argparse
        parser = argparse.ArgumentParser(description='Get Team Sprints')
        parser.add_argument('-b', '--boardId', required=True, help='Sprint Board Id Number')
        parser.add_argument('-l', '--limit', help='Limit count, "ytd", "year", or 4-digit year')
        parser.add_argument('-q', '--quietMode', action='store_true', help='Quiet mode')
        parser.add_argument('-ia', '--includeActive', action='store_true', help='Include current active sprint')

        options = parser.parse_args(args)

        command_line_helper = CommandLineHelper('.managerTools.cfg')
        if options.quietMode:
            command_line_helper.set_quiet_mode_no_prompts()

        jira_server = command_line_helper.get_jira_server()
        jira_auth = command_line_helper.get_jira_auth()

        self.jira_rest = JiraREST(jira_server, jira_auth)

        if options.limit is not None:
            limit_str = options.limit.strip()
            s = limit_str.lower()

            # Check for date-based limits first
            if s in ('ytd', 'year'):
                now = datetime.now(timezone.utc)
                if s == 'ytd':
                    start_date, end_date = datetime(now.year, 1, 1, tzinfo=timezone.utc), None
                else:  # year
                    try:
                        start_date = now.replace(year=now.year - 1)
                    except ValueError:  # Feb 29 on a leap year
                        start_date = now.replace(year=now.year - 1, day=28)
                    end_date = None
                data = self.get_sprints_by_date_range(options.includeActive, options.boardId, start_date, end_date)
            elif limit_str.isdigit() and len(limit_str) == 4 and int(limit_str) >= 1000:
                # 4-digit year
                year = int(limit_str)
                start_date = datetime(year, 1, 1, tzinfo=timezone.utc)
                end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
                data = self.get_sprints_by_date_range(options.includeActive, options.boardId, start_date, end_date)
            elif limit_str.lstrip('-').isdigit():
                # Numeric limit (sprint count)
                data = self.get_recent_sprints(options.includeActive, options.boardId, int(limit_str))
            else:
                raise ValueError(f"Invalid limit '{limit_str}'. Use a positive integer, 'ytd', 'year', or a 4-digit year.")
        else:
            # No limit, get all sprints
            data = self.get_sprints_by_date_range(options.includeActive, options.boardId,
                                                   datetime(1970, 1, 1, tzinfo=timezone.utc), None)

        for sprint in data:
            print(f"{sprint.get('id')}: {sprint.get('name')}")


if __name__ == '__main__':
    get_team_sprints = GetTeamSprints()
    get_team_sprints.run(sys.argv[1:])
