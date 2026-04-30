import sys
from datetime import datetime, timezone
from typing import List, Optional

from managertools.util.command_line_helper import CommandLineHelper
from managertools.rest.jira_rest import JiraREST


class GetTeamSprints:
    def __init__(self, jira_rest: Optional[JiraREST] = None):
        self.jira_rest = jira_rest

    def get_recent_sprints(self, include_active_sprint: bool, board_id: str, limit_count: Optional[int]) -> List[dict]:
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

        data = filtered_data

        if limit_count is not None:
            data = data[:min(limit_count, len(data))]

        # Flip back to original order
        data.reverse()

        return data

    def run(self, args: List[str]):
        import argparse
        parser = argparse.ArgumentParser(description='Get Team Sprints')
        parser.add_argument('-b', '--boardId', required=True, help='Sprint Board Id Number')
        parser.add_argument('-l', '--limit', type=int, help='Limit of count to get')
        parser.add_argument('-q', '--quietMode', action='store_true', help='Quiet mode')
        parser.add_argument('-ia', '--includeActive', action='store_true', help='Include current active sprint')
        parser.add_argument('-h', '--help', action='store_true', help='Show help')

        options = parser.parse_args(args)

        if options.help:
            parser.print_help()
            return

        command_line_helper = CommandLineHelper('.managerTools.cfg')
        if options.quietMode:
            command_line_helper.set_quiet_mode_no_prompts()

        jira_server = command_line_helper.get_jira_server()
        jira_auth = command_line_helper.get_jira_auth()

        self.jira_rest = JiraREST(jira_server, jira_auth)

        data = self.get_recent_sprints(options.includeActive, options.boardId, options.limit)
        for sprint in data:
            print(f"{sprint.get('id')}: {sprint.get('name')}")


if __name__ == '__main__':
    get_team_sprints = GetTeamSprints()
    get_team_sprints.run(sys.argv[1:])
