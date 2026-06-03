import argparse
import sys
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional

from managertools.util.command_line_helper import CommandLineHelper
from managertools.rest.jira_rest import JiraREST
from managertools.rest.bitbucket_rest import BitbucketREST
from managertools.rest.github_rest import GithubREST
from managertools.rest.null_rest import NullREST
from managertools.rest.exceptions import RESTException
from managertools.get_team_sprints import GetTeamSprints


def _sprint_limit_type(value: str) -> str:
    s = value.lower()
    if s in ('ytd', 'year'):
        return value
    if value.isdigit() and len(value) == 4 and int(value) >= 1000:
        return value
    if value.lstrip('-').isdigit():
        if int(value) <= 0:
            raise argparse.ArgumentTypeError(f"Limit must be positive, got: {value}")
        return value
    raise argparse.ArgumentTypeError(
        f"Invalid limit '{value}'. Use a positive integer, 'ytd', 'year', or a 4-digit year."
    )


class Mode(Enum):
    SCRUM = "SCRUM"
    KANBAN = "KANBAN"


class AbstractSprintReport(ABC):
    CONFIG_FILENAME = ".managerTools.cfg"

    def __init__(self, args: List[str]):
        self.args = args
        self.command_line_options = None
        self.command_line_helper = None
        self.jira_rest = None
        self.bitbucket_rest = None
        self.github_rest = None
        self.mode = Mode.SCRUM
        self.board_id = None
        self.team_name = None
        self.sprint_ids = []
        self.weeks = None
        self.kanban_start_date = None
        self.incomplete_sprints = []

    @abstractmethod
    def aggregate_data(self, team_name: Optional[str], board_id: Optional[str], mode: Mode, sprint_ids: List[str], weeks: Optional[int], kanban_start_date: Optional[str] = None):
        pass

    @abstractmethod
    def generate_output(self):
        pass

    def add_custom_command_line_options(self, parser: argparse.ArgumentParser):
        pass

    def validate_custom_command_line_options(self):
        pass

    @staticmethod
    def _resolve_limit_date_range(limit_str: str) -> tuple:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        s = limit_str.lower()
        if s == 'ytd':
            return datetime(now.year, 1, 1, tzinfo=timezone.utc), None
        elif s == 'year':
            try:
                start = now.replace(year=now.year - 1)
            except ValueError:  # Feb 29 on a leap year
                start = now.replace(year=now.year - 1, day=28)
            return start, None
        else:  # 4-digit year
            year = int(limit_str)
            return datetime(year, 1, 1, tzinfo=timezone.utc), datetime(year + 1, 1, 1, tzinfo=timezone.utc)

    def run(self):
        self.setup_run()

        import time
        start_time = time.time()

        self.aggregate_data(self.team_name, self.board_id, self.mode, self.sprint_ids, self.weeks, self.kanban_start_date)
        self.generate_output()

        if self.incomplete_sprints:
            _Y = "\033[93m"
            _R = "\033[0m"
            print(f"\n{_Y}*** WARNING: The following sprints/cycles have incomplete cached data ***{_R}", file=sys.stderr)
            for name in self.incomplete_sprints:
                print(f"{_Y}   - {name}{_R}", file=sys.stderr)
            print(f"{_Y}Re-run the same command to retry fetching the missing issues.{_R}", file=sys.stderr)
            print(f"{_Y}Only the previously-failed issues will be re-fetched.{_R}", file=sys.stderr)

        elapsed_seconds = int(time.time() - start_time)
        hours = elapsed_seconds // 3600
        minutes = (elapsed_seconds % 3600) // 60
        seconds = elapsed_seconds % 60
        print(f"Time to process: {hours:02d}:{minutes:02d}:{seconds:02d}")

    def setup_run(self):
        parser = argparse.ArgumentParser(
            description='Sprint Report Analysis',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument('-b', '--boardId', help='Sprint Board Id Number')
        parser.add_argument('-t', '--teamName', help='Sprint Team Name')
        parser.add_argument('-p', '--prompt', action='store_true', help='Prompt for team / timeframe info')
        parser.add_argument('-q', '--quietMode', action='store_true', help='Quiet mode (use default/stored values)')
        parser.add_argument('-l', '--limit', type=_sprint_limit_type, help='Recent sprints to process: a count, "ytd", "year", or a 4-digit year (e.g. "2025")')
        parser.add_argument('-s', '--sprintIds', help='Sprint Id Numbers (comma separated)')
        parser.add_argument('-ia', '--includeActive', action='store_true', help='Include current active sprint/cycle')

        self.add_custom_command_line_options(parser)

        self.command_line_options = parser.parse_args(self.args)

        # Validate conflicting options
        if self.command_line_options.prompt:
            conflicting = [self.command_line_options.boardId, self.command_line_options.teamName,
                          self.command_line_options.limit, self.command_line_options.sprintIds]
            if any(conflicting):
                print("Cannot mix prompt with board/team/limit/sprintIds")
                parser.print_help()
                sys.exit(1)

        try:
            self.validate_custom_command_line_options()
        except Exception as e:
            print(str(e))
            parser.print_help()
            sys.exit(1)

        self.setup_services()

        limit_raw = None
        if self.command_line_options.prompt:
            limit_raw = CommandLineHelper.prompt_number("Number of Sprints/Cycles")
        elif self.command_line_options.limit:
            limit_raw = self.command_line_options.limit

        if limit_raw is not None:
            limit_str = str(limit_raw).strip()
            if limit_str.lstrip('-').isdigit():
                # Numeric path — existing behaviour
                limit_int = int(limit_str)
                try:
                    sprint_data = GetTeamSprints(self.jira_rest).get_recent_sprints(
                        self.command_line_options.includeActive, self.board_id, limit_int)
                    self.sprint_ids = [str(s.get('id')) for s in sprint_data]
                except RESTException as re:
                    if re.status_code == 400:
                        self.weeks = limit_int   # Kanban fallback
                    else:
                        raise
            else:
                # Date-based path — new behaviour
                start_date, end_date = self._resolve_limit_date_range(limit_str)
                try:
                    sprint_data = GetTeamSprints(self.jira_rest).get_sprints_by_date_range(
                        self.command_line_options.includeActive, self.board_id, start_date, end_date)
                    self.sprint_ids = [str(s.get('id')) for s in sprint_data]
                except RESTException as re:
                    if re.status_code == 400:
                        # Kanban board: convert date range to cycle count
                        import math
                        from datetime import datetime, timezone
                        effective_end = end_date or datetime.now(timezone.utc)
                        cycle_length = getattr(self.command_line_options, 'kanbanCycleLength', 2)
                        total_weeks = (effective_end - start_date).days / 7
                        self.weeks = max(1, math.ceil(total_weeks / cycle_length))
                        self.kanban_start_date = start_date.isoformat()
                    else:
                        raise
        elif self.command_line_options.sprintIds:
            self.sprint_ids = self.command_line_options.sprintIds.split(',')

        # Validate that at least one sprint/cycle selection option was provided
        if not limit_raw and not self.sprint_ids:
            parser.error('Must provide one of: -l (limit), -s (sprintIds), or -p (prompt)')

        if self.weeks:
            self.mode = Mode.KANBAN

    def setup_services(self):
        self.command_line_helper = CommandLineHelper(self.CONFIG_FILENAME)
        if self.command_line_options.quietMode:
            self.command_line_helper.set_quiet_mode_no_prompts()

        jira_server = self.command_line_helper.get_jira_server()
        bitbucket_server = self.command_line_helper.get_bitbucket_server()

        auth_method = "c"  # Cookie-based auth (hardcoded for now)

        if auth_method == "p":
            username = self.command_line_helper.get_username()
            password = self.command_line_helper.get_password()

            self.jira_rest = JiraREST(jira_server, username, password)
            self.bitbucket_rest = (BitbucketREST(bitbucket_server, username, password)
                                  if password else NullREST("bitbucket"))
        elif auth_method == "c":
            jira_cookies = self.command_line_helper.get_jira_auth()
            bitbucket_cookies = self.command_line_helper.get_bitbucket_auth()

            self.jira_rest = JiraREST(jira_server, jira_cookies)
            self.bitbucket_rest = (BitbucketREST(bitbucket_server, bitbucket_cookies)
                                  if bitbucket_cookies else NullREST("bitbucket"))
        else:
            raise RuntimeError(f"Invalid auth method: {auth_method}")

        github_token = self.command_line_helper.get_github_token()
        config_mgr = self.command_line_helper.get_config_file_manager()
        graphql_points_reserved = (int(config_mgr.get_value("graphqlPointsReserved"))
                                 if config_mgr.contains_key("graphqlPointsReserved") else 5)
        self.github_rest = (GithubREST(github_token, graphql_points_reserved)
                           if github_token else NullREST("github"))

        self.team_name = self.command_line_options.teamName or None
        self.board_id = self.command_line_options.boardId or None

        if not self.board_id:
            if not self.team_name:
                self.team_name = self.command_line_helper.prompt_value("Team name")

            lookup_key = f"teamMappings.{self.team_name}"
            try:
                self.board_id = self.command_line_helper.get_config_file_manager().get_value(lookup_key)
            except RuntimeError:
                self.board_id = self.command_line_helper.prompt_number(f"Jira boardId for {self.team_name}")
                self.command_line_helper.get_config_file_manager().store_value(lookup_key, self.board_id)

        if not self.board_id:
            raise RuntimeError("boardId is required")
