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

    @abstractmethod
    def aggregate_data(self, team_name: Optional[str], board_id: Optional[str], mode: Mode, sprint_ids: List[str], weeks: Optional[int]):
        pass

    @abstractmethod
    def generate_output(self):
        pass

    def add_custom_command_line_options(self, parser: argparse.ArgumentParser):
        pass

    def validate_custom_command_line_options(self):
        pass

    def run(self):
        self.setup_run()

        import time
        start_time = time.time()

        self.aggregate_data(self.team_name, self.board_id, self.mode, self.sprint_ids, self.weeks)
        self.generate_output()

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
        parser.add_argument('-l', '--limit', type=int, help='Number of recent sprints to process')
        parser.add_argument('-s', '--sprintIds', help='Sprint Id Numbers (comma separated)')
        parser.add_argument('-w', '--weeks', type=int, help='Kanban weeks to process')
        parser.add_argument('-ia', '--includeActive', action='store_true', help='Include current active sprint')

        self.add_custom_command_line_options(parser)

        self.command_line_options = parser.parse_args(self.args)

        # Validate conflicting options
        if self.command_line_options.prompt:
            conflicting = [self.command_line_options.boardId, self.command_line_options.teamName,
                          self.command_line_options.limit, self.command_line_options.sprintIds,
                          self.command_line_options.weeks]
            if any(conflicting):
                print("Cannot mix prompt with board/team/limit/sprintIds/weeks")
                parser.print_help()
                sys.exit(1)

        try:
            self.validate_custom_command_line_options()
        except Exception as e:
            print(str(e))
            parser.print_help()
            sys.exit(1)

        self.setup_services()

        limit_val = None
        if self.command_line_options.prompt:
            limit_val = int(CommandLineHelper.prompt_number("Number of Sprints/Cycles"))
        elif self.command_line_options.limit:
            limit_val = self.command_line_options.limit
        elif self.command_line_options.weeks:
            self.weeks = self.command_line_options.weeks

        if limit_val:
            try:
                get_team_sprints = GetTeamSprints(self.jira_rest)
                sprint_data = get_team_sprints.get_recent_sprints(
                    self.command_line_options.includeActive,
                    self.board_id,
                    limit_val
                )
                self.sprint_ids = [str(sprint.get('id')) for sprint in sprint_data]
            except RESTException as re:
                if re.status_code == 400:  # BAD_REQUEST
                    if self.command_line_options.prompt:
                        self.weeks = limit_val
                    else:
                        raise RuntimeError("Notice: This is a Kanban board - no sprints found")
                else:
                    raise
        elif self.command_line_options.sprintIds:
            self.sprint_ids = self.command_line_options.sprintIds.split(',')

        if self.weeks:
            self.mode = Mode.KANBAN
            if not self.command_line_options.prompt:
                self.weeks = self.command_line_options.weeks

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
        self.github_rest = (GithubREST(self.command_line_helper, github_token)
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
