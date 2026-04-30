import sys
import time
import argparse
from abc import ABC, abstractmethod
from typing import List, Optional
from .rest.jira_rest import JiraREST
from .rest.null_rest import NullREST
from .rest.bitbucket_rest import BitbucketREST
from .rest.github_rest import GithubREST
from .rest.source_control_rest import SourceControlREST
from .rest.exceptions import RESTException
from .util.command_line_helper import CommandLineHelper
from .get_team_sprints import GetTeamSprints


class Mode:
    SCRUM = "SCRUM"
    KANBAN = "KANBAN"


class AbstractSprintReport(ABC):
    CONFIG_FILENAME = ".managerTools.cfg"

    def __init__(self, args: List[str] = None):
        self.command_line_helper = None
        self.jira_rest = None
        self.bitbucket_rest = None
        self.github_rest = None
        self.command_line_options = None
        self.mode = Mode.SCRUM
        self.board_id = None
        self.team_name = None
        self.sprint_ids = None
        self.weeks = None
        self.args = args or sys.argv[1:]

    @abstractmethod
    def aggregate_data(self, team_name: str, board_id: str, mode: str, sprint_ids: List[str], weeks: Optional[int]):
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

        start_time = time.time()

        self.aggregate_data(self.team_name, self.board_id, self.mode, self.sprint_ids, self.weeks)
        self.generate_output()

        elapsed = int((time.time() - start_time))
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        seconds = elapsed % 60
        print(f"Time to process: {hours:02d}:{minutes:02d}:{seconds:02d}")

    def setup_run(self):
        parser = argparse.ArgumentParser(
            prog='SprintReport',
            description='Sprint Report Analysis',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument('-h', '--help', action='help', help='Shows useful information')

        # Board or Team group (mutually exclusive)
        board_team_group = parser.add_mutually_exclusive_group(required=False)
        board_team_group.add_argument('-b', '--boardId', help='Sprint Board Id Number')
        board_team_group.add_argument('-t', '--teamName', help='Sprint Team Name')
        board_team_group.add_argument('-p', '--prompt', action='store_true', help='Prompt for team / timeframe info')

        parser.add_argument('-q', '--quietMode', action='store_true', help='Quiet mode (use default/stored values without prompt)')

        # Scrum or Kanban group (mutually exclusive)
        scrum_kanban_group = parser.add_mutually_exclusive_group(required=False)
        scrum_kanban_group.add_argument('-l', '--limit', type=int, help='Number of recent sprints to process')
        scrum_kanban_group.add_argument('-s', '--sprintIds', help='Sprint Id Numbers (comma separated)')
        scrum_kanban_group.add_argument('-w', '--weeks', type=int, help='Kanban weeks to process')

        parser.add_argument('-ia', '--includeActive', action='store_true', help='Include current active sprint in results')

        # Allow custom options
        self.add_custom_command_line_options(parser)

        self.command_line_options = parser.parse_args(self.args)

        if self.command_line_options.help:
            parser.print_help()
            sys.exit(0)

        # Validate that prompt is not mixed with other options
        if self.command_line_options.prompt:
            if (self.command_line_options.boardId or self.command_line_options.teamName or
                    self.command_line_options.limit or self.command_line_options.sprintIds or
                    self.command_line_options.weeks):
                print("Cannot mix prompt with board/team/limit/sprintIds/weeks")
                parser.print_help()
                sys.exit(-1)

        try:
            self.validate_custom_command_line_options()
        except Exception as e:
            print(str(e))
            parser.print_help()
            sys.exit(-1)

        self.setup_services()

        limit = None
        if self.command_line_options.prompt:
            limit = int(CommandLineHelper.prompt_number("Number of Sprints/Cycles"))
        elif self.command_line_options.limit:
            limit = self.command_line_options.limit
        elif self.command_line_options.weeks:
            self.weeks = self.command_line_options.weeks

        if limit:
            try:
                get_team_sprints = GetTeamSprints(self.jira_rest)
                sprint_data = GetTeamSprints.get_recent_sprints(
                    self.jira_rest,
                    self.command_line_options.includeActive,
                    self.board_id,
                    limit
                )
                self.sprint_ids = [str(sprint.get('id')) for sprint in sprint_data]
            except RESTException as re:
                if re.status_code == 400:  # Bad request
                    if self.command_line_options.prompt:
                        self.weeks = limit
                    else:
                        raise RuntimeError("Notice: This is a Kanban board - no sprints found")
                else:
                    raise
        elif self.command_line_options.sprintIds:
            self.sprint_ids = [s.strip() for s in self.command_line_options.sprintIds.split(',')]

        if self.weeks:
            self.mode = Mode.KANBAN
            if not self.command_line_options.prompt:
                self.weeks = self.command_line_options.weeks

    def set_command_line_options(self, options):
        self.command_line_options = options

    def get_command_line_options(self):
        return self.command_line_options

    def setup_services(self):
        self.command_line_helper = CommandLineHelper(self.CONFIG_FILENAME)
        if self.command_line_options.quietMode:
            CommandLineHelper.set_quiet_mode_no_prompts()

        # Get server information
        jira_server = self.command_line_helper.get_jira_server()
        bitbucket_server = self.command_line_helper.get_bitbucket_server()

        # Get auth method (default to cookie)
        auth_method = "c"  # Default to cookie-based auth

        if auth_method == "p":
            username = self.command_line_helper.get_username()
            password = self.command_line_helper.get_password()

            self.jira_rest = JiraREST(jira_server, username, password)
            self.bitbucket_rest = (
                BitbucketREST(bitbucket_server, username, password)
                if password
                else NullREST("bitbucket")
            )
        elif auth_method == "c":
            jira_cookies = self.command_line_helper.get_jira_auth()
            bitbucket_cookies = self.command_line_helper.get_bitbucket_auth()

            self.jira_rest = JiraREST(jira_server, jira_cookies)
            self.bitbucket_rest = (
                BitbucketREST(bitbucket_server, bitbucket_cookies)
                if bitbucket_cookies
                else NullREST("bitbucket")
            )
        else:
            raise RuntimeError(f"Invalid auth method: {auth_method}")

        # GitHub always uses token-based auth
        github_token = self.command_line_helper.get_github_token()
        self.github_rest = (
            GithubREST(github_token)
            if github_token
            else NullREST("github")
        )

        # Extract team name and boardId
        self.team_name = self.command_line_options.teamName if self.command_line_options.teamName else None
        self.board_id = self.command_line_options.boardId if self.command_line_options.boardId else None

        if self.board_id is None:
            if self.team_name is None:
                self.team_name = CommandLineHelper.prompt_value("Team name")

            # Look up boardId from team name
            lookup_value = f"teamMappings.{self.team_name}"
            try:
                self.board_id = self.command_line_helper.get_config_file_manager().get_value(lookup_value)
            except RuntimeError:
                self.board_id = CommandLineHelper.prompt_number(f"Jira boardId for {self.team_name}")
                self.command_line_helper.get_config_file_manager().update_value(lookup_value, self.board_id)

        if self.board_id is None:
            raise RuntimeError("boardId is required")
