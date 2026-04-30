import re
import sys
import getpass
import os
from typing import List, Optional
from .config_file_manager import ConfigFileManager


class TextSecurity:
    NONE = "NONE"
    PASSWORD = "PASSWORD"
    MASK = "MASK"


class CommandLineHelper:
    AUTH_PATTERN = re.compile(r"(p|password|pw|c|cookie)")
    USERS_MATCH_PATTERN = re.compile(r"^\*{1,2}$|^[a-zA-Z0-9.]+([,\s]+[a-zA-Z0-9.-]*)*$")
    DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
    NUMBERS_REQUIRED_PATTERN = re.compile(r"\d+")
    DATA_REQUIRED_PATTERN = re.compile(r"\S+")
    FQDN_PATTERN = re.compile(r"(\w+\.){2,}\w+")
    ANY_MATCH_PATTERN = re.compile(r".*")
    ANY_MATCH_PATTERN_LONG = re.compile(r".{32,}")

    quiet_mode = False

    def __init__(self, config_filename: Optional[str] = None):
        self.config_file_manager = None
        if config_filename:
            self.config_file_manager = ConfigFileManager(config_filename)

    @classmethod
    def set_quiet_mode_no_prompts(cls):
        cls.quiet_mode = True

    def get_config_file_manager(self) -> Optional[ConfigFileManager]:
        return self.config_file_manager

    def get_auth_method(self) -> str:
        result = self._prompt_and_store(
            f"Auth Method {self.AUTH_PATTERN.pattern}",
            TextSecurity.NONE,
            self.AUTH_PATTERN,
            "authMethod",
            True,
            "password"
        )
        return result[0]

    def get_username(self) -> str:
        username = self._prompt_and_store(
            f"Username (or press enter for {os.getenv('USER')})",
            TextSecurity.NONE,
            self.ANY_MATCH_PATTERN,
            None,
            False
        )
        if not username:
            username = os.getenv('USER', '')
        return username

    def get_password(self) -> str:
        return self._prompt_and_store(
            "Password",
            TextSecurity.PASSWORD,
            self.ANY_MATCH_PATTERN,
            None,
            False
        )

    def get_sprint_team(self) -> str:
        return self._prompt_and_store(
            "Sprint Team name",
            TextSecurity.NONE,
            self.ANY_MATCH_PATTERN,
            "sprintTeam",
            True
        )

    def get_sprint_team_board_id(self) -> str:
        if not self.config_file_manager or not self.config_file_manager.contains_key("sprintTeam"):
            raise RuntimeError("Need sprintTeam already found")

        sprint_team = self.config_file_manager.get_value("sprintTeam")
        return self._prompt_and_store(
            f"Board Id for {sprint_team}",
            TextSecurity.NONE,
            self.NUMBERS_REQUIRED_PATTERN,
            f"{sprint_team}-boardId",
            True
        )

    def get_jira_server(self) -> str:
        return self._prompt_and_store(
            "Jira Server (jira.x.com)",
            TextSecurity.NONE,
            self.FQDN_PATTERN,
            "jiraServer",
            False
        )

    def get_bitbucket_server(self) -> str:
        return self._prompt_and_store(
            "Bitbucket Server (bitbucket.x.com)",
            TextSecurity.NONE,
            self.FQDN_PATTERN,
            "bitbucketServer",
            False
        )

    def get_jira_auth(self) -> str:
        return self._prompt_and_store(
            "Jira Access Token or Cookies (DevTools/Request/Cookie)",
            TextSecurity.MASK,
            self.ANY_MATCH_PATTERN_LONG,
            "jiraAuth",
            True
        )

    def get_bitbucket_auth(self) -> str:
        return self._prompt_and_store(
            "Bitbucket Access Token or Cookies (DevTools/Request/Cookie)",
            TextSecurity.MASK,
            self.ANY_MATCH_PATTERN_LONG,
            "bitbucketAuth",
            True
        )

    def get_github_token(self) -> str:
        return self._prompt_and_store(
            "Github Token",
            TextSecurity.MASK,
            self.ANY_MATCH_PATTERN_LONG,
            "githubToken",
            True
        )

    def get_team_board_users(self, team_name: Optional[str], board_id: str) -> List[str]:
        if team_name:
            team_name = team_name.replace(".", "-")
            config_key = f"teamUsers.{team_name}"
            type_indicator = "team"
            item = team_name
        else:
            config_key = f"{board_id}-users"
            type_indicator = "board"
            item = board_id

        result = self._prompt_and_store(
            f"Team users (optional, comma/space separated, * for all authors, ** for all users) for {type_indicator}: {item}",
            TextSecurity.NONE,
            self.USERS_MATCH_PATTERN,
            config_key,
            True,
            "*"
        )
        users = [u.strip() for u in result.split(",") if u.strip()]
        return users

    def get_date_check(self, prompt_description: str, config_key: str) -> str:
        return self._prompt_and_store(
            f"Enter date {prompt_description} (yyyy-mm-dd)",
            TextSecurity.NONE,
            self.DATE_PATTERN,
            config_key,
            True
        )

    @staticmethod
    def prompt(text: str, pattern: Optional[re.Pattern] = None) -> str:
        if pattern is None:
            pattern = CommandLineHelper.ANY_MATCH_PATTERN
        return CommandLineHelper._perform_prompt(text, TextSecurity.NONE, pattern)

    @staticmethod
    def prompt_number(text: str) -> str:
        return CommandLineHelper._perform_prompt(text, TextSecurity.NONE, CommandLineHelper.NUMBERS_REQUIRED_PATTERN)

    @staticmethod
    def prompt_value(text: str) -> str:
        return CommandLineHelper._perform_prompt(text, TextSecurity.NONE, CommandLineHelper.DATA_REQUIRED_PATTERN)

    @staticmethod
    def _perform_prompt(text: str, text_security: str, validation_pattern: re.Pattern) -> str:
        if CommandLineHelper.quiet_mode:
            return ""

        while True:
            if text_security == TextSecurity.PASSWORD:
                input_value = getpass.getpass(f"{text}: ")
            else:
                input_value = input(f"{text}: ").strip()

            if validation_pattern.match(input_value):
                return input_value

            print(f"Input must match regular expression: {validation_pattern.pattern}")

    def _prompt_and_store(
        self,
        text: str,
        text_security: str,
        validation_pattern: re.Pattern,
        default_value_config_key: Optional[str],
        prompt_for_existing_value: bool,
        default_value: Optional[str] = None
    ) -> str:
        use_default_value = default_value

        if default_value_config_key and self.config_file_manager:
            if self.config_file_manager.contains_key(default_value_config_key):
                config_value = self.config_file_manager.get_value(default_value_config_key)
                if config_value:
                    use_default_value = config_value
                if not prompt_for_existing_value:
                    print(f"Found {default_value_config_key} configuration value - {use_default_value}")
                    return use_default_value

        if use_default_value:
            masked = "****" if text_security != TextSecurity.NONE else use_default_value
            prompt_text = f"{text} (press return to use default value: {masked})"
        else:
            prompt_text = text

        while True:
            input_value = self._perform_prompt(prompt_text, text_security, self.ANY_MATCH_PATTERN).strip()

            if not input_value and use_default_value:
                return use_default_value
            elif validation_pattern.match(input_value):
                if default_value_config_key and self.config_file_manager:
                    self.config_file_manager.update_value(default_value_config_key, input_value)
                return input_value

            if self.quiet_mode:
                raise RuntimeError(f"Couldn't quietly get text: {text}")

            print(f"Input must match regular expression: {validation_pattern.pattern}")

    def get_value(self, key: str):
        if not self.config_file_manager:
            raise RuntimeError("Config file manager not initialized")
        return self.config_file_manager.get_value(key)

    def store_value(self, key: str, value) -> None:
        if not self.config_file_manager:
            raise RuntimeError("Config file manager not initialized")
        self.config_file_manager.update_value(key, value)
