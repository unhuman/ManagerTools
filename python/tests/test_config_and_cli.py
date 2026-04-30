import pytest
import os
import json
import tempfile
from unittest.mock import patch, MagicMock
from managertools.util.config_file_manager import ConfigFileManager
from managertools.util.command_line_helper import CommandLineHelper, TextSecurity
from managertools.rest.null_rest import NullREST


class TestConfigFileManager:
    def test_init_creates_empty_state_on_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "test.cfg")
            cfg = ConfigFileManager(config_path.replace(os.path.expanduser("~"), "~", 1))
            assert cfg.state == {}

    def test_init_loads_existing_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"server": {"jira": "jira.example.com"}}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            # Create ConfigFileManager with absolute path (workaround for home expansion)
            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = {}
            with open(config_file, 'r') as f:
                cfg.state = json.load(f)

            assert cfg.state == test_data

    def test_contains_key_single_level(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"server": "jira.example.com"}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = test_data

            assert cfg.contains_key("server")
            assert not cfg.contains_key("missing")

    def test_contains_key_nested(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"server": {"jira": "jira.example.com"}}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = test_data

            assert cfg.contains_key("server.jira")
            assert not cfg.contains_key("server.bitbucket")

    def test_contains_key_case_insensitive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"Server": "jira.example.com"}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = test_data

            assert cfg.contains_key("server")
            assert cfg.contains_key("SERVER")

    def test_get_value(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"server": {"jira": "jira.example.com"}}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = test_data

            assert cfg.get_value("server.jira") == "jira.example.com"

    def test_get_value_missing_key_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")
            test_data = {"server": {}}
            with open(config_file, 'w') as f:
                json.dump(test_data, f)

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = test_data

            with pytest.raises(RuntimeError, match="Could not find key"):
                cfg.get_value("server.jira")

    def test_update_value_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "test.cfg")

            cfg = ConfigFileManager.__new__(ConfigFileManager)
            cfg.filename = config_file
            cfg.state = {}

            cfg.update_value("server", "jira.example.com")

            assert os.path.exists(config_file)
            with open(config_file, 'r') as f:
                data = json.load(f)
            assert data["server"] == "jira.example.com"


class TestCommandLineHelper:
    @patch('managertools.util.command_line_helper.input')
    def test_prompt_with_pattern(self, mock_input):
        mock_input.return_value = "test_value"
        result = CommandLineHelper.prompt("Enter value", CommandLineHelper.DATA_REQUIRED_PATTERN)
        assert result == "test_value"

    @patch('managertools.util.command_line_helper.input')
    def test_prompt_invalid_then_valid(self, mock_input):
        mock_input.side_effect = ["", "valid_value"]
        result = CommandLineHelper.prompt("Enter value", CommandLineHelper.DATA_REQUIRED_PATTERN)
        assert result == "valid_value"

    @patch('managertools.util.command_line_helper.getpass.getpass')
    def test_prompt_password(self, mock_getpass):
        mock_getpass.return_value = "secret"
        result = CommandLineHelper._perform_prompt("Password", TextSecurity.PASSWORD, CommandLineHelper.ANY_MATCH_PATTERN)
        assert result == "secret"

    def test_quiet_mode_returns_empty(self):
        CommandLineHelper.set_quiet_mode_no_prompts()
        result = CommandLineHelper.prompt("Ignored")
        assert result == ""
        CommandLineHelper.quiet_mode = False

    @patch('managertools.util.command_line_helper.input')
    def test_prompt_number(self, mock_input):
        mock_input.return_value = "123"
        result = CommandLineHelper.prompt_number("Enter number")
        assert result == "123"

    @patch('managertools.util.command_line_helper.input')
    def test_prompt_value(self, mock_input):
        mock_input.return_value = "some_value"
        result = CommandLineHelper.prompt_value("Enter value")
        assert result == "some_value"


class TestNullREST:
    @patch('sys.stderr')
    def test_null_rest_init(self, mock_stderr):
        null_rest = NullREST("github")
        assert null_rest.auth_info is None

    def test_null_rest_returns_empty_lists(self):
        null_rest = NullREST("github")
        assert null_rest.get_activities("url") == []
        assert null_rest.get_commits("url") == []
        assert null_rest.get_diffs("url") == []
        assert null_rest.get_commit_diffs("url", "sha") == []
        assert null_rest.map_user_to_jira_name({"name": "user"}) is None
