import csv
import io
import sys
import re
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError as RequestsConnectionError

from managertools.abstract_sprint_report import AbstractSprintReport, Mode
from managertools.flexidb.flexidb import FlexiDB
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.flexidb.data.flexidb_row import FlexiDBRow
from managertools.flexidb.init.abstract_flexidb_init_column import AbstractFlexiDBInitColumn
from managertools.flexidb.init.flexidb_init_data_column import FlexiDBInitDataColumn
from managertools.flexidb.init.flexidb_init_index_column import FlexiDBInitIndexColumn
from managertools.flexidb.output.convert_zeros_to_empty_output_filter import ConvertZerosToEmptyOutputFilter
from managertools.flexidb.output.output_filter import OutputFilter
from managertools.data import DBData, DBIndexData, UserActivity
from managertools.output.convert_self_metrics_empty_to_zero_output_filter import ConvertSelfMetricsEmptyToZeroOutputFilter
from managertools.rest.source_control_rest import SourceControlREST
from managertools.rest.exceptions import RESTException
from managertools.util.command_line_helper import CommandLineHelper
from managertools.util.sprint_data_cache import SprintDataCache


class CommentBlocker:
    def __init__(self):
        self.name = None
        self.date = None


class SprintReportTeamAnalysis(AbstractSprintReport):
    IGNORE_USERS = ["codeowners", "deployman", "sa-sre-jencim"]
    IGNORE_COMMENTS = ["Tasks to Complete Before Merging Pull Request"]

    SELF_PREFIX = "SELF_"
    TOTAL_PREFIX = "TOTAL_"

    PR_PREFIX = "PR_"
    COMMIT_PREFIX = "COMMIT_"

    TOTAL_PRS = "TOTAL_PRS"
    NON_DECLINED_PRS = "NON_DECLINED_PRS"

    STANDARD_OUTPUT_RULES = [ConvertZerosToEmptyOutputFilter()]
    SAME_USER_OUTPUT_RULES = [ConvertSelfMetricsEmptyToZeroOutputFilter()]

    MERGE_COMMIT_REGEX = r"(?i)(?:^|.*[ :])s*(down)?merge.*"

    def __init__(self, args: List[str]):
        super().__init__(args)
        self.database = None
        self.max_file_change_size = None
        self.max_commit_size = None
        self.ignore_filenames = set()

    def add_custom_command_line_options(self, parser):
        parser.add_argument('-i', '--isolateTicket', help='Isolate ticket for processing (debugging)')
        parser.add_argument('-o', '--outputCSV', required=True, help='Output filename (.csv)')
        parser.add_argument('-mt', '--multithread', help='Number of threads, default 1, *=cores')
        parser.add_argument('--includeMergeCommits', action='store_true', help='Include merge commits in code metrics')
        parser.add_argument('--maxCommitSize', type=int, help='Limit commit size (adds+removes) for code metrics')
        parser.add_argument('--kanbanCycleLength', type=int, default=2, help='Kanban cycle length in weeks (default 2)')

    def validate_custom_command_line_options(self):
        if not self.command_line_options.outputCSV.endswith('.csv'):
            raise RuntimeError("Output filename must end in .csv")

    def aggregate_data(self, team_name: Optional[str], board_id: Optional[str], mode: Mode, sprint_ids: List[str], cycles: Optional[int]):
        self.max_file_change_size = self.command_line_helper.get_config_file_manager().get_value("maxFileChangeSize")
        self.max_commit_size = self.command_line_helper.get_config_file_manager().get_value("maxCommitSize")
        self.ignore_filenames = self.command_line_helper.get_config_file_manager().get_value("ignoreFilenames") or set()

        self.database = FlexiDB(self.generate_db_signature(), True)

        # Determine thread count
        if self.command_line_options.multithread:
            if self.command_line_options.multithread == "*":
                import os
                thread_count = os.cpu_count() or 1
            else:
                thread_count = int(self.command_line_options.multithread)
        else:
            thread_count = 1

        print(f"Using {thread_count} threads")

        if mode == Mode.SCRUM:
            print(f"Processing Scrum: {len(sprint_ids)} sprints...")
            for i, sprint_id in enumerate(sprint_ids):
                try:
                    data = self.jira_rest.get_sprint_report(board_id, sprint_id)

                    all_issues = []
                    all_issues.extend(data.get('contents', {}).get('completedIssues', []))
                    all_issues.extend(data.get('contents', {}).get('issuesNotCompletedInCurrentSprint', []))

                    sprint_name = data.get('sprint', {}).get('name', '')
                    sprint_state = data.get('sprint', {}).get('state', '').lower()
                    is_completed = sprint_state == 'closed'

                    print(f"{i + 1} / {len(sprint_ids)}: {team_name}: {sprint_name} "
                          f"(id: {sprint_id}, issues: {len(all_issues)}, "
                          f"dates: {self.clean_date(data.get('sprint', {}).get('startDate', ''))} - "
                          f"{self.clean_date(data.get('sprint', {}).get('endDate', ''))})")

                    self.process_potentially_cached_sprint_data(thread_count, team_name, data.get('sprint', {}), mode, all_issues, is_completed)
                except Exception as e:
                    print(f"Error processing sprint {sprint_id}: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc(file=sys.stderr)
                    continue

        elif mode == Mode.KANBAN:
            if not team_name:
                raise RuntimeError("Team name is required for Kanban mode")

            cycle_length = (CommandLineHelper.prompt_number("Kanban cycle length, in weeks")
                           if self.command_line_options.prompt
                           else self.command_line_options.kanbanCycleLength)

            print(f"Processing Kanban {cycles} cycles...")
            for cycle in range(cycles):
                print(f"Kanban Cycle: {cycle} / {cycles}")
                try:
                    self.process_kanban_cycle(thread_count, team_name, cycle, cycles, cycle_length, mode)
                except Exception as e:
                    print(f"Error processing Kanban cycle {cycle}: {e}", file=sys.stderr)
                    continue

    def process_kanban_cycle(self, thread_count: int, team_name: str, cycle: int, cycles: int, cycle_length: int, mode: Mode):
        # Calculate cycle dates
        from datetime import datetime
        start_date = datetime.now() - timedelta(weeks=(cycles - cycle) * cycle_length)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # Move to Monday
        days_since_monday = start_date.weekday()
        if days_since_monday != 0:
            start_date -= timedelta(days=days_since_monday)

        end_date = start_date + timedelta(days=7 * cycle_length - 1, hours=23, minutes=59, seconds=59)

        start_date_str = start_date.strftime("%d/%b/%y 00:00 AM")
        end_date_str = end_date.strftime("%d/%b/%y 11:59 PM")
        clean_start_date = self.clean_date(start_date_str)
        clean_end_date = self.clean_date(end_date_str)

        cycle_name = f"{team_name} Cycle {cycle}"

        # Check if cycle is complete
        cycle_end_datetime = datetime.strptime(end_date_str, "%d/%b/%y %I:%M %p").replace(tzinfo=timezone.utc)
        is_cycle_complete = cycle_end_datetime.timestamp() * 1000 < datetime.now(timezone.utc).timestamp() * 1000

        print(f"   Cycle dates: {clean_start_date} - {clean_end_date}, complete: {is_cycle_complete}")

        # Check cache
        if is_cycle_complete and SprintDataCache.has_cached_data(team_name, cycle_name, clean_start_date, clean_end_date):
            print(f"   [DEBUG] Found cached data for cycle {cycle}, loading from cache...")
            cached_data = SprintDataCache.load_cached_data(team_name, cycle_name, clean_start_date, clean_end_date)
            self.load_cached_data_into_database(cached_data)
            print(f"   [DEBUG] Successfully loaded cycle {cycle} from cache")
            return

        print(f"   [DEBUG] No cache for cycle {cycle}, fetching from Jira...")

        data = None
        max_retries = 3
        retry_delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                data = self.jira_rest.get_kanban_cycle(team_name, cycle, cycles, cycle_length)
                print(f"   [DEBUG] Successfully fetched cycle {cycle} data from Jira")
                break
            except Exception as e:
                print(f"   [ERROR] Attempt {attempt}/{max_retries} failed for cycle {cycle}: {e}", file=sys.stderr)
                if attempt < max_retries:
                    print(f"   [INFO] Retrying in {retry_delay} seconds...", file=sys.stderr)
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise RuntimeError(f"Failed to fetch Kanban cycle {cycle} after {max_retries} attempts")

        if data is None:
            raise RuntimeError(f"Failed to fetch Kanban cycle {cycle} - data is None")

        all_issues = data.get('issues', [])
        print(f"   Cycle {cycle} / {cycles}: {len(all_issues)} issues")

        sprint_simulation = {
            'name': cycle_name,
            'startDate': data.get('startDate'),
            'endDate': data.get('endDate')
        }

        print(f"   [DEBUG] Processing fresh data for cycle {cycle}...")
        self.get_issue_category_information(thread_count, sprint_simulation, mode, all_issues)
        print(f"   [DEBUG] Finished processing cycle {cycle}")

        if is_cycle_complete:
            print(f"   [DEBUG] Saving cycle {cycle} to cache...")
            data_to_cache = self.extract_database_data_for_cache(cycle_name)
            SprintDataCache.save_to_cache(team_name, cycle_name, clean_start_date, clean_end_date, data_to_cache)
            print(f"   [DEBUG] Cycle {cycle} saved to cache")

    def process_potentially_cached_sprint_data(self, thread_count: int, team_name: str, sprint: Dict[str, Any], mode: Mode, all_issues: List[Any], use_cache: bool):
        sprint_name = sprint.get('name', '')
        start_date = self.clean_date(sprint.get('startDate', ''))
        end_date = self.clean_date(sprint.get('endDate', ''))

        print(f"   [DEBUG] Processing sprint: {sprint_name}, useCache: {use_cache}")

        if use_cache and SprintDataCache.has_cached_data(team_name, sprint_name, start_date, end_date):
            print("   [DEBUG] Loading from cache...")
            cached_data = SprintDataCache.load_cached_data(team_name, sprint_name, start_date, end_date)
            print("   [DEBUG] Cache loaded, loading into database...")
            self.load_cached_data_into_database(cached_data)
            print("   [DEBUG] Cache data loaded into database successfully")
        else:
            print("   [DEBUG] Processing fresh data (no cache or cache disabled)...")
            print(f"   [DEBUG] Starting getIssueCategoryInformation with {len(all_issues)} issues...")
            self.get_issue_category_information(thread_count, sprint, mode, all_issues)
            print("   [DEBUG] Finished getIssueCategoryInformation")

            if use_cache:
                print("   [DEBUG] Extracting data for cache...")
                data_to_cache = self.extract_database_data_for_cache(sprint_name)
                print("   [DEBUG] Saving to cache...")
                SprintDataCache.save_to_cache(team_name, sprint_name, start_date, end_date, data_to_cache)
                print("   [DEBUG] Cache saved successfully")

        print(f"   [DEBUG] Completed processing sprint: {sprint_name}")

    def extract_database_data_for_cache(self, sprint_name: str) -> Dict[str, Any]:
        sprint_filter = [FlexiDBQueryColumn(DBIndexData.SPRINT.name, sprint_name)]
        rows = self.database.find_rows(sprint_filter, True)

        serialized_rows = []
        for row in rows:
            serialized_row = dict(row)
            serialized_rows.append(serialized_row)

        return {'rows': serialized_rows}

    def load_cached_data_into_database(self, cached_data: Dict[str, Any]):
        serialized_rows = cached_data.get('rows', [])

        for serialized_row in serialized_rows:
            index_lookup = [
                FlexiDBQueryColumn(DBIndexData.SPRINT.name, serialized_row.get(DBIndexData.SPRINT.name)),
                FlexiDBQueryColumn(DBIndexData.TICKET.name, serialized_row.get(DBIndexData.TICKET.name)),
                FlexiDBQueryColumn(DBIndexData.PR_ID.name, serialized_row.get(DBIndexData.PR_ID.name)),
                FlexiDBQueryColumn(DBIndexData.PR_STATUS.name, serialized_row.get(DBIndexData.PR_STATUS.name, '')),
                FlexiDBQueryColumn(DBIndexData.USER.name, serialized_row.get(DBIndexData.USER.name))
            ]

            for key, value in serialized_row.items():
                if value is not None:
                    field_name = str(key)
                    if isinstance(value, list):
                        for item in value:
                            self.database.append(index_lookup, field_name, item)
                    else:
                        self.database.set_value(index_lookup, field_name, value)

    # ... (continue with rest of methods in next message due to size)

    def generate_columns_order(self) -> List[str]:
        column_order = list(self.database.get_original_column_order())
        
        # Move START_DATE and END_DATE after SPRINT
        if DBData.START_DATE.name in column_order:
            column_order.remove(DBData.START_DATE.name)
            column_order.insert(1, DBData.START_DATE.name)
        if DBData.END_DATE.name in column_order:
            column_order.remove(DBData.END_DATE.name)
            column_order.insert(2, DBData.END_DATE.name)

        # Remove self-comment fields
        for col in [UserActivity.COMMENTED_ON_SELF.name,
                   UserActivity.COMMENTED_ON_OTHERS.name,
                   UserActivity.OTHERS_COMMENTED.name]:
            if col in column_order:
                column_order.remove(col)

        return column_order

    def generate_output(self):
        column_order = self.generate_columns_order()
        sprints = self.database.find_unique_values(DBIndexData.SPRINT.name)
        users = list(set(self.database.find_unique_values(DBIndexData.USER.name)))

        sb = []
        sb.append(FlexiDBRow.headings_to_csv(column_order))

        overall_totals_row = FlexiDBRow({})
        for sprint in sprints:
            sprint_finder = [FlexiDBQueryColumn(DBIndexData.SPRINT.name, sprint)]
            self.find_rows_and_append_csv_data(sprint_finder, sb, overall_totals_row)

        self.append_summary(sb, overall_totals_row)

        data_indicator = self.team_name or self.board_id
        filename = self.command_line_options.outputCSV.replace('.csv', f'-{data_indicator}.csv')
        self.write_results_file(filename, '\n'.join(sb))

    def find_rows_and_append_csv_data(self, rows_filter: List[FlexiDBQueryColumn], sb: list, overall_totals_row: FlexiDBRow):
        rows = self.database.find_rows(rows_filter, True)

        if not rows:
            return

        # Sort rows by ticket, then PR ID
        def row_comparator(r1, r2):
            ticket1 = str(r1.get(DBIndexData.TICKET.name, '')).casefold()
            ticket2 = str(r2.get(DBIndexData.TICKET.name, '')).casefold()
            if ticket1 < ticket2:
                return -1
            elif ticket1 > ticket2:
                return 1

            try:
                pr1 = int(str(r1.get(DBIndexData.PR_ID.name, '0')).replace('#', ''))
                pr2 = int(str(r2.get(DBIndexData.PR_ID.name, '0')).replace('#', ''))
                return -1 if pr1 < pr2 else (1 if pr1 > pr2 else 0)
            except ValueError:
                pr_id1 = str(r1.get(DBIndexData.PR_ID.name, '')).casefold()
                pr_id2 = str(r2.get(DBIndexData.PR_ID.name, '')).casefold()
                return -1 if pr_id1 < pr_id2 else (1 if pr_id1 > pr_id2 else 0)

        from functools import cmp_to_key
        rows.sort(key=cmp_to_key(row_comparator))

        column_order = self.generate_columns_order()
        sprint_totals_row = FlexiDBRow({})

        total_prs = 0
        non_declined_prs = 0

        for row in rows:
            row_user = row.get(DBIndexData.USER.name)
            author = row.get(DBData.AUTHOR.name)
            pr_status = row.get(DBIndexData.PR_STATUS.name)

            output_row = FlexiDBRow(row)

            if not (row_user and author and row_user.casefold() == author.casefold()):
                output_row[DBIndexData.PR_STATUS.name] = ""

            sb.append(output_row.to_csv(column_order, self.STANDARD_OUTPUT_RULES))

            if row_user and author and row_user.casefold() == author.casefold():
                total_prs += 1
                if pr_status and pr_status.upper() != "DECLINED":
                    non_declined_prs += 1

            for column in column_order:
                value = row.get(column)
                if isinstance(value, (int, float)):
                    long_value = int(value)
                    sprint_totals_row[column] = sprint_totals_row.get(column, 0) + long_value
                    overall_totals_row[column] = overall_totals_row.get(column, 0) + long_value

        sprint_totals_row[self.TOTAL_PRS] = total_prs
        sprint_totals_row[self.NON_DECLINED_PRS] = non_declined_prs

        overall_totals_row[self.TOTAL_PRS] = overall_totals_row.get(self.TOTAL_PRS, 0) + total_prs
        overall_totals_row[self.NON_DECLINED_PRS] = overall_totals_row.get(self.NON_DECLINED_PRS, 0) + non_declined_prs

        self.append_totals_info(sb, "Sprint Totals", sprint_totals_row)
        sb.append("")

    def append_summary(self, sb: list, overall_totals_row: FlexiDBRow):
        self.append_totals_info(sb, "Overall Totals", overall_totals_row)

    def write_results_file(self, filename: str, data: str):
        print(f"Writing file: {filename}")
        with open(filename, 'w') as f:
            f.write(data)

    def get_issue_category_information(self, thread_count: int, sprint: Dict[str, Any], mode: Mode, issue_list: List[Any]):
        sprint_name = sprint.get('name', '')
        start_date = self.clean_date(sprint.get('startDate', ''))
        end_date = self.clean_date(sprint.get('endDate', ''))

        print(f"      [DEBUG] getIssueCategoryInformation started for sprint: {sprint_name}")

        # Parse sprint times
        sprint_start_time = datetime.strptime(sprint.get('startDate', ''), "%d/%b/%y %I:%M %p")
        sprint_end_time = datetime.strptime(sprint.get('endDate', ''), "%d/%b/%y %I:%M %p")
        sprint_start_ms = sprint_start_time.timestamp() * 1000
        sprint_end_ms = sprint_end_time.timestamp() * 1000

        isolated_ticket = self.command_line_options.isolateTicket or None

        counter = [0]  # Use list for mutable counter in nested function
        lock = threading.Lock()
        processing_errors = []  # Track errors to prevent cache on failure

        print(f"      [DEBUG] Starting thread pool with {thread_count} threads for {len(issue_list)} issues")

        def process_issue(issue):
            import traceback
            max_retries = 3
            retry_delay = 2

            try:
                ticket = issue.get('key')

                if isolated_ticket and ticket != isolated_ticket:
                    return

                issue_id = issue.get('id')

                pull_requests = []
                last_error = None

                for attempt in range(1, max_retries + 1):
                    try:
                        pull_requests = self.jira_rest.get_ticket_pull_request_info(str(issue_id))
                        last_error = None
                        break
                    except (RemoteDisconnected, RequestsConnectionError, BrokenPipeError, EOFError) as ce:
                        last_error = ce
                        if attempt < max_retries:
                            # Calculate exponential backoff with jitter
                            backoff = retry_delay * (2 ** (attempt - 1))
                            jitter = random.uniform(0, backoff * 0.1)
                            wait_time = backoff + jitter
                            with lock:
                                print(f"   {ticket}: Connection error (attempt {attempt}/{max_retries}): {type(ce).__name__}. "
                                      f"Retrying in {wait_time:.1f}s...", file=sys.stderr)
                            time.sleep(wait_time)
                        else:
                            with lock:
                                print(f"   {ticket}: Connection error after {max_retries} attempts: {ce}", file=sys.stderr)
                    except RESTException as re:
                        if re.status_code not in [403, 404]:  # FORBIDDEN, NOT_FOUND
                            return
                        break

                if last_error:
                    raise last_error

                with lock:
                    counter[0] += 1
                    print(f"   {counter[0]}/{len(issue_list)}: {ticket} / Issue {issue_id} has {len(pull_requests)} PRs")

                for pull_request in pull_requests:
                    self.process_pull_request(ticket, pull_request, sprint_name, start_date, end_date, sprint_start_ms, sprint_end_ms, mode)
            except Exception as e:
                with lock:
                    processing_errors.append(e)
                print(f"Error processing issue: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            list(executor.map(process_issue, issue_list))

        print(f"      [DEBUG] Parallel processing completed for {len(issue_list)} issues")
        print(f"      [DEBUG] getIssueCategoryInformation finished for sprint: {sprint_name}")

        if processing_errors:
            raise RuntimeError(f"Encountered {len(processing_errors)} errors during issue processing. First error: {processing_errors[0]}")

    def _retry_rest_call(self, func, max_retries: int = 3, retry_delay: int = 2):
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except (RemoteDisconnected, RequestsConnectionError, BrokenPipeError, EOFError) as ce:
                last_error = ce
                if attempt < max_retries:
                    backoff = retry_delay * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    wait_time = backoff + jitter
                    print(f"Connection error (attempt {attempt}/{max_retries}): {type(ce).__name__}. "
                          f"Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                else:
                    print(f"Connection error after {max_retries} attempts: {ce}", file=sys.stderr)
        if last_error:
            raise last_error

    def process_pull_request(self, ticket: str, pull_request: Dict[str, Any], sprint_name: str, start_date: str,
                            end_date: str, sprint_start_ms: float, sprint_end_ms: float, mode: Mode):
        print(f"      [DEBUG] Processing PR: {ticket}/{pull_request.get('id')}")

        pr_url = pull_request.get('url', '')
        is_github = 'github.com/' in pr_url.lower()
        source_control_rest = self.github_rest if is_github else self.bitbucket_rest

        pr_url = source_control_rest.api_convert(pr_url)
        pr_id = pull_request.get('id', '').lstrip('#')
        pr_status = pull_request.get('status', '')
        pr_author = source_control_rest.map_user_to_jira_name(pull_request.get('author', ''))

        # Get commits with retry logic
        pr_commits = self._retry_rest_call(lambda: source_control_rest.get_commits(pr_url))
        print(f"      [DEBUG] PR {ticket}/{pr_id}: Got {len(pr_commits) if pr_commits else 0} commits")

        if pr_author is None:
            if pr_commits:
                author_names = set()
                for commit in pr_commits:
                    author_names.add(commit.get('committer', {}).get('name', ''))
                if len(author_names) == 1:
                    pr_author = source_control_rest.map_user_to_jira_name(list(author_names)[0])

            if pr_author is None:
                print(f"Skipping processing of PR {ticket} / {pr_id} due to unknown author: {pull_request.get('author')}", file=sys.stderr)
                return

        # Get activities with retry logic
        pr_activities = self._retry_rest_call(lambda: source_control_rest.get_activities(pr_url))
        print(f"      [DEBUG] PR {ticket}/{pr_id}: Got {len(pr_activities) if pr_activities else 0} activities")

        comment_blockers = []

        # Process activities
        if pr_activities:
            activities = pr_activities if isinstance(pr_activities, list) else pr_activities.get('values', [])
            for activity in reversed(activities):
                user_name = activity.get('user', {}).get('name', '')

                if user_name.lower() in self.IGNORE_USERS:
                    continue

                action_str = activity.get('action', '')
                pr_info_action = UserActivity.get_resolved_value(action_str)
                if pr_info_action is None:
                    continue

                created_date = activity.get('createdDate', 0)
                if mode != Mode.KANBAN and (sprint_start_ms > created_date or created_date >= sprint_end_ms):
                    continue

                index_lookup = self.create_index_lookup(sprint_name, ticket, pr_id, user_name, pr_status)
                self.populate_baseline_db_info(index_lookup, start_date, end_date, pr_author)

                if pr_info_action == UserActivity.COMMENTED:
                    self.process_comment(comment_blockers, index_lookup, pr_author, activity)
                else:
                    self.increment_counter(index_lookup, pr_info_action)

        # Process commits
        if pr_commits:
            for commit in reversed(pr_commits):
                commit_sha = commit.get('id', '')
                commit_timestamp = commit.get('committerTimestamp', 0)

                if sprint_start_ms > commit_timestamp or commit_timestamp >= sprint_end_ms:
                    continue

                if not self.command_line_options.includeMergeCommits and re.match(self.MERGE_COMMIT_REGEX, commit.get('message', '')):
                    continue

                user_name = commit.get('committer', {}).get('name', '')

                if user_name.lower() in self.IGNORE_USERS:
                    continue

                index_lookup = self.create_index_lookup(sprint_name, ticket, pr_id, user_name, pr_status)
                self.populate_baseline_db_info(index_lookup, start_date, end_date, pr_author)

                commit_url = commit.get('url') or pr_url
                diffs_response = self._retry_rest_call(lambda: source_control_rest.get_commit_diffs(commit_url, commit_sha))

                if diffs_response:
                    self.process_diffs(self.COMMIT_PREFIX, diffs_response, index_lookup)

                commit_message = re.sub(r'(\r|\n)?\n', '  ', commit.get('message', '').strip())
                self.database.increment_field(index_lookup, UserActivity.COMMITS.name)
                self.database.append(index_lookup, DBData.COMMIT_MESSAGES.name, commit_message, True)

        # Process PR diffs if there was commit activity
        index_lookup = self.create_index_lookup(sprint_name, ticket, pr_id, pr_author, pr_status)
        if self.database.find_rows(index_lookup, False):
            commit_added = self.database.get_value(index_lookup, UserActivity.COMMIT_ADDED.name) or 0
            commit_removed = self.database.get_value(index_lookup, UserActivity.COMMIT_REMOVED.name) or 0
            if commit_added > 0 or commit_removed > 0:
                diffs_response = self._retry_rest_call(lambda: source_control_rest.get_diffs(pr_url))
                if diffs_response:
                    self.populate_baseline_db_info(index_lookup, start_date, end_date, pr_author)
                    self.process_diffs(self.PR_PREFIX, diffs_response, index_lookup)

    @staticmethod
    def sanitize_name_for_index(name: str) -> str:
        return name.replace('-', '.')

    def create_index_lookup(self, sprint_name: str, ticket: str, pr_id: str, user_name: str, pr_status: str = '') -> List[FlexiDBQueryColumn]:
        index_lookup = [
            FlexiDBQueryColumn(DBIndexData.SPRINT.name, sprint_name),
            FlexiDBQueryColumn(DBIndexData.TICKET.name, ticket),
            FlexiDBQueryColumn(DBIndexData.PR_ID.name, pr_id),
            FlexiDBQueryColumn(DBIndexData.PR_STATUS.name, pr_status),
            FlexiDBQueryColumn(DBIndexData.USER.name, self.sanitize_name_for_index(user_name))
        ]
        return index_lookup

    def populate_baseline_db_info(self, index_lookup: List[FlexiDBQueryColumn], start_date: str, end_date: str, pr_author: str):
        self.database.set_value(index_lookup, DBData.START_DATE.name, start_date)
        self.database.set_value(index_lookup, DBData.END_DATE.name, end_date)
        self.database.set_value(index_lookup, DBData.AUTHOR.name, self.sanitize_name_for_index(pr_author))

    def process_diffs(self, prefix: str, diffs_response: Dict[str, Any], index_lookup: List[FlexiDBQueryColumn]):
        additions = diffs_response.get('additions')
        deletions = diffs_response.get('deletions')

        if diffs_response.get('stats'):
            additions = diffs_response['stats'].get('additions')
            deletions = diffs_response['stats'].get('deletions')

        if additions is not None or deletions is not None:
            total = (additions or 0) + (deletions or 0)
            if self.command_line_options.maxCommitSize and total >= self.command_line_options.maxCommitSize:
                return

            self.increment_counter(index_lookup, UserActivity[prefix + "ADDED"], additions or 0)
            self.increment_counter(index_lookup, UserActivity[prefix + "REMOVED"], deletions or 0)
            return

        diffs = diffs_response.get('diffs', [])
        if not diffs:
            return

        added_calculated = 0
        removed_calculated = 0

        for diff in diffs:
            for hunk in diff.get('hunks', []):
                for segment in hunk.get('segments', []):
                    if segment.get('type') == 'ADDED':
                        added_calculated += len(segment.get('lines', []))
                    elif segment.get('type') == 'REMOVED':
                        removed_calculated += len(segment.get('lines', []))

        total = added_calculated + removed_calculated
        if self.command_line_options.maxCommitSize and total >= self.command_line_options.maxCommitSize:
            return

        self.increment_counter(index_lookup, UserActivity[prefix + "ADDED"], added_calculated)
        self.increment_counter(index_lookup, UserActivity[prefix + "REMOVED"], removed_calculated)

    def increment_counter(self, index_lookup: List[FlexiDBQueryColumn], activity: UserActivity, increment: int = 1) -> int:
        return self.database.increment_field(index_lookup, activity.name, increment)

    def process_comment(self, comment_blockers: List[CommentBlocker], index_lookup: List[FlexiDBQueryColumn], 
                       pr_author: str, pr_activity: Dict[str, Any]):
        self.process_comment_recursive(comment_blockers, index_lookup, pr_author, 
                                      pr_activity.get('user', {}).get('name', ''),
                                      pr_activity.get('action', ''),
                                      pr_activity.get('commentAction', ''),
                                      pr_activity.get('comment', {}), 3)

    def process_comment_recursive(self, comment_blockers: List[CommentBlocker], original_index_lookup: List[FlexiDBQueryColumn],
                                  pr_author: str, user_name: str, action: str, comment_action: str, 
                                  comment: Dict[str, Any], indentation: int):

        sprint_ticket_pr_base = [q for q in original_index_lookup if q.get_name() != DBIndexData.USER.name]
        
        pr_author_lookup = sprint_ticket_pr_base + [FlexiDBQueryColumn(DBIndexData.USER.name, self.sanitize_name_for_index(pr_author))]
        current_user_lookup = sprint_ticket_pr_base + [FlexiDBQueryColumn(DBIndexData.USER.name, self.sanitize_name_for_index(user_name))]

        comment_text = comment.get('text', '')

        # Check comment blockers
        for blocker in comment_blockers:
            if (blocker.name == comment.get('author', {}).get('name') and 
                abs(blocker.date - comment.get('createdDate', 0)) <= 1000):
                return

        if comment_text in self.IGNORE_COMMENTS:
            blocker = CommentBlocker()
            blocker.name = comment.get('author', {}).get('name')
            blocker.date = comment.get('createdDate')
            comment_blockers.append(blocker)
            return

        comment_text = re.sub(r'(\r|\n)?\n', '  ', comment_text).strip()

        self.populate_baseline_db_info(current_user_lookup,
                                      self.database.get_value(original_index_lookup, DBData.START_DATE.name),
                                      self.database.get_value(original_index_lookup, DBData.END_DATE.name),
                                      pr_author)

        self.database.append(current_user_lookup, DBData.COMMENTS.name, comment_text, True)
        self.increment_counter(current_user_lookup, UserActivity.COMMENTED)

        if pr_author.casefold() == user_name.casefold():
            self.increment_counter(current_user_lookup, UserActivity.COMMENTED_ON_SELF)
        else:
            self.increment_counter(current_user_lookup, UserActivity.COMMENTED_ON_OTHERS)
            self.increment_counter(pr_author_lookup, UserActivity.OTHERS_COMMENTED)
            self.database.append(pr_author_lookup, DBData.OTHERS_COMMENTS.name, f"({user_name}) {comment_text}", True)

        # Recursively process replies
        for reply in comment.get('comments', []):
            self.process_comment_recursive(comment_blockers, original_index_lookup, pr_author,
                                         reply.get('author', {}).get('name', ''),
                                         UserActivity.COMMENTED.name, 'REPLY', reply, indentation + 3)

    def generate_db_signature(self) -> List[AbstractFlexiDBInitColumn]:
        columns = []

        for index_data in DBIndexData:
            columns.append(FlexiDBInitIndexColumn(index_data.name))

        for activity in UserActivity:
            columns.append(FlexiDBInitDataColumn(activity.name, activity.get_default_value()))

        for data in DBData:
            columns.append(FlexiDBInitDataColumn(data.name, data.get_default_value()))

        return columns

    def append_totals_info(self, sb: list, totals_description: str, totals_row: FlexiDBRow):
        column_order = self.generate_columns_order()

        if column_order and column_order[0] not in totals_row:
            column_order[0] = ""
            totals_row[column_order[0]] = totals_description

        pr_id_index = column_order.index(DBIndexData.PR_ID.name) if DBIndexData.PR_ID.name in column_order else -1
        pr_status_index = column_order.index(DBIndexData.PR_STATUS.name) if DBIndexData.PR_STATUS.name in column_order else -1

        if self.TOTAL_PRS in totals_row and self.NON_DECLINED_PRS in totals_row:
            if pr_id_index != -1:
                totals_row[column_order[pr_id_index]] = totals_row.get(self.TOTAL_PRS, 0)
            if pr_status_index != -1:
                totals_row[column_order[pr_status_index]] = totals_row.get(self.NON_DECLINED_PRS, 0)

        for i in range(1, len(column_order)):
            column_name = column_order[i]

            if column_name == DBIndexData.PR_ID.name or column_name == DBIndexData.PR_STATUS.name:
                if column_name == DBIndexData.PR_ID.name and column_name in totals_row:
                    totals_row[column_name] = f"PRs: {totals_row[column_name]}"
                if column_name == DBIndexData.PR_STATUS.name and column_name in totals_row:
                    totals_row[column_name] = f"Valid PRs: {totals_row[column_name]}"
                continue

            try:
                user_activity = UserActivity[column_name]
                if column_name in totals_row or user_activity:
                    value = totals_row.get(column_name, user_activity.get_default_value() if user_activity else None)
                    if user_activity and column_name in totals_row and value == user_activity.get_default_value():
                        value = 0
                    totals_row[column_name] = f"{column_name}: {value}"
            except KeyError:
                pass

        sb.append(totals_row.to_csv(column_order))

    @staticmethod
    def clean_date(date_str: str) -> str:
        try:
            parsed = datetime.strptime(date_str, "%d/%b/%y")
            return parsed.strftime("%Y/%m/%d")
        except ValueError:
            try:
                parsed = datetime.strptime(date_str, "%Y-%m-%d")
                return parsed.strftime("%Y/%m/%d")
            except ValueError:
                return date_str


if __name__ == '__main__':
    analysis = SprintReportTeamAnalysis(sys.argv[1:])
    analysis.run()
