import csv
import io
import sys
import re
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
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
from managertools.data import DBData, DBIndexData, UserActivity, WorkSource
from managertools.output.convert_self_metrics_empty_to_zero_output_filter import ConvertSelfMetricsEmptyToZeroOutputFilter
from managertools.output.format_commit_data_output_filter import FormatCommitDataOutputFilter
from managertools.output.append_asterisk_output_filter import AppendAsteriskOutputFilter
from managertools.rest.source_control_rest import SourceControlREST
from managertools.rest.exceptions import RESTException, NeedsRetryException
from managertools.util.command_line_helper import CommandLineHelper
from managertools.util.log_util import debug_print
from managertools.util.sprint_data_cache import SprintDataCache
from managertools.util.pr_data_cache import PRDataCache


class CommentBlocker:
    def __init__(self):
        self.name = None
        self.date = None


class SprintReportTeamAnalysis(AbstractSprintReport):
    IGNORE_USERS = ["codeowners", "deployman", "sa-sre-jencim", "Jenkins"]
    IGNORE_COMMENTS = ["Tasks to Complete Before Merging Pull Request"]

    SELF_PREFIX = "SELF_"
    TOTAL_PREFIX = "TOTAL_"

    PR_PREFIX = "PR_"
    COMMIT_PREFIX = "COMMIT_"

    TOTAL_PRS = "TOTAL_PRS"
    NON_DECLINED_PRS = "NON_DECLINED_PRS"

    # Synthetic PR id for commits sourced directly from the Jira dev-status commit view
    # (WorkSource.COMMIT / BOTH). These commits have no PR, so they roll up under one bucket
    # per ticket and are excluded from the TOTAL_PRS / NON_DECLINED_PRS counts.
    COMMITS_PR_ID = "(commits)"

    STANDARD_OUTPUT_RULES = [ConvertZerosToEmptyOutputFilter(), FormatCommitDataOutputFilter()]
    SAME_USER_OUTPUT_RULES = [ConvertSelfMetricsEmptyToZeroOutputFilter()]

    MERGE_COMMIT_REGEX = r"(?i)(?:^|.*[ :])\s*(down)?merge.*"
    DEFAULT_MAX_COMMIT_SIZE = 2_000  # lines added + removed

    # Collection-time down-merge PR skip ruleset (see README "Configuration"). A PR is skipped
    # if its title matches any title pattern (free, from Jira dev-status) OR its source branch
    # (headRefName) is a trunk branch (needs a cheap GraphQL metadata probe). Both lists are
    # configurable; an empty list disables that rule. --includeDownMergePRs disables skipping.
    DEFAULT_DOWN_MERGE_TITLE_PATTERNS = [r"(?i).*down\s*merge.*"]
    DEFAULT_DOWN_MERGE_TRUNK_BRANCHES = ["main", "master", "develop", r"release/.*"]

    def __init__(self, args: List[str]):
        super().__init__(args)
        self.database = None
        self.max_file_change_size = None
        self.max_commit_size = None
        self.ignore_filenames = set()
        self._counted_pr_activities = set()
        self._pr_primary_ticket = {}
        self._ticket_pr_data = {}
        self._pr_mem_cache: Dict[str, Any] = {}
        self._pr_data_cache: Optional[PRDataCache] = None
        self._github_pr_fetch_index: int = 0
        self._github_pr_fetch_total: int = 0
        self.down_merge_title_patterns: List[str] = []
        self.down_merge_trunk_branches: List[str] = []
        self._down_merge_skips: List[str] = []  # skip_reason strings, for the per-pass summary
        self._overall_capped_cols: set = set()  # columns capped on any row, for the Overall Totals '*'
        self.work_source: WorkSource = WorkSource.PR  # where work is sourced (pr/commit/both)
        self._ticket_commit_data: Dict[str, Any] = {}  # ticket -> dev-status commit list (prefetch)

    def add_custom_command_line_options(self, parser):
        parser.add_argument('-i', '--isolateTicket', help='Isolate ticket for processing (debugging)')
        parser.add_argument('-o', '--outputCSV', required=True, help='Output filename (.csv)')
        parser.add_argument('-mt', '--multithread', help='Number of threads, default 1, *=cores')
        parser.add_argument('--includeMergeCommits', action='store_true', help='Include merge commits (2+ parents) in report metrics; excluded by default')
        parser.add_argument('--includeDownMergePRs', action='store_true', help='Process down-merge PRs (huge trunk-into-branch merges) instead of skipping them at collection time')
        parser.add_argument('--includeBroughtInCommits', action='store_true', help='Include commits merged in from other branches (2nd-parent of a merge) in report metrics; excluded by default')
        parser.add_argument('--maxCommitSize', type=int, help='Limit commit size (adds+removes) for code metrics')
        parser.add_argument('--workSource', choices=['pr', 'commit', 'both'], default=None,
                            help="Where sprint work is sourced: 'pr' (default), 'commit' (Jira dev-status "
                                 "Commits view), or 'both' (PRs + uncovered commits, de-duped by SHA). "
                                 "Overrides the workSource config value.")
        parser.add_argument('--kanbanCycleLength', type=int, default=2, help='Kanban cycle length in weeks (default 2)')

    def validate_custom_command_line_options(self):
        if not self.command_line_options.outputCSV.endswith('.csv'):
            raise RuntimeError("Output filename must end in .csv")

    def aggregate_data(self, team_name: Optional[str], board_id: Optional[str], mode: Mode, sprint_ids: List[str], cycles: Optional[int], kanban_start_date: Optional[str] = None):
        self.max_file_change_size = self.command_line_helper.get_config_file_manager().get_value("maxFileChangeSize")
        config_value = self.command_line_helper.get_config_file_manager().get_value("maxCommitSize")
        self.max_commit_size = int(config_value) if config_value else self.DEFAULT_MAX_COMMIT_SIZE
        # CLI flag takes precedence over config file / default
        if self.command_line_options.maxCommitSize is not None:
            self.max_commit_size = int(self.command_line_options.maxCommitSize)
        self.ignore_filenames = self.command_line_helper.get_config_file_manager().get_value("ignoreFilenames") or set()

        # Load PR title and commit message filters
        self.ignore_pr_title_patterns = self.command_line_helper.get_config_file_manager().get_value("ignorePRTitleContent") or []
        self.ignore_commit_message_patterns = self.command_line_helper.get_config_file_manager().get_value("ignoreCommitMessageContent") or []

        # Down-merge PR skip ruleset (configurable; defaults applied when keys are absent).
        config_mgr = self.command_line_helper.get_config_file_manager()
        self.down_merge_title_patterns = (config_mgr.get_value("downMergePRTitlePatterns")
                                          if config_mgr.contains_key("downMergePRTitlePatterns")
                                          else self.DEFAULT_DOWN_MERGE_TITLE_PATTERNS)
        self.down_merge_trunk_branches = (config_mgr.get_value("downMergeTrunkBranches")
                                          if config_mgr.contains_key("downMergeTrunkBranches")
                                          else self.DEFAULT_DOWN_MERGE_TRUNK_BRANCHES)

        # Work source: CLI flag wins, then config "workSource", then default "pr" (backward compatible).
        work_source_str = (config_mgr.get_value("workSource")
                           if config_mgr.contains_key("workSource") else "pr")
        if self.command_line_options.workSource is not None:
            work_source_str = self.command_line_options.workSource
        self.work_source = WorkSource(str(work_source_str).lower())

        self.database = FlexiDB(self.generate_db_signature(), True)
        self.incomplete_sprints = []
        self._counted_pr_activities = set()
        self._pr_primary_ticket = {}
        self._ticket_pr_data = {}
        self._ticket_commit_data = {}
        self._pr_mem_cache: Dict[str, Any] = {}
        self._pr_data_cache = PRDataCache(self.team_name)

        try:
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
                        # Retry get_sprint_report up to 3 times on transient failures
                        max_retries = 3
                        retry_delay = 5
                        data = None
                        for attempt in range(1, max_retries + 1):
                            try:
                                data = self.jira_rest.get_sprint_report(board_id, sprint_id)
                                break
                            except Exception as e:
                                print(f"   [ERROR] Attempt {attempt}/{max_retries} failed for sprint {sprint_id}: {e}", file=sys.stderr)
                                if attempt < max_retries:
                                    print(f"   [INFO] Retrying in {retry_delay} seconds...", file=sys.stderr)
                                    time.sleep(retry_delay)
                                    retry_delay *= 2
                                else:
                                    print(f"   [ERROR] Failed to fetch sprint {sprint_id} after {max_retries} attempts, skipping", file=sys.stderr)
                        if data is None:
                            continue

                        contents = data.get('contents', {})
                        completed = contents.get('completedIssues', [])
                        incomplete = contents.get('issuesNotCompletedInCurrentSprint', [])
                        punted = contents.get('puntedIssues', [])

                        seen_ids = set()
                        all_issues = []
                        for issue in completed + incomplete + punted:
                            issue_id_key = issue.get('id') or issue.get('key')
                            if issue_id_key not in seen_ids:
                                seen_ids.add(issue_id_key)
                                all_issues.append(issue)

                        sprint_name = data.get('sprint', {}).get('name', '')
                        sprint_state = data.get('sprint', {}).get('state', '').lower()
                        is_completed = sprint_state == 'closed'
                        start_date = self.clean_date(data.get('sprint', {}).get('startDate', ''))
                        end_date = self.clean_date(data.get('sprint', {}).get('endDate', ''))

                        _G = "\033[92m"
                        _R = "\033[0m"
                        print(f"{_G}{i + 1} / {len(sprint_ids)}: {team_name}: {sprint_name} "
                              f"(id: {sprint_id}, issues: {len(all_issues)} "
                              f"[{len(completed)} done, {len(incomplete)} incomplete, {len(punted)} punted], "
                              f"dates: {start_date} - {end_date}){_R}")

                        self.process_potentially_cached_sprint_data(thread_count, team_name, data.get('sprint', {}), mode, all_issues, is_completed)

                        # Evict PR cache entries not referenced by any ticket in this sprint
                        self._evict_stale_pr_cache_entries()

                        # Track incomplete sprints (only for the work sources this run requires)
                        if is_completed and not SprintDataCache.is_cache_complete(
                                team_name, sprint_name, start_date, end_date,
                                required_sources=self._sources_for(self.work_source)):
                            self.incomplete_sprints.append(sprint_name)
                    except Exception as e:
                        print(f"Error processing sprint {sprint_id}: {e}", file=sys.stderr)
                        import traceback
                        traceback.print_exc(file=sys.stderr)
                        continue

            elif mode == Mode.KANBAN:
                if not team_name:
                    raise RuntimeError("Team name is required for Kanban mode")

                from datetime import datetime
                cycle_length = (int(CommandLineHelper.prompt_number("Kanban cycle length, in weeks"))
                               if self.command_line_options.prompt
                               else self.command_line_options.kanbanCycleLength)

                # Trim count upfront if the last cycle is still in progress and --includeActive not set
                effective_cycles = cycles
                if not self.command_line_options.includeActive and kanban_start_date:
                    last_start = datetime.fromisoformat(kanban_start_date).replace(hour=0, minute=0, second=0, microsecond=0)
                    last_start += timedelta(weeks=(cycles - 1) * cycle_length)
                    dsm = last_start.weekday()
                    if dsm != 0:
                        last_start += timedelta(days=7 - dsm)
                    last_end = last_start + timedelta(days=7 * cycle_length - 1, hours=23, minutes=59, seconds=59)
                    if last_end.replace(tzinfo=timezone.utc).timestamp() * 1000 >= datetime.now(timezone.utc).timestamp() * 1000:
                        effective_cycles = cycles - 1
                        print(f"   Cycle {cycles} is still in progress; processing {effective_cycles} complete cycles (use --includeActive to include cycle {cycles})")

                _G = "\033[92m"
                _R = "\033[0m"
                print(f"{_G}Processing Kanban {effective_cycles} cycles...{_R}")
                for cycle in range(1, effective_cycles + 1):
                    print(f"{_G}Kanban Cycle: {cycle} / {effective_cycles}{_R}")
                    try:
                        # Calculate cycle dates
                        if kanban_start_date:
                            start_date_calc = datetime.fromisoformat(kanban_start_date)
                            start_date_calc = start_date_calc.replace(hour=0, minute=0, second=0, microsecond=0)
                            start_date_calc += timedelta(weeks=(cycle - 1) * cycle_length)
                            days_since_monday = start_date_calc.weekday()
                            if days_since_monday != 0:
                                start_date_calc += timedelta(days=7 - days_since_monday)
                        else:
                            start_date_calc = datetime.now() - timedelta(weeks=(effective_cycles - cycle + 1) * cycle_length)
                            start_date_calc = start_date_calc.replace(hour=0, minute=0, second=0, microsecond=0)
                            days_since_monday = start_date_calc.weekday()
                            if days_since_monday != 0:
                                start_date_calc -= timedelta(days=days_since_monday)
                        end_date_calc = start_date_calc + timedelta(days=7 * cycle_length - 1, hours=23, minutes=59, seconds=59)
                        clean_start_date = self.clean_date(start_date_calc.strftime("%d/%b/%y 12:00 AM"))
                        clean_end_date = self.clean_date(end_date_calc.strftime("%d/%b/%y 11:59 PM"))
                        cycle_name = f"{team_name} Cycle {cycle}"

                        cycle_end_datetime = end_date_calc.replace(tzinfo=timezone.utc)
                        is_cycle_complete = cycle_end_datetime.timestamp() * 1000 < datetime.now(timezone.utc).timestamp() * 1000

                        self.process_kanban_cycle(thread_count, team_name, cycle, effective_cycles, cycle_length, mode, kanban_start_date)

                        # Evict PR cache entries not referenced by any ticket in this cycle
                        self._evict_stale_pr_cache_entries()

                        # Track complete cycles whose cache is still incomplete (for required sources)
                        if is_cycle_complete and not SprintDataCache.is_cache_complete(
                                team_name, "", clean_start_date, clean_end_date,
                                required_sources=self._sources_for(self.work_source)):
                            self.incomplete_sprints.append(cycle_name)
                    except Exception as e:
                        print(f"Error processing Kanban cycle {cycle}: {e}", file=sys.stderr)
                        cycle_name = f"{team_name} Cycle {cycle}"
                        self.incomplete_sprints.append(cycle_name)
                        continue
        finally:
            self._pr_data_cache.release()

    def process_kanban_cycle(self, thread_count: int, team_name: str, cycle: int, cycles: int, cycle_length: int, mode: Mode, kanban_start_date: Optional[str] = None):
        # Calculate cycle dates
        from datetime import datetime
        if kanban_start_date:
            start_date = datetime.fromisoformat(kanban_start_date)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            # Move forward to the start of this cycle
            start_date += timedelta(weeks=(cycle - 1) * cycle_length)
            # If not Monday, move forward to next Monday to align cycle boundaries
            days_since_monday = start_date.weekday()
            if days_since_monday != 0:
                start_date += timedelta(days=7 - days_since_monday)
        else:
            start_date = datetime.now() - timedelta(weeks=(cycles - cycle + 1) * cycle_length)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            # Move to Monday
            days_since_monday = start_date.weekday()
            if days_since_monday != 0:
                start_date -= timedelta(days=days_since_monday)

        end_date = start_date + timedelta(days=7 * cycle_length - 1, hours=23, minutes=59, seconds=59)

        start_date_str = start_date.strftime("%d/%b/%y 12:00 AM")
        end_date_str = end_date.strftime("%d/%b/%y 11:59 PM")
        clean_start_date = self.clean_date(start_date_str)
        clean_end_date = self.clean_date(end_date_str)

        cycle_name = f"{team_name} Cycle {cycle}"

        # Check if cycle is complete
        cycle_end_datetime = datetime.strptime(end_date_str, "%d/%b/%y %I:%M %p").replace(tzinfo=timezone.utc)
        is_cycle_complete = cycle_end_datetime.timestamp() * 1000 < datetime.now(timezone.utc).timestamp() * 1000

        print(f"   Cycle dates: {clean_start_date} - {clean_end_date}, complete: {is_cycle_complete}")

        # Kanban bypasses the per-activity sprint window (mode == KANBAN), so the simulation dates
        # only need to be parseable — use the computed cycle boundaries rather than refetching.
        sprint_simulation = {
            'name': cycle_name,
            'startDate': start_date_str,
            'endDate': end_date_str,
        }

        def fetch_cycle_issues():
            # Lazy: only called when the cache doesn't already satisfy the run. Returns the issue
            # list, or None on persistent failure (so the helper keeps any existing cache).
            data = None
            max_retries = 3
            retry_delay = 5
            for attempt in range(1, max_retries + 1):
                try:
                    data = self.jira_rest.get_kanban_cycle(team_name, cycle, cycles, cycle_length)
                    debug_print(f"Successfully fetched cycle {cycle} data from Jira")
                    break
                except Exception as e:
                    print(f"   [ERROR] Attempt {attempt}/{max_retries} failed for cycle {cycle}: {e}", file=sys.stderr)
                    if attempt < max_retries:
                        print(f"   [INFO] Retrying in {retry_delay} seconds...", file=sys.stderr)
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        return None
            if data is None:
                return None
            issues = data.get('issues', [])
            print(f"   Cycle {cycle} / {cycles}: {len(issues)} issues")
            return issues

        # Cache key uses team + dates only ("" sprint component); cycle number is display-only.
        self._process_sprint_with_source_aware_cache(
            thread_count, team_name, "", clean_start_date, clean_end_date,
            sprint_simulation, mode, cacheable=is_cycle_complete, get_all_issues=fetch_cycle_issues)

    def _evict_stale_pr_cache_entries(self) -> None:
        """Evict PR cache entries not referenced by any ticket in current sprint."""
        active_pr_urls = {
            pr.get('url')
            for pr_list in self._ticket_pr_data.values()
            for pr in pr_list
            if pr.get('url')
        }
        stale = [url for url in self._pr_mem_cache if url not in active_pr_urls]
        for url in stale:
            del self._pr_mem_cache[url]
        if stale:
            debug_print(f"Evicted {len(stale)} stale PR cache entries")

    def _set_github_commit_retry_mode(self, enabled: bool) -> None:
        """Toggle the GitHub GraphQL small commit page-size ladder, if a GitHub client is in use."""
        if self.github_rest is not None and hasattr(self.github_rest, 'set_commit_page_size_retry_mode'):
            self.github_rest.set_commit_page_size_retry_mode(enabled)

    @staticmethod
    def _sources_for(work_source: WorkSource) -> set:
        """The set of work-source values a run must satisfy: pr->{pr}, commit->{commit}, both->{pr,commit}."""
        if work_source == WorkSource.PR:
            return {WorkSource.PR.value}
        if work_source == WorkSource.COMMIT:
            return {WorkSource.COMMIT.value}
        return {WorkSource.PR.value, WorkSource.COMMIT.value}

    def _row_source(self, row: Dict[str, Any]) -> str:
        """Which work source produced a cached row, inferred from its PR_ID (commit rows use the
        synthetic COMMITS_PR_ID bucket; everything else came from PR processing)."""
        return (WorkSource.COMMIT.value if row.get(DBIndexData.PR_ID.name) == self.COMMITS_PR_ID
                else WorkSource.PR.value)

    def _plan_incomplete_cache_retry(self, all_issues: List[Any], prev_failed: List[str],
                                     prev_failed_prs: List[Dict[str, str]], cached_data: Dict[str, Any]):
        """Decide what an incomplete cache needs to reprocess, at PR-level granularity.

        - A *failed issue* (ticket absent from cache) → reprocess the whole ticket.
        - A *failed PR only* (ticket cached, specific PRs failed) → reprocess only those PR ids
          (via the returned pr_id_filter); the ticket's other (good) PR rows are kept from cache.

        Returns (retry_issues, filtered_cached, all_failed_tickets, pr_id_filter). The row filter
        drops whole-ticket rows for failed issues and only the failed-PR rows for PR-only tickets,
        so reprocessing never duplicates list fields / over-counts counters. pr_id_filter is
        passed to get_issue_category_information to narrow PR fetching to the failed PRs.
        """
        failed_issue_set = set(prev_failed or [])
        pr_id_filter: Dict[str, set] = {}
        for pr in (prev_failed_prs or []):
            ticket = pr.get('ticket')
            if not ticket or ticket in failed_issue_set:
                continue  # whole ticket already being retried
            pr_id_filter.setdefault(ticket, set()).add(pr.get('pr_id'))
        all_failed_tickets = failed_issue_set | set(pr_id_filter.keys())

        retry_issues = [i for i in all_issues if i.get('key') in all_failed_tickets]
        if pr_id_filter:
            debug_print("PR-level retry filter: "
                        + ", ".join(f"{t}={sorted(p for p in ids if p)}" for t, ids in pr_id_filter.items()))

        def _keep(row):
            ticket = row.get(DBIndexData.TICKET.name)
            if ticket in failed_issue_set:
                return False  # whole ticket reprocessed
            if ticket in pr_id_filter:
                return row.get(DBIndexData.PR_ID.name) not in pr_id_filter[ticket]  # keep good PRs
            return True
        filtered_cached = {'rows': [r for r in cached_data.get('rows', []) if _keep(r)]}
        return retry_issues, filtered_cached, all_failed_tickets, pr_id_filter

    def process_potentially_cached_sprint_data(self, thread_count: int, team_name: str, sprint: Dict[str, Any], mode: Mode, all_issues: List[Any], use_cache: bool):
        # SCRUM wrapper: cache key uses the sprint name; issues are already fetched.
        sprint_name = sprint.get('name', '')
        start_date = self.clean_date(sprint.get('startDate', ''))
        end_date = self.clean_date(sprint.get('endDate', ''))
        self._process_sprint_with_source_aware_cache(
            thread_count, team_name, sprint_name, start_date, end_date,
            sprint, mode, cacheable=use_cache, get_all_issues=lambda: all_issues)

    def _process_sprint_with_source_aware_cache(self, thread_count: int, team_name: str,
                                                cache_sprint_key: str, start_date: str, end_date: str,
                                                sprint_simulation: Dict[str, Any], mode: Mode,
                                                cacheable: bool, get_all_issues):
        """Shared SCRUM/Kanban cache orchestration, source-aware and additive.

        The cache file (one per sprint/cycle) records per-source completeness; a run only fetches
        the work sources it requires that aren't already present-and-complete, and merges them into
        the same file. `cache_sprint_key` is the sprint component of the cache key (sprint name for
        SCRUM, "" for Kanban); `sprint_simulation['name']` is the DB SPRINT value used for
        extract/render. `get_all_issues` is called lazily (returns None on fetch failure) so a full
        cache hit needs no Jira/SCM calls.
        """
        db_sprint_name = sprint_simulation.get('name', '')
        required = self._sources_for(self.work_source)
        debug_print(f"Processing {db_sprint_name}: workSource={self.work_source.value}, "
                    f"required={sorted(required)}, cacheable={cacheable}")

        meta = (SprintDataCache.load_cache_meta(team_name, cache_sprint_key, start_date, end_date)
                if cacheable else None)

        # FAST PATH: cache already satisfies every required source — no network.
        if meta is not None:
            cached_sources = meta.get('sources', {})
            if all(cached_sources.get(s, {}).get('complete') for s in required):
                debug_print(f"Complete cache for {db_sprint_name} (sources={sorted(required)}); loading...")
                self.load_cached_data_into_database(meta.get('data', {}))
                return

        all_issues = get_all_issues()
        if all_issues is None:
            print(f"   [ERROR] Could not fetch issues for {db_sprint_name}; keeping existing cache", file=sys.stderr)
            return

        if meta is None:
            # Fresh (no/incompatible cache, or not cacheable): process every required source.
            debug_print(f"No usable cache for {db_sprint_name}; processing {len(all_issues)} issues "
                        f"for sources {sorted(required)}...")
            per_source = self.get_issue_category_information(thread_count, sprint_simulation, mode,
                                                             all_issues, sources=required)
            if cacheable:
                data_to_cache = self.extract_database_data_for_cache(db_sprint_name)
                SprintDataCache.save_to_cache(team_name, cache_sprint_key, start_date, end_date,
                                              data_to_cache, per_source)
            return

        # MIXED/INCOMPLETE: keep good rows, top up absent sources, retry incomplete ones — per source.
        cached_rows = meta.get('data', {}).get('rows', [])
        cached_sources = meta.get('sources', {})
        new_records = dict(cached_sources)  # sources not touched this run retain their prior records

        present_complete = {s for s in required if cached_sources.get(s, {}).get('complete')}
        absent = {s for s in required if s not in cached_sources}
        present_incomplete = required - present_complete - absent
        debug_print(f"{db_sprint_name}: complete={sorted(present_complete)}, "
                    f"absent={sorted(absent)}, incomplete={sorted(present_incomplete)}")

        # 1) Seed the DB with rows we keep: sources not required this run (preserve verbatim),
        #    present-complete required sources (all rows), and the surviving rows of incomplete ones.
        rows_to_keep = [r for r in cached_rows if self._row_source(r) not in required]
        for s in present_complete:
            rows_to_keep.extend(r for r in cached_rows if self._row_source(r) == s)
        retry_for = {}  # source -> (retry_issues, pr_id_filter, record)
        for s in present_incomplete:
            rec = cached_sources.get(s, {})
            s_rows = {'rows': [r for r in cached_rows if self._row_source(r) == s]}
            retry_issues, filtered_cached_s, _, pr_id_filter = self._plan_incomplete_cache_retry(
                all_issues, rec.get('failed_issues', []), rec.get('failed_prs', []), s_rows)
            rows_to_keep.extend(filtered_cached_s['rows'])
            retry_for[s] = (retry_issues, pr_id_filter, rec)
        self.load_cached_data_into_database({'rows': rows_to_keep})

        # 2) Absent required sources: process all issues (single combined pass).
        if absent:
            res = self.get_issue_category_information(thread_count, sprint_simulation, mode,
                                                      all_issues, sources=absent)
            new_records.update(res)

        # 3) Present-but-incomplete required sources: retry per source.
        for s, (retry_issues, pr_id_filter, rec) in retry_for.items():
            if retry_issues:
                self._set_github_commit_retry_mode(True)
                try:
                    res = self.get_issue_category_information(thread_count, sprint_simulation, mode,
                                                              retry_issues, pr_id_filter, sources={s})
                finally:
                    self._set_github_commit_retry_mode(False)
                new_records.update(res)
            elif not rec.get('failed_issues') and not rec.get('failed_prs'):
                # Incompleteness was from processing_errors with no tracked failures: reprocess all.
                res = self.get_issue_category_information(thread_count, sprint_simulation, mode,
                                                          all_issues, sources={s})
                new_records.update(res)
            else:
                # Tracked failures no longer in this sprint: clear and mark the source complete.
                new_records[s] = {'complete': True, 'failed_issues': [], 'failed_prs': []}

        if cacheable:
            data_to_cache = self.extract_database_data_for_cache(db_sprint_name)
            SprintDataCache.save_to_cache(team_name, cache_sprint_key, start_date, end_date,
                                          data_to_cache, new_records)
        debug_print(f"Completed processing sprint: {db_sprint_name}")

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
        self._overall_capped_cols = set()
        for sprint in sprints:
            sprint_finder = [FlexiDBQueryColumn(DBIndexData.SPRINT.name, sprint)]
            self.find_rows_and_append_csv_data(sprint_finder, sb, overall_totals_row)

        self.append_summary(sb, overall_totals_row)

        data_indicator = self.team_name or self.board_id
        filename = self.command_line_options.outputCSV.replace('.csv', f'-{data_indicator}.csv')
        self.write_results_file(filename, '\n'.join(sb))

    def _should_filter_row_by_pr_title(self, row: FlexiDBRow) -> bool:
        if not self.ignore_pr_title_patterns:
            return False
        pr_title = row.get(DBData.PR_TITLE_FOR_FILTER.name, '')
        if not pr_title:
            return False
        for pattern in self.ignore_pr_title_patterns:
            try:
                if re.search(pattern, str(pr_title)):
                    return True
            except re.error:
                print(f"Invalid regex pattern in ignorePRTitleContent: {pattern}", file=sys.stderr)
        return False

    def _commit_message_matches_filter(self, message: str) -> bool:
        if not self.ignore_commit_message_patterns or not message:
            return False
        for pattern in self.ignore_commit_message_patterns:
            try:
                if re.search(pattern, message):
                    return True
            except re.error:
                print(f"Invalid regex pattern in ignoreCommitMessageContent: {pattern}", file=sys.stderr)
        return False

    @staticmethod
    def _pr_title(pull_request: Dict[str, Any]) -> str:
        """PR title from a Jira dev-status PR dict. Dev-status stores the title under 'name';
        fall back to 'title' for any other source."""
        return pull_request.get('name') or pull_request.get('title') or ''

    @staticmethod
    def _pr_source_branch(pull_request: Dict[str, Any]) -> Optional[str]:
        """The PR's source ('from'/head) branch from a dev-status PR dict. 'source' is normally
        {'branch': ..., 'url': ..., 'repository': {...}}; tolerate a bare string."""
        src = pull_request.get('source')
        if isinstance(src, dict):
            return src.get('branch')
        return src if isinstance(src, str) else None

    def _pr_title_is_down_merge(self, pr_title: str) -> bool:
        """True if the PR title matches any configured down-merge pattern (substring search)."""
        if not self.down_merge_title_patterns or not pr_title:
            return False
        for pattern in self.down_merge_title_patterns:
            try:
                if re.search(pattern, pr_title):
                    return True
            except re.error:
                print(f"Invalid regex pattern in downMergePRTitlePatterns: {pattern}", file=sys.stderr)
        return False

    def _head_branch_is_trunk(self, head_ref: Optional[str]) -> bool:
        """True if the PR's source branch is a trunk branch. Uses fullmatch so 'main' does not
        match 'maintenance' while 'release/.*' matches 'release/2026.6'."""
        if not self.down_merge_trunk_branches or not head_ref:
            return False
        for pattern in self.down_merge_trunk_branches:
            try:
                if re.fullmatch(pattern, head_ref):
                    return True
            except re.error:
                print(f"Invalid regex pattern in downMergeTrunkBranches: {pattern}", file=sys.stderr)
        return False

    def _is_merge_commit(self, commit: Dict[str, Any], message: str) -> bool:
        """A merge commit has 2+ parents. Prefer the authoritative parent count
        (GitHub normalized `parents_count`, Bitbucket raw `parents` list); fall
        back to the message regex only when parent data is unavailable."""
        parent_count = commit.get("parents_count")
        if parent_count is None and isinstance(commit.get("parents"), list):
            parent_count = len(commit["parents"])
        if parent_count is not None:
            return parent_count > 1
        return bool(re.match(self.MERGE_COMMIT_REGEX, message))

    @staticmethod
    def _brought_in_oids(commits: List[Dict[str, Any]], head_oid: Optional[str]) -> set:
        """Identify commits merged in from another branch (a merge's 2nd+ parent side).

        Walks first-parent only from the PR head through the fetched commit set; that linear
        chain is the PR's own "mainline" (including merge nodes). Every fetched commit not on the
        chain was brought in by a merge and was already authored/counted in its own PR.

        Returns the set of brought-in oids. Empty (no-op) when head_oid is missing/unknown or
        parent oids aren't available (e.g. Bitbucket), so callers degrade gracefully.
        """
        by_oid = {c.get('id'): c for c in commits if c.get('id')}
        if not head_oid or head_oid not in by_oid:
            return set()
        mainline = set()
        cur = head_oid
        while cur and cur in by_oid and cur not in mainline:
            mainline.add(cur)
            parent_oids = by_oid[cur].get('parent_oids') or []
            cur = parent_oids[0] if parent_oids else None
        return set(by_oid) - mainline

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
        sprint_capped_cols = set()  # columns capped on any row this sprint, for the Sprint Totals '*'

        total_prs = 0
        non_declined_prs = 0

        # Render is filtered by the active work source: PR shows only PR rows, COMMIT shows only
        # the synthetic (commits) rows, BOTH shows everything but de-dupes commit entries whose SHA
        # was already counted by one of the ticket's PR commits. Cached rows for the other source
        # stay in the file untouched; they're simply not rendered here.
        render_prs = self.work_source in (WorkSource.PR, WorkSource.BOTH)
        render_commits = self.work_source in (WorkSource.COMMIT, WorkSource.BOTH)
        ticket_pr_shas = {}  # ticket -> set(sha) counted by its PR rows (BOTH-mode render dedupe)
        if self.work_source == WorkSource.BOTH:
            for r in rows:
                if r.get(DBIndexData.PR_ID.name) != self.COMMITS_PR_ID:
                    for e in (r.get(DBData.COMMIT_DATA.name) or []):
                        if isinstance(e, dict) and e.get('sha'):
                            ticket_pr_shas.setdefault(r.get(DBIndexData.TICKET.name), set()).add(e['sha'])

        for row in rows:
            row_user = row.get(DBIndexData.USER.name)
            author = row.get(DBData.AUTHOR.name)
            pr_status = row.get(DBIndexData.PR_STATUS.name)
            pr_id = row.get(DBIndexData.PR_ID.name)
            is_commit_row = (pr_id == self.COMMITS_PR_ID)

            # Mode filter: skip rows whose source isn't rendered in the active work source.
            if is_commit_row and not render_commits:
                continue
            if not is_commit_row and not render_prs:
                continue

            # Filter out rows by PR title patterns (entire PR is excluded)
            if self._should_filter_row_by_pr_title(row):
                continue

            output_row = FlexiDBRow(row)

            # Derive COMMITS and line counts by summing per-commit COMMIT_DATA rather than the
            # stored totals. Merge commits are excluded unless --includeMergeCommits, and
            # brought-in (merged-from-another-branch) commits unless --includeBroughtInCommits.
            # Commits at/above maxCommitSize (e.g. large vendoring commits) are excluded from BOTH
            # COMMITS and the line totals. COMMITS counts only commits whose lines are counted.
            include_merges = self.command_line_options.includeMergeCommits
            include_brought_in = self.command_line_options.includeBroughtInCommits
            commit_count = 0
            commit_added = 0
            commit_removed = 0
            commits_excluded = False  # a real commit was dropped from COMMITS (merge/brought-in/oversized)
            for commit_entry in (output_row.get(DBData.COMMIT_DATA.name) or []):
                if not isinstance(commit_entry, dict):
                    continue
                entry_type = commit_entry.get("type")
                if entry_type == "skipped":
                    continue  # collection-time skip marker (e.g. down-merge); never a real commit
                if (is_commit_row and self.work_source == WorkSource.BOTH
                        and commit_entry.get("sha")
                        and commit_entry["sha"] in ticket_pr_shas.get(row.get(DBIndexData.TICKET.name), ())):
                    # Already counted via a PR commit for this ticket; de-dupe at render (not a
                    # capped/excluded case — don't flip commits_excluded / the '*' flag).
                    continue
                if entry_type == "merge" and not include_merges:
                    commits_excluded = True
                    continue
                if entry_type == "brought-in" and not include_brought_in:
                    commits_excluded = True  # merged in from another branch; already counted in its own PR
                    continue
                entry_added = commit_entry.get("additions", 0) or 0
                entry_removed = commit_entry.get("deletions", 0) or 0
                if self.max_commit_size and (entry_added + entry_removed) >= self.max_commit_size:
                    commits_excluded = True  # oversized; excluded from both COMMITS and the line totals
                    continue
                commit_count += 1
                commit_added += entry_added
                commit_removed += entry_removed
            # None renders as empty (ConvertZerosToEmptyOutputFilter handles zeros too).
            output_row[UserActivity.COMMITS.name] = commit_count or None
            output_row[UserActivity.COMMIT_ADDED.name] = commit_added or None
            output_row[UserActivity.COMMIT_REMOVED.name] = commit_removed or None

            # Cap PR line counts at the authored-commit totals. PR_ADDED/PR_REMOVED is the whole-PR
            # net diff — credited entirely to the PR author and not sprint-scoped — so it can
            # massively overstate one developer's contribution (merged-in / others' / carryover
            # work). Never credit more PR lines than authored commit lines; flag capped cells with
            # a trailing '*' at render (kept numeric here so the totals below stay correct).
            capped_cols = []
            if (output_row.get(UserActivity.PR_ADDED.name) or 0) > commit_added:
                output_row[UserActivity.PR_ADDED.name] = commit_added or None
                capped_cols.append(UserActivity.PR_ADDED.name)
            if (output_row.get(UserActivity.PR_REMOVED.name) or 0) > commit_removed:
                output_row[UserActivity.PR_REMOVED.name] = commit_removed or None
                capped_cols.append(UserActivity.PR_REMOVED.name)
            if commits_excluded:
                # COMMITS excluded merge/brought-in/oversized commits; flag it with '*' too.
                capped_cols.append(UserActivity.COMMITS.name)
            # Propagate to the totals so aggregated counts are flagged when any component was.
            sprint_capped_cols.update(capped_cols)
            self._overall_capped_cols.update(capped_cols)

            if not (row_user and author and row_user.casefold() == author.casefold()):
                output_row[DBIndexData.PR_STATUS.name] = ""

            row_rules = ([AppendAsteriskOutputFilter(capped_cols)] + self.STANDARD_OUTPUT_RULES
                         if capped_cols else self.STANDARD_OUTPUT_RULES)
            sb.append(output_row.to_csv(column_order, row_rules))

            if (row_user and author and row_user.casefold() == author.casefold()
                    and pr_id != self.COMMITS_PR_ID):
                # Synthetic (commits) rows have no PR, so they never count toward PR totals.
                total_prs += 1
                if pr_status and pr_status.upper() != "DECLINED":
                    non_declined_prs += 1

            for column in column_order:
                # Read from output_row so totals reflect the recomputed (size-filtered) commit counts.
                value = output_row.get(column)
                if isinstance(value, (int, float)):
                    long_value = int(value)
                    sprint_existing = sprint_totals_row.get(column, 0)
                    overall_existing = overall_totals_row.get(column, 0)

                    if isinstance(sprint_existing, str):
                        sprint_existing = 0
                    if isinstance(overall_existing, str):
                        overall_existing = 0

                    sprint_totals_row[column] = sprint_existing + long_value
                    overall_totals_row[column] = overall_existing + long_value

        sprint_totals_row[self.TOTAL_PRS] = total_prs
        sprint_totals_row[self.NON_DECLINED_PRS] = non_declined_prs

        overall_pr_count = overall_totals_row.get(self.TOTAL_PRS, 0)
        overall_non_declined = overall_totals_row.get(self.NON_DECLINED_PRS, 0)
        if isinstance(overall_pr_count, str):
            overall_pr_count = 0
        if isinstance(overall_non_declined, str):
            overall_non_declined = 0

        overall_totals_row[self.TOTAL_PRS] = overall_pr_count + total_prs
        overall_totals_row[self.NON_DECLINED_PRS] = overall_non_declined + non_declined_prs

        self.append_totals_info(sb, "Sprint Totals", sprint_totals_row, sprint_capped_cols)
        sb.append("")

    def append_summary(self, sb: list, overall_totals_row: FlexiDBRow):
        self.append_totals_info(sb, "Overall Totals", overall_totals_row, self._overall_capped_cols)

    def write_results_file(self, filename: str, data: str):
        print(f"Writing file: {filename}")
        with open(filename, 'w') as f:
            f.write(data)

    def get_issue_category_information(self, thread_count: int, sprint: Dict[str, Any], mode: Mode, issue_list: List[Any],
                                       pr_id_filter: Optional[Dict[str, set]] = None,
                                       sources: Optional[set] = None) -> Dict[str, Dict[str, Any]]:
        # sources: set of work-source values ("pr"/"commit") to process this pass; defaults to the
        # active --workSource. Returns a per-source record dict:
        #   {"pr": {"complete": bool, "failed_issues": [...], "failed_prs": [...]}, "commit": {...}}
        # pr_id_filter: {ticket -> set of PR ids (no '#')}. When set, only those PRs are
        # processed for that ticket (PR-level retry); other tickets are unaffected.
        sprint_name = sprint.get('name', '')
        start_date = self.clean_date(sprint.get('startDate', ''))
        end_date = self.clean_date(sprint.get('endDate', ''))

        debug_print(f"getIssueCategoryInformation started for sprint: {sprint_name}")

        # Parse sprint times
        sprint_start_time = datetime.strptime(sprint.get('startDate', ''), "%d/%b/%y %I:%M %p").replace(tzinfo=timezone.utc)
        sprint_end_time = datetime.strptime(sprint.get('endDate', ''), "%d/%b/%y %I:%M %p").replace(tzinfo=timezone.utc)
        sprint_start_ms = sprint_start_time.timestamp() * 1000
        sprint_end_ms = sprint_end_time.timestamp() * 1000

        isolated_ticket = self.command_line_options.isolateTicket or None

        counter = [0]  # Use list for mutable counter in nested function
        lock = threading.Lock()
        processing_errors = []  # Track errors to prevent cache on failure
        failed_issue_keys = []  # Track ticket keys that failed to process (shared: counts against every source run)
        failed_pr_entries = []  # Track individual PRs that failed to process (PR source only)
        failed_commit_issue_keys = []  # Track tickets whose commit pass failed (commit source only)

        # Pre-pass: fetch all PR data in parallel to build the primary-ticket map for activity attribution.
        # A PR linked to multiple tickets is attributed to the ticket whose key appears in the PR title
        # (by convention e.g. "AI-164: ..."); falls back to first-found when no title match exists.
        def _apply_pr_filter(t, prs):
            # PR-level retry: keep only the failed PR ids for this ticket.
            if pr_id_filter and t in pr_id_filter:
                return [pr for pr in prs if pr.get('id', '').lstrip('#') in pr_id_filter[t]]
            return prs

        if sources is None:
            sources = self._sources_for(self.work_source)
        process_prs = WorkSource.PR.value in sources
        process_commits = WorkSource.COMMIT.value in sources

        def prefetch_prs(issue):
            t = issue.get('key')
            if isolated_ticket and t != isolated_ticket:
                return
            try:
                if process_prs:
                    prs = self.jira_rest.get_ticket_pull_request_info(str(issue.get('id')))
                    self._ticket_pr_data[t] = _apply_pr_filter(t, prs)
                if process_commits:
                    self._ticket_commit_data[t] = self.jira_rest.get_ticket_commit_info(str(issue.get('id')))
            except Exception:
                pass  # main pass will retry with full backoff logic

        with ThreadPoolExecutor(max_workers=thread_count) as pre_executor:
            list(pre_executor.map(prefetch_prs, issue_list))

        # Count total GitHub PRs to fetch for progress tracking
        self._github_pr_fetch_index = 0
        self._github_pr_fetch_total = sum(
            1 for prs in self._ticket_pr_data.values()
            for pr in prs
            if 'github.com/' in (pr.get('url') or '').lower()
        )

        for ticket_key, prs in self._ticket_pr_data.items():
            for pr in prs:
                pr_id = str(pr.get('id', ''))
                pr_title = self._pr_title(pr)
                if pr_id and pr_title:
                    activity_key = (sprint_name, pr_id)
                    if activity_key not in self._pr_primary_ticket:
                        if re.search(rf'\b{re.escape(ticket_key)}\b', pr_title, re.IGNORECASE):
                            self._pr_primary_ticket[activity_key] = ticket_key

        debug_print(f"Starting thread pool with {thread_count} threads for {len(issue_list)} issues")

        def process_issue(issue):
            import traceback
            max_retries = 3
            retry_delay = 2

            try:
                ticket = issue.get('key')

                if isolated_ticket and ticket != isolated_ticket:
                    return

                issue_id = issue.get('id')

                pull_requests = self._ticket_pr_data.get(ticket)
                last_error = None

                if process_prs and pull_requests is None:
                    pull_requests = []
                    for attempt in range(1, max_retries + 1):
                        try:
                            pull_requests = _apply_pr_filter(ticket, self.jira_rest.get_ticket_pull_request_info(str(issue_id)))
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
                                    failed_issue_keys.append(ticket)
                        except RESTException as re:
                            if re.status_code not in [403, 404]:  # FORBIDDEN, NOT_FOUND
                                return
                            with lock:
                                failed_issue_keys.append(ticket)
                            break

                    if last_error:
                        raise last_error

                pull_requests = pull_requests or []
                ticket_commits = self._ticket_commit_data.get(ticket) if process_commits else None

                with lock:
                    counter[0] += 1
                    _B = "\033[94m"
                    _R = "\033[0m"
                    commit_note = f", {len(ticket_commits or [])} commits" if process_commits else ""
                    print(f"   {_B}{counter[0]}/{len(issue_list)}: {ticket} / Issue {issue_id} "
                          f"has {len(pull_requests)} PRs{commit_note}{_R}")

                for pull_request in pull_requests:
                    try:
                        self.process_pull_request(ticket, pull_request, sprint_name, start_date, end_date, sprint_start_ms, sprint_end_ms, mode)
                    except Exception as e:
                        pr_id = pull_request.get('id', '').lstrip('#')
                        pr_url = pull_request.get('url', '')
                        with lock:
                            failed_pr_entries.append({"ticket": ticket, "pr_id": pr_id, "pr_url": pr_url})
                        with lock:
                            print(f"   [ERROR] Failed to process PR {pr_id} for {ticket}: {e}", file=sys.stderr)

                # Commit pass (WorkSource.COMMIT / BOTH). Runs AFTER the PR loop so BOTH-mode
                # de-dupe can read the SHAs the PRs just counted for this ticket.
                if process_commits:
                    if ticket_commits is None:
                        try:
                            ticket_commits = self.jira_rest.get_ticket_commit_info(str(issue_id))
                        except Exception as e:
                            ticket_commits = []
                            with lock:
                                failed_commit_issue_keys.append(ticket)
                                print(f"   [ERROR] Failed to fetch commits for {ticket}: {e}", file=sys.stderr)
                    try:
                        # Full commit set is always stored; BOTH-mode de-dupe happens at render time.
                        self.process_ticket_commits(ticket, ticket_commits, sprint_name, start_date,
                                                    end_date, sprint_start_ms, sprint_end_ms, mode)
                    except Exception as e:
                        with lock:
                            failed_commit_issue_keys.append(ticket)
                            print(f"   [ERROR] Failed to process commits for {ticket}: {e}", file=sys.stderr)
            except Exception as e:
                with lock:
                    processing_errors.append(e)
                    failed_issue_keys.append(issue.get('key'))
                print(f"Error processing issue: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

        skips_before = len(self._down_merge_skips)
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            list(executor.map(process_issue, issue_list))

        debug_print(f"Parallel processing completed for {len(issue_list)} issues")
        debug_print(f"getIssueCategoryInformation finished for sprint: {sprint_name}")

        new_skips = self._down_merge_skips[skips_before:]
        if new_skips:
            by_branch = sum(1 for r in new_skips if r.startswith("down-merge PR (source"))
            by_title = len(new_skips) - by_branch
            print(f"   \033[92mDown-merge skips for {sprint_name}: {len(new_skips)} PRs "
                  f"(source-branch: {by_branch}, title: {by_title})\033[0m", file=sys.stderr)

        if processing_errors:
            print(f"\033[93mWarning: Encountered {len(processing_errors)} errors during issue processing. First error: {processing_errors[0]}\033[0m", file=sys.stderr)

        # Per-source completeness. failed_issue_keys (PR-fetch / top-level exceptions) and
        # processing_errors are issue-level and count against every source run this pass;
        # failed_pr_entries is PR-exclusive and failed_commit_issue_keys is commit-exclusive.
        shared_fail = bool(failed_issue_keys) or bool(processing_errors)
        result: Dict[str, Dict[str, Any]] = {}
        if process_prs:
            result[WorkSource.PR.value] = {
                "complete": not shared_fail and not failed_pr_entries,
                "failed_issues": list(failed_issue_keys),
                "failed_prs": failed_pr_entries,
            }
        if process_commits:
            result[WorkSource.COMMIT.value] = {
                "complete": not shared_fail and not failed_commit_issue_keys,
                "failed_issues": sorted(set(failed_issue_keys) | set(failed_commit_issue_keys)),
                "failed_prs": [],
            }
        return result

    def _retry_rest_call(self, func, max_retries: int = 3, retry_delay: int = 2):
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except NeedsRetryException as nre:
                last_error = nre
                if attempt < max_retries:
                    wait_time = nre.get_retry_after() or retry_delay
                    print(f"Rate limited (attempt {attempt}/{max_retries}). "
                          f"Waiting {wait_time}s as indicated by server...", file=sys.stderr)
                    time.sleep(wait_time)
                else:
                    print(f"Rate limit after {max_retries} attempts: {nre}", file=sys.stderr)
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

    def _record_skipped_pr(self, sprint_name: str, ticket: str, pr_id: str, marker_author: str,
                           pr_status: str, start_date: str, end_date: str, pr_title: str, skip_reason: str):
        """Write a visible [skipped] marker row for a PR skipped at collection time (no commit
        data fetched). The row shows the PR title and a single skipped COMMIT_DATA entry."""
        self._down_merge_skips.append(skip_reason)  # list.append is atomic under the GIL
        print(f"   {ticket}/{pr_id}: skipped {skip_reason}", file=sys.stderr)
        index_lookup = self.create_index_lookup(sprint_name, ticket, pr_id, marker_author, pr_status)
        self.populate_baseline_db_info(index_lookup, start_date, end_date, marker_author)
        if pr_title:
            self.database.set_value(index_lookup, DBData.PR_TITLE_FOR_FILTER.name, pr_title)
        self.database.append(index_lookup, DBData.COMMIT_DATA.name,
                             {"message": f"[skipped: {skip_reason}]", "additions": 0,
                              "deletions": 0, "type": "skipped"})

    def process_pull_request(self, ticket: str, pull_request: Dict[str, Any], sprint_name: str, start_date: str,
                            end_date: str, sprint_start_ms: float, sprint_end_ms: float, mode: Mode):
        pr_id = pull_request.get('id', 'UNKNOWN')
        pr_url = pull_request.get('url', '')
        debug_print(f"Processing PR: {ticket}/{pr_id}")
        if not pr_url:
            debug_print(f"PR {ticket}/{pr_id}: WARNING - No URL in pull_request data. Keys: {list(pull_request.keys())}")
        debug_print(f"PR {ticket}/{pr_id}: url={pr_url}, keys={list(pull_request.keys())}")

        is_github = 'github.com/' in pr_url.lower()
        source_control_rest = self.github_rest if is_github else self.bitbucket_rest

        pr_url = source_control_rest.api_convert(pr_url)
        pr_id = pull_request.get('id', '').lstrip('#')
        pr_status = pull_request.get('status', '')
        pr_author = source_control_rest.map_user_to_jira_name(pull_request.get('author', ''))
        pr_title = self._pr_title(pull_request)

        # Collection-time down-merge skip: a down-merge PR (trunk merged into a branch) carries
        # hundreds of already-counted commits and is absurdly expensive to fetch. Both signals —
        # the source/from branch and the title — come from the Jira dev-status payload, so this
        # runs for free before any cache/network work, on every PR. The branch rule is preferred;
        # title is the fallback. Skipped PRs get a visible marker rather than disappearing.
        skip_enabled = not self.command_line_options.includeDownMergePRs
        if skip_enabled:
            src_branch = self._pr_source_branch(pull_request)
            skip_reason = None
            if self._head_branch_is_trunk(src_branch):
                skip_reason = f"down-merge PR (source={src_branch})"
            elif self._pr_title_is_down_merge(pr_title):
                skip_reason = f"down-merge PR (title: {pr_title})"
            debug_print(f"PR {ticket}/{pr_id}: down-merge check title={pr_title!r} "
                        f"source={src_branch} -> {skip_reason or 'keep'}")
            if skip_reason:
                marker_author = pr_author or pull_request.get('author') or 'unknown'
                self._record_skipped_pr(sprint_name, ticket, pr_id, marker_author, pr_status,
                                        start_date, end_date, pr_title, skip_reason)
                return

        # For GitHub, fetch all PR data in one GraphQL call; Bitbucket uses REST
        if is_github:
            # Two-tier cache: check memory first, then disk (merged PRs), then fetch from GitHub
            pr_full = self._pr_mem_cache.get(pr_url)

            if pr_full is None:
                pr_full = self._pr_data_cache.load(ticket, pr_id)
                if pr_full is not None:
                    debug_print(f"PR {ticket}/{pr_id}: disk cache hit")

            if pr_full is None:
                self._github_pr_fetch_index += 1
                source_control_rest.set_pr_progress(self._github_pr_fetch_index, self._github_pr_fetch_total)
                pr_full = self._retry_rest_call(lambda: source_control_rest.get_pull_request_full(pr_url))
                if pr_full is not None and pr_full.get("merged_ms", 0) > 0:
                    self._pr_data_cache.save(ticket, pr_id, pr_url, pr_full)

            if pr_full is not None:
                self._pr_mem_cache[pr_url] = pr_full

            pr_commits = pr_full.get("commits", []) if pr_full else None
            _github_created_ms = pr_full.get("created_ms", 0) if pr_full else 0
            _github_merged_ms = pr_full.get("merged_ms", 0) if pr_full else 0
            _github_activities = pr_full.get("activities") if pr_full else None
            # Classify commits merged in from other branches (a merge's 2nd-parent side) so they
            # aren't double-counted; they were already authored/counted in their own PR.
            brought_in_oids = self._brought_in_oids(pr_commits or [], pr_full.get("head_oid") if pr_full else None)
        else:
            pr_commits = self._retry_rest_call(lambda: source_control_rest.get_commits(pr_url))
            _github_created_ms = None
            _github_merged_ms = None
            _github_activities = None
            brought_in_oids = set()
        debug_print(f"PR {ticket}/{pr_id}: Got {len(pr_commits) if pr_commits else 0} commits"
                    + (f", {len(brought_in_oids)} brought-in" if brought_in_oids else ""))

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

        # Store PR title in database for filtering (pr_title captured earlier, pre-fetch)
        if pr_title:
            title_index = self.create_index_lookup(sprint_name, ticket, pr_id, pr_author, pr_status)
            self.database.set_value(title_index, DBData.PR_TITLE_FOR_FILTER.name, pr_title)

        # Check if PR title matches ignore patterns
        if self.ignore_pr_title_patterns and pr_title:
            for pattern in self.ignore_pr_title_patterns:
                try:
                    if re.search(pattern, pr_title):
                        debug_print(f"PR {ticket}/{pr_id}: Filtered by PR title pattern (title: {pr_title})")
                        return
                except re.error:
                    print(f"Invalid regex pattern in ignorePRTitleContent: {pattern}", file=sys.stderr)

        # Increment OPENED if the PR was created within the sprint/cycle window
        pr_created_ms = (_github_created_ms if is_github
                         else self._retry_rest_call(lambda: source_control_rest.get_pr_created_ms(pr_url)))
        if pr_created_ms and (mode == Mode.KANBAN or sprint_start_ms <= pr_created_ms < sprint_end_ms):
            debug_print(f"PR {ticket}/{pr_id}: OPENED for {pr_author}")
            open_index = self.create_index_lookup(sprint_name, ticket, pr_id, pr_author, pr_status)
            self.populate_baseline_db_info(open_index, start_date, end_date, pr_author)
            self.increment_counter(open_index, UserActivity.OPENED)

        # For GitHub PRs, MERGED doesn't come from activities — use pre-fetched GraphQL timestamp
        if is_github and pr_status == 'MERGED':
            pr_merged_ms = _github_merged_ms
            if pr_merged_ms and (mode == Mode.KANBAN or sprint_start_ms <= pr_merged_ms < sprint_end_ms):
                debug_print(f"PR {ticket}/{pr_id}: MERGED for {pr_author}")
                merged_index = self.create_index_lookup(sprint_name, ticket, pr_id, pr_author, pr_status)
                self.populate_baseline_db_info(merged_index, start_date, end_date, pr_author)
                self.increment_counter(merged_index, UserActivity.MERGED)

        # Determine whether this ticket should process activities for this PR.
        # If the PR title contains this ticket's key (by convention), it is the primary ticket.
        # Otherwise fall back to first-found deduplication to avoid double-counting.
        activity_key = (sprint_name, pr_id)
        primary_ticket = self._pr_primary_ticket.get(activity_key)
        if primary_ticket is not None:
            should_process_activities = (ticket == primary_ticket)
        else:
            should_process_activities = activity_key not in self._counted_pr_activities
            if should_process_activities:
                self._counted_pr_activities.add(activity_key)

        if not should_process_activities:
            pr_activities = None
        elif is_github and _github_activities is not None:
            pr_activities = _github_activities
        else:
            pr_activities = self._retry_rest_call(lambda: source_control_rest.get_activities(pr_url))
        debug_print(f"PR {ticket}/{pr_id}: Got {len(pr_activities) if pr_activities else 0} activities")

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

                if mode != Mode.KANBAN and (sprint_start_ms > commit_timestamp or commit_timestamp >= sprint_end_ms):
                    continue

                user_name = commit.get('committer', {}).get('name', '')

                if user_name.lower() in self.IGNORE_USERS:
                    continue

                index_lookup = self.create_index_lookup(sprint_name, ticket, pr_id, user_name, pr_status)
                self.populate_baseline_db_info(index_lookup, start_date, end_date, pr_author)

                commit_message = re.sub(r'(\r|\n)?\n', '  ', commit.get('message', '').strip())
                commit_filtered = self._commit_message_matches_filter(commit_message)
                if self._is_merge_commit(commit, commit_message):
                    commit_type = "merge"
                elif commit_sha in brought_in_oids:
                    commit_type = "brought-in"  # merged in from another branch; already counted there
                else:
                    commit_type = "normal"

                commit_url = commit.get('url') or pr_url
                # Every commit is recorded in COMMIT_DATA with its type; report generation decides
                # which to count (merges/brought-in excluded unless their include flag is set). The
                # stored COMMIT_ADDED/COMMIT_REMOVED counters (which gate PR-diff fetching) only
                # accumulate normal, non-filtered commits, matching legacy behavior.
                c_add = 0
                c_del = 0
                if commit_type in ("merge", "brought-in"):
                    # Use inline values only; skip the per-commit REST diff fallback (these aren't
                    # counted by default and the brought-in ones were already measured in their PR).
                    c_add = commit.get("additions") or 0
                    c_del = commit.get("deletions") or 0
                elif not commit_filtered:
                    if is_github and ("additions" in commit or "deletions" in commit):
                        c_add = commit.get("additions", 0)
                        c_del = commit.get("deletions", 0)
                        if c_add > 0 or c_del > 0:
                            c_add, c_del = self.process_diffs(self.COMMIT_PREFIX, {"additions": c_add, "deletions": c_del}, index_lookup)
                        elif commit.get("changedFilesIfAvailable", 0) > 0:
                            # Large-repo fallback: GraphQL returned 0/0 but files changed
                            diffs_response = self._retry_rest_call(lambda: source_control_rest.get_commit_diffs(commit_url, commit_sha))
                            if diffs_response:
                                c_add, c_del = self.process_diffs(self.COMMIT_PREFIX, diffs_response, index_lookup)
                    else:
                        diffs_response = self._retry_rest_call(lambda: source_control_rest.get_commit_diffs(commit_url, commit_sha))
                        if diffs_response:
                            c_add, c_del = self.process_diffs(self.COMMIT_PREFIX, diffs_response, index_lookup)

                self.database.increment_field(index_lookup, UserActivity.COMMITS.name)
                # No add_line_number: COMMIT_DATA holds dicts, which must NOT be stringified.
                # "sha" lets WorkSource.BOTH de-dupe commit-view commits against these PR commits.
                self.database.append(index_lookup, DBData.COMMIT_DATA.name,
                                     {"message": commit_message, "additions": c_add, "deletions": c_del,
                                      "type": commit_type, "sha": commit_sha})

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
    def _coerce_epoch_ms(value: Any) -> Optional[int]:
        """Normalize a dev-status timestamp to epoch milliseconds. The repository detail returns
        authorTimestamp as an epoch-ms int, a numeric string (which may be in *seconds*), or an
        ISO-8601 string depending on the provider — return None when it can't be parsed so callers
        can default it. Numeric values that look like epoch *seconds* are scaled to ms so the
        sprint-window comparison (always in ms) doesn't silently reject every commit."""
        def _normalize(v: int) -> int:
            # Epoch seconds for current dates are ~1.7e9; epoch ms are ~1.7e12. Anything below
            # ~1e11 must be seconds (or finer) — scale up so the comparison stays in ms.
            return v * 1000 if 0 < v < 100_000_000_000 else v

        if value is None:
            return None
        if isinstance(value, (int, float)):
            return _normalize(int(value))
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            try:
                return _normalize(int(s))
            except ValueError:
                pass
            try:
                # Normalize 'Z' and a colon-less offset (e.g. +0000) for fromisoformat.
                iso = s.replace('Z', '+00:00')
                iso = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', iso)
                return int(datetime.fromisoformat(iso).timestamp() * 1000)
            except ValueError:
                return None
        return None

    def process_ticket_commits(self, ticket: str, commits: List[Dict[str, Any]], sprint_name: str,
                               start_date: str, end_date: str, sprint_start_ms: float, sprint_end_ms: float,
                               mode: Mode):
        """Process commits sourced from the Jira dev-status commit view (no PR involved).

        Mirrors the commit loop in process_pull_request: same sprint-window filter, committer
        attribution, IGNORE_USERS, merge/maxCommitSize handling, and per-commit diff fetch for
        line counts. Loose commits roll up under the synthetic COMMITS_PR_ID bucket per ticket.
        'brought-in' is never assigned here — it requires a PR head/first-parent walk we don't have.

        The FULL commit set is always stored (each entry keeps its 'sha'); BOTH-mode de-dupe
        against PR commits happens at render time so the cached commit rows are mode-independent.
        """
        if not commits:
            return
        _kept = 0
        _out_of_window = 0
        _ignored = 0
        for commit in commits:
            sha = commit.get('id') or commit.get('displayId') or ''

            # dev-status uses authorTimestamp (often a string); coerce to epoch ms so the
            # window check matches the PR path's numeric comparison.
            raw_ts = commit.get('committerTimestamp')
            if raw_ts is None:
                raw_ts = commit.get('authorTimestamp')
            commit_timestamp = self._coerce_epoch_ms(raw_ts) or 0
            if mode != Mode.KANBAN and (sprint_start_ms > commit_timestamp or commit_timestamp >= sprint_end_ms):
                _out_of_window += 1
                debug_print(f"  commit {sha[:8]} ts={raw_ts!r}->{commit_timestamp} outside "
                            f"[{int(sprint_start_ms)},{int(sprint_end_ms)}) for {ticket}")
                continue

            # dev-status commit author/committer is a display name block.
            author_block = commit.get('committer') or commit.get('author') or {}
            user_name = author_block.get('name', '') if isinstance(author_block, dict) else ''
            if not user_name or user_name.lower() in self.IGNORE_USERS:
                _ignored += 1
                debug_print(f"  commit {sha[:8]} skipped (user={user_name!r}) for {ticket}")
                continue
            _kept += 1

            repo_url = (commit.get('_repository') or {}).get('url')
            is_github = 'github.com/' in (repo_url or '').lower()
            scm = self.github_rest if is_github else self.bitbucket_rest

            index_lookup = self.create_index_lookup(sprint_name, ticket, self.COMMITS_PR_ID, user_name, "")
            # No separate PR author for loose commits; attribute authorship to the committer.
            self.populate_baseline_db_info(index_lookup, start_date, end_date, user_name)

            commit_message = re.sub(r'(\r|\n)?\n', '  ', (commit.get('message') or '').strip())
            commit_filtered = self._commit_message_matches_filter(commit_message)

            # dev-status may flag merges with a 'merge' bool; map it to a parent count so the
            # authoritative _is_merge_commit path applies (it otherwise falls back to the regex).
            merge_probe = dict(commit)
            if 'parents_count' not in merge_probe and isinstance(commit.get('merge'), bool):
                merge_probe['parents_count'] = 2 if commit.get('merge') else 1
            commit_type = "merge" if self._is_merge_commit(merge_probe, commit_message) else "normal"

            c_add = 0
            c_del = 0
            if commit_type == "merge":
                # Use inline values only (dev-status rarely supplies them); skip the diff fetch.
                c_add = commit.get("additions") or 0
                c_del = commit.get("deletions") or 0
            elif not commit_filtered and sha and repo_url:
                diffs_response = self._retry_rest_call(lambda: scm.get_repo_commit_diffs(repo_url, sha))
                if diffs_response:
                    c_add, c_del = self.process_diffs(self.COMMIT_PREFIX, diffs_response, index_lookup)

            self.database.increment_field(index_lookup, UserActivity.COMMITS.name)
            self.database.append(index_lookup, DBData.COMMIT_DATA.name,
                                 {"message": commit_message, "additions": c_add, "deletions": c_del,
                                  "type": commit_type, "sha": sha})

        debug_print(f"process_ticket_commits {ticket}: {len(commits)} commits "
                    f"-> {_kept} kept, {_out_of_window} out-of-window, {_ignored} ignored-user")

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

    def process_diffs(self, prefix: str, diffs_response: Dict[str, Any], index_lookup: List[FlexiDBQueryColumn]) -> Tuple[int, int]:
        """Accumulate diff line counts into the prefixed counters and return the (additions, deletions) computed."""
        additions = diffs_response.get('additions')
        deletions = diffs_response.get('deletions')

        if diffs_response.get('stats'):
            additions = diffs_response['stats'].get('additions')
            deletions = diffs_response['stats'].get('deletions')

        if additions is not None or deletions is not None:
            additions = additions or 0
            deletions = deletions or 0
            self.increment_counter(index_lookup, UserActivity[prefix + "ADDED"], additions)
            self.increment_counter(index_lookup, UserActivity[prefix + "REMOVED"], deletions)
            return additions, deletions

        diffs = diffs_response.get('diffs', [])
        if not diffs:
            return 0, 0

        added_calculated = 0
        removed_calculated = 0

        for diff in diffs:
            for hunk in diff.get('hunks', []):
                for segment in hunk.get('segments', []):
                    if segment.get('type') == 'ADDED':
                        added_calculated += len(segment.get('lines', []))
                    elif segment.get('type') == 'REMOVED':
                        removed_calculated += len(segment.get('lines', []))

        self.increment_counter(index_lookup, UserActivity[prefix + "ADDED"], added_calculated)
        self.increment_counter(index_lookup, UserActivity[prefix + "REMOVED"], removed_calculated)
        return added_calculated, removed_calculated

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
            self.populate_baseline_db_info(pr_author_lookup,
                                          self.database.get_value(original_index_lookup, DBData.START_DATE.name),
                                          self.database.get_value(original_index_lookup, DBData.END_DATE.name),
                                          pr_author)
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

    def append_totals_info(self, sb: list, totals_description: str, totals_row: FlexiDBRow, capped_cols: set = None):
        capped_cols = capped_cols or set()
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
                    # Flag aggregated counts whose components were capped (e.g. PR_ADDED/PR_REMOVED/COMMITS).
                    asterisk = "*" if column_name in capped_cols else ""
                    totals_row[column_name] = f"{column_name}: {value}{asterisk}"
            except KeyError:
                pass

        sb.append(totals_row.to_csv(column_order))

    @staticmethod
    def clean_date(date_str: str) -> str:
        try:
            parsed = datetime.strptime(date_str, "%d/%b/%y %I:%M %p")
            return parsed.strftime("%Y/%m/%d")
        except ValueError:
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
    try:
        analysis = SprintReportTeamAnalysis(sys.argv[1:])
        analysis.run()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(0)
    except RuntimeError as e:
        print(f"Caught: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
