#!/usr/bin/env python3
"""
Generate a complete Claude Code team usage report end-to-end.

Fetches rosters from Backstage, queries Datadog for usage, builds params,
and generates an interactive HTML report - all in one command.

Usage:
  python -m managertools.tools.team_usage_generate TEAMS TIME_PERIOD [--output PATH]

Arguments:
  TEAMS: Team name(s) comma-separated, or 'org' to use orgTeams from config
  TIME_PERIOD: 'mtd' (month-to-date) or 'past-month'
  --output PATH: Optional output HTML path (default: ~/claude_team_usage.html)

Configuration required in ~/.managerTools.cfg:
  - backstageServer: Backstage FQDN
  - datadogPAT: Datadog Personal Access Token
  - orgTeams: Array of team names (if using 'org' parameter)
"""
import sys
import json
import os
import socket
import urllib.request
import urllib.parse
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.tools.team_usage_report import generate_html


def get_date_range(time_period):
    """Get start and end timestamps for the period (milliseconds since epoch).

    Args:
        time_period: 'mtd', 'past-month', or 'Nd' where N is 1-30 (days)
    """
    today = date.today()

    if time_period == 'mtd':
        start_date = date(today.year, today.month, 1)
        end_date = today
    elif time_period == 'past-month':
        if today.month == 1:
            start_date = date(today.year - 1, 12, 1)
            end_date = date(today.year - 1, 12, 31)
        else:
            start_date = date(today.year, today.month - 1, 1)
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
    elif time_period.endswith('d') and time_period[:-1].isdigit():
        # Handle 'Nd' format (e.g., '5d' = last 5 days)
        num_days = int(time_period[:-1])
        if num_days < 1 or num_days > 30:
            raise ValueError("Day range must be between 1d and 30d")
        start_date = today - timedelta(days=num_days - 1)
        end_date = today
    else:
        raise ValueError("time_period must be 'mtd', 'past-month', or 'Nd' (where N is 1-30)")

    # Convert to milliseconds since epoch (Datadog API expects this format)
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    return start_ms, end_ms, start_date, end_date


def get_period_label(time_period):
    """Generate human-readable period label."""
    today = date.today()
    if time_period == 'mtd':
        month_name = today.strftime('%B %Y')
        return f"{month_name} (MTD)"
    elif time_period == 'past-month':
        if today.month == 1:
            month_name = date(today.year - 1, 12, 1).strftime('%B %Y')
        else:
            month_name = date(today.year, today.month - 1, 1).strftime('%B %Y')
        return month_name
    elif time_period.endswith('d') and time_period[:-1].isdigit():
        num_days = int(time_period[:-1])
        start_date = today - timedelta(days=num_days - 1)
        if num_days == 1:
            return f"Today ({today.strftime('%b %d, %Y')})"
        else:
            return f"Last {num_days} days ({start_date.strftime('%b %d')} - {today.strftime('%b %d, %Y')})"
    return time_period


def fetch_rosters(teams, config_mgr):
    """Fetch team rosters from Backstage."""
    if not config_mgr.contains_key('backstageServer'):
        raise RuntimeError("backstageServer not configured in ~/.managerTools.cfg")

    backstage_server = config_mgr.get_value('backstageServer')
    backstage_auth = config_mgr.get_value('backstageAuth') if config_mgr.contains_key('backstageAuth') else None
    backstage_cache_days = int(config_mgr.get_value('backstageCacheDays')) if config_mgr.contains_key('backstageCacheDays') else 7

    backstage = BackstageREST(backstage_server, backstage_auth)
    cache = BackstageCache(cache_ttl_days=backstage_cache_days)

    members_by_email = {}

    for team_name in teams:
        roster = cache.get(team_name)
        if roster is None:
            roster = backstage.get_team_roster(team_name)
            if roster:
                cache.put(team_name, roster)

        if roster:
            for member in roster:
                raw_entity = member.get('raw_entity', {})
                email_raw = raw_entity.get('spec', {}).get('profile', {}).get('email')

                if email_raw:
                    email = email_raw.lower()
                    if email not in members_by_email:
                        members_by_email[email] = {
                            'name': member.get('display_name', ''),
                            'email': email,
                            'team': team_name
                        }

    return list(members_by_email.values())


def split_date_range(start_ms, end_ms):
    """Split a date range into individual calendar days.

    Returns:
        List of tuples: (day_start_ms, day_end_ms, date_str)
    """
    start_dt = datetime.fromtimestamp(start_ms / 1000)
    end_dt = datetime.fromtimestamp(end_ms / 1000)

    result = []
    current = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    while current <= end_dt:
        day_start = current
        day_end = current.replace(hour=23, minute=59, second=59, microsecond=999999)
        if day_end > end_dt:
            day_end = end_dt

        day_start_ms = int(day_start.timestamp() * 1000)
        day_end_ms = int(day_end.timestamp() * 1000)
        date_str = current.strftime('%Y-%m-%d')

        result.append((day_start_ms, day_end_ms, date_str))

        current += timedelta(days=1)

    return result


def merge_usage_dicts(usage_dicts):
    """Merge multiple per-day usage dicts into one.

    Args:
        usage_dicts: List of {(email, model, product): {'cost': ..., 'requests': ..., 'sessions': {...}}} dicts

    Returns:
        Merged dict with costs/requests summed and sessions union'd
    """
    merged = {}

    for usage_dict in usage_dicts:
        for key, data in usage_dict.items():
            if key not in merged:
                merged[key] = {
                    'cost': 0,
                    'requests': 0,
                    'sessions': set()
                }

            merged[key]['cost'] += data['cost']
            merged[key]['requests'] += data['requests']
            merged[key]['sessions'].update(data['sessions'])

    return merged


def query_datadog_day(config_mgr, start_ms, end_ms, day_str, resume_id=None):
    """Query Datadog for a single day of Anthropic product usage by email, model, and product.

    Args:
        config_mgr: ConfigFileManager
        start_ms: Day start (milliseconds since epoch)
        end_ms: Day end (milliseconds since epoch)
        day_str: Date string for logging (e.g., "2026-07-15")
        resume_id: Optional resume ID for per-day checkpointing

    Returns:
        Dict {(email, model, product): {'cost': float, 'requests': int, 'sessions': set}}
    """
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")

    pat = config_mgr.get_value('datadogPAT')

    # Datadog logs query API endpoint - include all services (claude-code, claude-web, etc.)
    query = "service:claude* @event.name:api_request"

    headers = {
        'Authorization': f'Bearer {pat}',
        'Content-Type': 'application/json'
    }

    usage_by_email_model_product = {}
    page = 0
    cursor = None
    total_logs = 0

    # Per-day checkpoint (optional)
    checkpoint_file = None
    if resume_id:
        checkpoint_file = f"/tmp/datadog_daycheck_{resume_id}_{day_str}.json"
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    cursor = checkpoint.get('cursor')
                    page = checkpoint.get('page', 0)
                    total_logs = checkpoint.get('total_logs', 0)

                    # Restore accumulated usage data
                    saved_usage = checkpoint.get('usage', {})
                    for key_str, data in saved_usage.items():
                        parts = key_str.split('|')
                        email, model, product = parts[0], parts[1], parts[2] if len(parts) > 2 else ''
                        usage_by_email_model_product[(email, model, product)] = {
                            'cost': data['cost'],
                            'requests': data['requests'],
                            'sessions': set(data['sessions'])
                        }

                    print(f"\n✓ Resuming from checkpoint: Page {page}, Total: {total_logs}, Recovered {len(usage_by_email_model_product)} user/model/product tuples", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"\n⚠ Failed to load checkpoint: {e}", file=sys.stderr)
                pass

    try:
        while True:
            page += 1

            # Build URL with pagination
            base_url = f"https://api.datadoghq.com/api/v2/logs/events?filter[query]={urllib.parse.quote(query)}&filter[from]={start_ms}&filter[to]={end_ms}&page[limit]=1000"
            if cursor:
                url = f"{base_url}&page[cursor]={urllib.parse.quote(cursor)}"
            else:
                url = base_url

            # Retry logic for connection issues - infinite retries with exponential backoff
            retry_count = 0
            max_retry_wait = 300  # Cap backoff at 5 minutes
            data = None

            while data is None:
                try:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as response:
                        data = json.loads(response.read().decode('utf-8'))

                        # Check rate limit headers
                        remaining = response.headers.get('X-RateLimit-Remaining')
                        limit = response.headers.get('X-RateLimit-Limit')
                        reset = response.headers.get('X-RateLimit-Reset')

                        if remaining and limit:
                            remaining_int = int(remaining)
                            limit_int = int(limit)

                            # If we're running low on requests, sleep until reset
                            if remaining_int < 5:
                                reset_int = int(reset) if reset else None
                                if reset_int:
                                    wait_until_reset = reset_int - int(time.time())
                                    if wait_until_reset > 0:
                                        print(f"\r⏱ Rate limit approaching ({remaining_int}/{limit_int} remaining), sleeping {wait_until_reset}s...", file=sys.stderr, flush=True)
                                        time.sleep(wait_until_reset + 1)  # +1 to be safe

                except (urllib.error.URLError, ConnectionResetError, BrokenPipeError, TimeoutError, socket.timeout) as e:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, max_retry_wait)  # Exponential backoff, capped at 5 minutes
                    error_type = type(e).__name__
                    print(f"\r⚠ {error_type} on page {page}, retrying in {wait_time}s (attempt {retry_count})...", file=sys.stderr, flush=True)
                    time.sleep(wait_time)

            if data is None:
                break

            if not data.get('data'):
                break

            logs_in_page = len(data['data'])
            total_logs += logs_in_page

            # Extract timestamp from last entry in page for progress indication
            latest_timestamp = ""
            if data['data']:
                last_log = data['data'][-1]
                ts_raw = last_log.get('attributes', {}).get('attributes', {}).get('event', {}).get('timestamp')
                if ts_raw:
                    # Convert ISO format to readable format
                    try:
                        # Parse ISO timestamp and format as readable date/time
                        from datetime import datetime as dt_module
                        dt_obj = dt_module.fromisoformat(ts_raw.replace('Z', '+00:00'))
                        latest_timestamp = f" - Latest: {dt_obj.strftime('%Y-%m-%d %H:%M:%S')}"
                    except:
                        pass

            print(f"\rPage {page}: Fetched {logs_in_page} log entries (total: {total_logs}){latest_timestamp}", file=sys.stderr, end='', flush=True)

            # Process logs
            for log in data['data']:
                # Extract product from top-level service field
                product = log.get('attributes', {}).get('service', '')
                attrs = log.get('attributes', {}).get('attributes', {})

                # Extract email from user object
                user = attrs.get('user', {})
                email = (user.get('normalized_email') or user.get('email') or '').lower()

                # Extract model and cost
                model = attrs.get('model', '')
                cost = float(attrs.get('cost_usd', 0) or 0)

                # Extract session ID
                session_id = attrs.get('session', {}).get('id', '')

                if email and model and product:
                    key = (email, model, product)
                    if key not in usage_by_email_model_product:
                        usage_by_email_model_product[key] = {
                            'cost': 0,
                            'requests': 0,
                            'sessions': set()
                        }

                    usage_by_email_model_product[key]['cost'] += cost
                    usage_by_email_model_product[key]['requests'] += 1

                    if session_id:
                        usage_by_email_model_product[key]['sessions'].add(session_id)

            # Save checkpoint for resume capability
            if checkpoint_file:
                next_cursor = data.get('meta', {}).get('page', {}).get('after')
                checkpoint_data = {
                    'page': page,
                    'cursor': next_cursor,
                    'total_logs': total_logs,
                    'usage': {
                        f"{email}|{model}|{product}": {
                            'cost': usage_data['cost'],
                            'requests': usage_data['requests'],
                            'sessions': list(usage_data['sessions'])
                        }
                        for (email, model, product), usage_data in usage_by_email_model_product.items()
                    }
                }
                try:
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint_data, f)
                except:
                    pass  # Fail silently if checkpoint can't be saved

            # Check for next page
            cursor = data.get('meta', {}).get('page', {}).get('after')
            if not cursor:
                break

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        print(f"Datadog API error ({e.code}): {error_body}", file=sys.stderr)
        raise RuntimeError(f"Failed to query Datadog: {e}")
    except Exception as e:
        print(f"Error querying Datadog: {e}", file=sys.stderr)
        raise

    # Clean up checkpoint file on successful day completion
    if checkpoint_file and os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
        except:
            pass  # Fail silently if cleanup fails

    print(f"✓ {day_str}: Fetched {total_logs} log entries ({page} page(s))", file=sys.stderr)

    # Return raw usage dict for merging with other days
    return usage_by_email_model_product


def query_datadog(config_mgr, start_ms, end_ms, resume_id=None):
    """Query Datadog for all days in parallel, merge results.

    Args:
        config_mgr: ConfigFileManager
        start_ms: Period start (milliseconds since epoch)
        end_ms: Period end (milliseconds since epoch)
        resume_id: Optional resume ID for per-day checkpointing

    Returns:
        List of usage dicts with model and product cost breakdowns
    """
    days = split_date_range(start_ms, end_ms)

    # Get max workers from config, default to 8
    max_workers = 8
    if config_mgr.contains_key('datadogParallelDays'):
        try:
            max_workers = int(config_mgr.get_value('datadogParallelDays'))
        except:
            max_workers = 8

    # Query each day in parallel
    all_usage_dicts = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(query_datadog_day, config_mgr, d_start, d_end, day_str, resume_id): day_str
            for d_start, d_end, day_str in days
        }

        for future in as_completed(futures):
            try:
                day_usage = future.result()
                all_usage_dicts.append(day_usage)
            except Exception as e:
                day_str = futures[future]
                print(f"✗ {day_str}: Failed to fetch - {e}", file=sys.stderr)
                raise

    # Merge results from all days
    merged_usage = merge_usage_dicts(all_usage_dicts)

    # Convert to flat array, grouped by email, with breakdown by model and product
    usage_by_email = {}
    for (email, model, product), data in merged_usage.items():
        if email not in usage_by_email:
            usage_by_email[email] = {
                'cost': 0,
                'requests': 0,
                'sessions': set(),
                'model_costs': {},
                'product_costs': {}
            }

        usage_by_email[email]['cost'] += data['cost']
        usage_by_email[email]['requests'] += data['requests']
        usage_by_email[email]['sessions'].update(data['sessions'])

        # Track cost by model
        if model not in usage_by_email[email]['model_costs']:
            usage_by_email[email]['model_costs'][model] = 0
        usage_by_email[email]['model_costs'][model] += data['cost']

        # Track cost by product
        if product not in usage_by_email[email]['product_costs']:
            usage_by_email[email]['product_costs'][product] = 0
        usage_by_email[email]['product_costs'][product] += data['cost']

    # Convert to flat array for downstream processing
    usage_data = []
    for email, data in usage_by_email.items():
        usage_data.append({
            'email': email,
            'cost': round(data['cost'], 2),
            'requests': data['requests'],
            'sessions': len(data['sessions']),
            'model_costs': {m: round(c, 2) for m, c in data['model_costs'].items()},
            'product_costs': {p: round(c, 2) for p, c in data['product_costs'].items()}
        })

    print(f"\nParallel fetch complete: {len(days)} days queried", file=sys.stderr)

    return usage_data


def build_params(roster, usage_data, teams, time_period):
    """Build params from roster and usage data."""
    usage_by_email = {}
    models = set()
    products = set()

    # Initialize from roster
    for member in roster:
        email = member['email']
        usage_by_email[email] = {
            'cost': 0,
            'requests': 0,
            'sessions': 0,
            'model_costs': {},
            'product_costs': {}
        }

    # Merge usage data
    for row in usage_data:
        email = row.get('email', '').lower()
        cost = row.get('cost', 0)
        requests = row.get('requests', 0)
        sessions = row.get('sessions', 0)

        if email in usage_by_email:
            usage_by_email[email]['cost'] += cost
            usage_by_email[email]['requests'] += requests
            usage_by_email[email]['sessions'] = max(usage_by_email[email]['sessions'], sessions)

            # Merge model costs
            model_costs = row.get('model_costs', {})
            for model, model_cost in model_costs.items():
                usage_by_email[email]['model_costs'][model] = model_cost
                models.add(model)

            # Merge product costs
            product_costs = row.get('product_costs', {})
            for product, product_cost in product_costs.items():
                usage_by_email[email]['product_costs'][product] = product_cost
                products.add(product)

    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': sorted(list(models)),
        'products': sorted(list(products))
    }

    return params


def find_user_in_rosters(user_email, config_mgr):
    """Find a user by email across all org teams.

    Args:
        user_email: Email address to find
        config_mgr: ConfigFileManager instance

    Returns:
        Dict with 'name', 'email', 'team' keys

    Raises:
        RuntimeError if user not found
    """
    if not config_mgr.contains_key('orgTeams'):
        raise RuntimeError("orgTeams not configured in ~/.managerTools.cfg")

    org_teams = config_mgr.get_value('orgTeams')
    if not isinstance(org_teams, list):
        raise RuntimeError("orgTeams must be an array of team names")

    # Fetch rosters for all teams
    backstage_server = config_mgr.get_value('backstageServer')
    backstage_auth = config_mgr.get_value('backstageAuth') if config_mgr.contains_key('backstageAuth') else None
    backstage_cache_days = int(config_mgr.get_value('backstageCacheDays')) if config_mgr.contains_key('backstageCacheDays') else 7

    backstage = BackstageREST(backstage_server, backstage_auth)
    cache = BackstageCache(cache_ttl_days=backstage_cache_days)

    target_email = user_email.lower()
    for team_name in org_teams:
        roster = cache.get(team_name)
        if roster is None:
            roster = backstage.get_team_roster(team_name)
            if roster:
                cache.put(team_name, roster)

        if roster:
            for member in roster:
                raw_entity = member.get('raw_entity', {})
                email_raw = raw_entity.get('spec', {}).get('profile', {}).get('email')
                if email_raw and email_raw.lower() == target_email:
                    return {
                        'name': member.get('display_name', ''),
                        'email': email_raw.lower(),
                        'team': team_name
                    }

    raise RuntimeError(f"User {user_email} not found in any team")


def print_help():
    """Print comprehensive usage help."""
    help_text = """
Generate an interactive Claude Code usage report with multi-dimensional cost analysis.

USAGE:
    python -m managertools.tools.team_usage_generate [-u EMAIL | -t TEAMS] TIME_PERIOD OUTPUT_PATH
    python -m managertools.tools.team_usage_generate --help

REQUIRED ARGUMENTS (choose one):
    -u EMAIL                Single user email to analyze (mutually exclusive with -t)
    -t TEAMS                Team name(s) to analyze (mutually exclusive with -u)
                            - Use "org" to include all teams from orgTeams config
                            - Use comma-separated names for multiple teams: "Team A,Team B"

REQUIRED POSITIONAL ARGUMENTS:
    TIME_PERIOD             Report period:
                            - mtd        Month-to-date (1st of current month to today)
                            - past-month Previous calendar month
                            - Nd         Last N days, where N is 1-30
                            - Examples: 1d (today), 7d (last 7 days), 30d (last 30 days)

    OUTPUT_PATH             Where to write the HTML report
                            - Relative: report.html, ./reports/usage.html
                            - Home dir: ~/usage.html, ~/reports/usage.html
                            - Absolute: /tmp/report.html

CONFIGURATION (required in ~/.managerTools.cfg):
    backstageServer        Backstage FQDN (e.g., backstage.core.cvent.org)
    datadogPAT            Datadog Personal Access Token
    orgTeams              Array of team names (required if using -t org)

OPTIONAL CONFIGURATION:
    datadogParallelDays   Number of parallel day queries (default: 8, max: 16)

EXAMPLES:
    # Single user for last 7 days
    python -m managertools.tools.team_usage_generate -u alice@cvent.com 7d ~/usage.html

    # Single team, month-to-date
    python -m managertools.tools.team_usage_generate -t Queueless mtd report.html

    # Multiple teams, previous month
    python -m managertools.tools.team_usage_generate -t "Team A,Team B" past-month ~/reports/usage.html

    # All org teams, last 30 days (parallel query - very fast)
    python -m managertools.tools.team_usage_generate -t org 30d ~/usage.html

    # Single user, today only
    python -m managertools.tools.team_usage_generate -u bob@cvent.com 1d usage-today.html

OUTPUT:
    - Interactive HTML report with sortable tables
    - Per-person metrics: Name, Team, Email, Total Cost, Requests, Sessions
    - Cost breakdown by Claude model (Haiku, Sonnet, Opus, etc.)
    - Cost breakdown by Anthropic product (Claude Code, Claude Web, etc.)
    - Team summary section showing team-level aggregation
    - Inactive users section (zero usage in period)
    - JSON summary to stdout with metadata

FEATURES:
    - Parallel day-by-day querying for speed (30-day queries in 1-3 minutes)
    - Rate limit handling (respects Datadog API limits)
    - Resume capability (can restart interrupted queries)
    - Per-day checkpointing for resilience

TROUBLESHOOTING:
    - "User not found": Email doesn't exist in any team roster
    - "orgTeams not configured": Need to set orgTeams in ~/.managerTools.cfg
    - "datadogPAT not configured": Need to set datadogPAT in ~/.managerTools.cfg
    - Rate limit errors: Reduce datadogParallelDays in config (default 8, try 4)
"""
    print(help_text)


def main(user_email, teams_str, time_period, output_path):
    """Main entry point.

    Args:
        user_email: Single user email (mutually exclusive with teams_str)
        teams_str: Team names (comma-separated) or 'org' (mutually exclusive with user_email)
        time_period: 'mtd', 'past-month', or 'Nd' (where N is 1-30)
        output_path: Required path for output HTML file
    """
    # Validate time_period
    valid = (
        time_period in ('mtd', 'past-month') or
        (time_period.endswith('d') and time_period[:-1].isdigit() and 1 <= int(time_period[:-1]) <= 30)
    )
    if not valid:
        raise ValueError("time_period must be 'mtd', 'past-month', or 'Nd' (where N is 1-30)")

    output_path = os.path.expanduser(output_path)

    # Load config
    config_mgr = ConfigFileManager('.managerTools.cfg')

    # Parse teams or user
    if user_email:
        print(f"Fetching user: {user_email}", file=sys.stderr)
        member = find_user_in_rosters(user_email, config_mgr)
        roster = [member]
        teams = [member['team']]
        print(f"Found user {member['name']} ({member['email']}) on team {member['team']}", file=sys.stderr)
    else:
        if teams_str.lower() == 'org':
            if not config_mgr.contains_key('orgTeams'):
                raise RuntimeError("orgTeams not configured in ~/.managerTools.cfg")
            teams = config_mgr.get_value('orgTeams')
        else:
            teams = [t.strip() for t in teams_str.split(',')]

        print(f"Fetching rosters for teams: {', '.join(teams)}", file=sys.stderr)
        roster = fetch_rosters(teams, config_mgr)
        print(f"Found {len(roster)} team members", file=sys.stderr)

    # Get date range
    start_ms, end_ms, start_date, end_date = get_date_range(time_period)
    print(f"Querying Datadog for {start_date} to {end_date}", file=sys.stderr)
    print(f"Timestamps: {start_ms} to {end_ms} (ms)", file=sys.stderr)

    # Debug: show what the dates convert to
    from datetime import datetime as dt_module
    print(f"Start: {dt_module.fromtimestamp(start_ms/1000)} UTC", file=sys.stderr)
    print(f"End: {dt_module.fromtimestamp(end_ms/1000)} UTC", file=sys.stderr)

    # Query Datadog with resume capability
    # Use user_email or teams_str + time_period as resume ID so same query can resume
    if user_email:
        resume_id = f"user_{user_email.replace('@', '_')}_{time_period}".replace(' ', '_')
    else:
        resume_id = f"teams_{teams_str}_{time_period}".replace(' ', '_').replace(',', '')

    usage_data = query_datadog(config_mgr, start_ms, end_ms, resume_id=resume_id)
    print(f"Found usage data for {len(set(u['email'] for u in usage_data))} users", file=sys.stderr)

    # Build params
    params = build_params(roster, usage_data, teams, time_period)

    # Generate HTML
    html = generate_html(
        teams,
        time_period,
        params['period_label'],
        roster,
        params['usage_by_email'],
        params['models'],
        params['products']
    )

    # Write output
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    # Print status message to stderr for user visibility
    print(f"\n✓ Report generated successfully!", file=sys.stderr)
    print(f"  Output file: {output_path}", file=sys.stderr)

    # Print JSON summary to stdout
    print(json.dumps({
        'success': True,
        'output': output_path,
        'teams': teams,
        'members': len(roster),
        'active_users': sum(1 for u in params['usage_by_email'].values() if u['cost'] > 0),
        'total_cost': round(sum(u['cost'] for u in params['usage_by_email'].values()), 2),
        'models': params['models'],
        'products': params['products']
    }))


if __name__ == '__main__':
    # Check for help first
    if len(sys.argv) > 1 and (sys.argv[1] == '--help' or sys.argv[1] == '-h' or sys.argv[1] == 'help'):
        print_help()
        sys.exit(0)

    if len(sys.argv) < 4:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_generate [-u EMAIL | -t TEAMS] TIME_PERIOD OUTPUT_PATH\n"
                     "       python -m managertools.tools.team_usage_generate --help\n"
                     "\nQuick examples:\n"
                     "  -u EMAIL:     python -m managertools.tools.team_usage_generate -u alice@cvent.com 7d ~/usage.html\n"
                     "  -t TEAMS:     python -m managertools.tools.team_usage_generate -t Queueless mtd ~/usage.html\n"
                     "  -t org:       python -m managertools.tools.team_usage_generate -t org 30d ~/usage.html\n"
                     "\nFor full help: python -m managertools.tools.team_usage_generate --help"
        }), file=sys.stderr)
        sys.exit(1)

    # Parse -u and -t flags (mutually exclusive)
    user_email = None
    teams_str = None
    time_period = None
    output_path = None

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '-u':
            if user_email is not None or teams_str is not None:
                print(json.dumps({"error": "-u and -t are mutually exclusive, and can each only be specified once"}), file=sys.stderr)
                sys.exit(1)
            if i + 1 >= len(sys.argv):
                print(json.dumps({"error": "-u requires an EMAIL argument"}), file=sys.stderr)
                sys.exit(1)
            user_email = sys.argv[i + 1]
            i += 2
        elif arg == '-t':
            if user_email is not None or teams_str is not None:
                print(json.dumps({"error": "-u and -t are mutually exclusive, and can each only be specified once"}), file=sys.stderr)
                sys.exit(1)
            if i + 1 >= len(sys.argv):
                print(json.dumps({"error": "-t requires a TEAMS argument"}), file=sys.stderr)
                sys.exit(1)
            teams_str = sys.argv[i + 1]
            i += 2
        else:
            # Positional arguments
            if time_period is None:
                time_period = arg
            elif output_path is None:
                output_path = arg
            else:
                print(json.dumps({"error": f"Unexpected argument: {arg}"}), file=sys.stderr)
                sys.exit(1)
            i += 1

    # Validate we have one of -u or -t, and both required positional args
    if user_email is None and teams_str is None:
        print(json.dumps({"error": "Either -u EMAIL or -t TEAMS is required"}), file=sys.stderr)
        sys.exit(1)
    if time_period is None or output_path is None:
        print(json.dumps({"error": "TIME_PERIOD and OUTPUT_PATH are required"}), file=sys.stderr)
        sys.exit(1)

    try:
        main(user_email, teams_str, time_period, output_path)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
