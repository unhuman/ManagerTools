#!/usr/bin/env python3
"""
Generate a complete Claude Code team usage report end-to-end.

Fetches rosters from Backstage, queries Datadog Cloud Cost API for usage, builds params,
and generates an interactive HTML report - all in one command.

Usage:
  python -m managertools.tools.team_usage_generate TEAMS TIME_PERIOD [--output PATH]

Arguments:
  TEAMS: Team name(s) comma-separated, or 'org' to use orgTeams from config
  TIME_PERIOD: 'mtd', 'last-month', 'past-month', or 'Nd' (where N is 1-30 days)
  --output PATH: Optional output HTML path (default: ~/claude_team_usage.html)

Configuration required in ~/.managerTools.cfg:
  - backstageServer: Backstage FQDN
  - datadogPAT: Datadog Personal Access Token with scopes:
    * cloud_cost_management_read (required for Cloud Cost API)
    * timeseries_query (required for metrics API)
  - orgTeams: Array of team names (if using 'org' parameter)
"""
import sys
import json
import os
import urllib.request
import urllib.parse
import time
import re
import calendar
from datetime import date, datetime, timedelta

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.tools.team_usage_report import generate_html




def get_date_range(time_period):
    """Get start and end timestamps for the period (milliseconds since epoch).

    Args:
        time_period: 'mtd', 'last-month', 'past-month', or 'Nd' where N is 1-30 (days)
    """
    today = date.today()

    if time_period == 'mtd':
        start_date = date(today.year, today.month, 1)
        end_date = today
    elif time_period == 'last-month':
        if today.month == 1:
            start_date = date(today.year - 1, 12, 1)
            end_date = date(today.year - 1, 12, 31)
        else:
            start_date = date(today.year, today.month - 1, 1)
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
    elif time_period == 'past-month':
        start_date = today - timedelta(days=29)
        end_date = today
    elif time_period.endswith('d') and time_period[:-1].isdigit():
        # Handle 'Nd' format (e.g., '5d' = last 5 days)
        num_days = int(time_period[:-1])
        if num_days < 1 or num_days > 30:
            raise ValueError("Day range must be between 1d and 30d")
        start_date = today - timedelta(days=num_days - 1)
        end_date = today
    else:
        raise ValueError("time_period must be 'mtd', 'last-month', 'past-month', or 'Nd' (where N is 1-30)")

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
        first = date(today.year, today.month, 1)
        month_name = first.strftime('%B')
        return f"{month_name} MTD ({first.strftime('%b %d')} - {today.strftime('%b %d, %Y')})"
    elif time_period == 'last-month':
        if today.month == 1:
            first = date(today.year - 1, 12, 1)
            last = date(today.year - 1, 12, 31)
        else:
            first = date(today.year, today.month - 1, 1)
            last = date(today.year, today.month, 1) - timedelta(days=1)
        month_name = first.strftime('%B %Y')
        return f"{month_name} ({first.strftime('%b %d')} - {last.strftime('%b %d, %Y')})"
    elif time_period == 'past-month':
        start_date = today - timedelta(days=29)
        return f"Past month ({start_date.strftime('%b %d')} - {today.strftime('%b %d, %Y')})"
    elif time_period.endswith('d') and time_period[:-1].isdigit():
        num_days = int(time_period[:-1])
        start_date = today - timedelta(days=num_days - 1)
        if num_days == 1:
            return f"Today ({today.strftime('%b %d, %Y')})"
        else:
            return f"Last {num_days} days ({start_date.strftime('%b %d')} - {today.strftime('%b %d, %Y')})"
    return time_period


def count_workdays(start_date, end_date):
    """Count Mon-Fri workdays (inclusive) between start_date and end_date."""
    count = 0
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # weekday() returns 0-6 for Mon-Sun
            count += 1
        current += timedelta(days=1)
    return count


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




def query_datadog(config_mgr, start_ms, end_ms, roster=None):
    """Query Datadog Cloud Cost Management API for all Anthropic product usage.

    Uses the v1/query endpoint to retrieve metrics from Cloud Cost Management, which
    includes all Anthropic products (chat, claude-code, claude-design, etc.).

    Args:
        config_mgr: ConfigFileManager
        start_ms: Period start (milliseconds since epoch)
        end_ms: Period end (milliseconds since epoch)
        roster: Optional list of member dicts with email field (for filtering results)

    Returns:
        List of usage dicts with model and product cost breakdowns
    """
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")

    pat = config_mgr.get_value('datadogPAT')

    # Convert milliseconds to seconds (Cloud Cost API expects unix seconds)
    start_seconds = int(start_ms / 1000)
    end_seconds = int(end_ms / 1000)

    # Cloud Cost metrics query: all costs by display_user_email, product, and servicename (model)
    query = "sum:custom.cost.amortized{providername:Anthropic} by {display_user_email,product,servicename,cost_type}"

    headers = {
        'Authorization': f'Bearer {pat}',
        'Content-Type': 'application/json'
    }

    # Construct URL for Cloud Cost metrics query
    url = f"https://api.datadoghq.com/api/v1/query?query={urllib.parse.quote(query)}&from={start_seconds}&to={end_seconds}"

    # Retry logic with exponential backoff
    retry_count = 0
    max_retries = 10
    max_retry_wait = 300  # Cap backoff at 5 minutes
    response_data = None

    while response_data is None and retry_count < max_retries:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_code = e.code
            # 4xx errors (except 429 rate-limit) are permanent — retrying won't help
            if 400 <= error_code < 500 and error_code != 429:
                raise RuntimeError(f"Datadog API returned HTTP {error_code}: check datadogPAT in ~/.managerTools.cfg") from e
            retry_count += 1
            wait_time = min(2 ** retry_count, max_retry_wait)
            error_msg = e.reason if hasattr(e, 'reason') else str(e)
            print(f"HTTP {error_code} ({error_msg}), retrying in {wait_time}s (attempt {retry_count})", file=sys.stderr)
            time.sleep(wait_time)
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, max_retry_wait)
            error_type = type(e).__name__
            print(f"{error_type}: {str(e)[:100]}, retrying in {wait_time}s (attempt {retry_count})", file=sys.stderr)
            time.sleep(wait_time)

    if response_data is None:
        raise RuntimeError(f"Failed to query Datadog after {max_retries} retries")

    # Extract roster emails for filtering (if provided)
    roster_emails = set()
    if roster:
        roster_emails = {member['email'].lower() for member in roster}

    # Parse response and aggregate costs by email, model, and product
    usage_by_email = {}

    series_list = response_data.get('series', [])
    print(f"⚙️  Retrieved {len(series_list)} series from Cloud Cost API", file=sys.stderr)

    for series in series_list:
        expression = series.get('expression', '')

        # Extract tags from expression string: "sum:metric{tag1:val1,tag2:val2,...}"
        # Find the {...} portion and parse tags
        match = re.search(r'\{([^}]+)\}', expression)
        if not match:
            continue

        tags_str = match.group(1)
        tags = {}
        for tag_pair in tags_str.split(','):
            if ':' in tag_pair:
                key, value = tag_pair.split(':', 1)
                tags[key] = value

        email = tags.get('display_user_email', '').lower()
        product = tags.get('product', '')
        servicename = tags.get('servicename', '')  # This is the model name
        cost_type = tags.get('cost_type', '')

        # Skip if missing required fields
        if not email or not product or not servicename:
            continue

        # Filter to roster if provided
        if roster_emails and email not in roster_emails:
            continue

        # Sum the pointlist values for total cost
        pointlist = series.get('pointlist', [])
        total_cost = sum(p[1] for p in pointlist if p[1] is not None)

        if total_cost <= 0:
            continue

        # Initialize email entry if needed
        if email not in usage_by_email:
            usage_by_email[email] = {
                'cost': 0,
                'requests': 0,
                'sessions': 0,
                'model_costs': {},
                'product_costs': {}
            }

        # Accumulate costs
        usage_by_email[email]['cost'] += total_cost

        # Track cost by model (servicename)
        if servicename not in usage_by_email[email]['model_costs']:
            usage_by_email[email]['model_costs'][servicename] = 0
        usage_by_email[email]['model_costs'][servicename] += total_cost

        # Track cost by product
        if product not in usage_by_email[email]['product_costs']:
            usage_by_email[email]['product_costs'][product] = 0
        usage_by_email[email]['product_costs'][product] += total_cost

    # Convert to flat array for downstream processing
    usage_data = []
    for email, data in usage_by_email.items():
        usage_data.append({
            'email': email,
            'cost': round(data['cost'], 2),
            'requests': 0,  # Cloud Cost API doesn't provide request counts
            'sessions': 0,  # Cloud Cost API doesn't provide session counts
            'model_costs': {m: round(c, 2) for m, c in data['model_costs'].items()},
            'product_costs': {p: round(c, 2) for p, c in data['product_costs'].items()}
        })

    print(f"✓ Cloud Cost query complete: {len(usage_data)} user(s) with usage", file=sys.stderr)

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

    # Compute forecast for MTD: project spend to end of month
    forecast_by_email = {}
    if time_period == 'mtd':
        today = date.today()
        data_end = today - timedelta(days=1)   # data is one day lagging
        month_start = date(today.year, today.month, 1)
        month_end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
        data_workdays = count_workdays(month_start, data_end)
        month_workdays = count_workdays(month_start, month_end)
        for email, usage in usage_by_email.items():
            cost = usage.get('cost', 0)
            if data_workdays > 0:
                forecast_by_email[email] = round(cost * month_workdays / data_workdays, 2)
            else:
                forecast_by_email[email] = 0.0

    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': sorted(list(models)),
        'products': sorted(list(products)),
        'forecast_by_email': forecast_by_email
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
                            - last-month Previous calendar month
                            - past-month Most recent 30 days (rolling window)
                            - Nd         Last N days, where N is 1-30
                            - Examples: 1d (today), 7d (last 7 days), 30d (last 30 days)

    OUTPUT_PATH             Where to write the HTML report
                            - Relative: report.html, ./reports/usage.html
                            - Home dir: ~/usage.html, ~/reports/usage.html
                            - Absolute: /tmp/report.html

CONFIGURATION (required in ~/.managerTools.cfg):
    backstageServer        Backstage FQDN (e.g., backstage.core.cvent.org)
    datadogPAT            Datadog Personal Access Token with scopes:
                          - cloud_cost_management_read (to read Cloud Cost metrics)
                          - timeseries_query (to query metrics API)
    orgTeams              Array of team names (required if using -t org)

OPTIONAL CONFIGURATION:
    datadogParallelDays   (deprecated - no longer used)

EXAMPLES:
    # Single user for last 7 days
    python -m managertools.tools.team_usage_generate -u alice@cvent.com 7d ~/usage.html

    # Single team, month-to-date
    python -m managertools.tools.team_usage_generate -t Queueless mtd report.html

    # Multiple teams, previous calendar month
    python -m managertools.tools.team_usage_generate -t "Team A,Team B" last-month ~/reports/usage.html

    # Multiple teams, rolling past 30 days
    python -m managertools.tools.team_usage_generate -t "Team A,Team B" past-month ~/reports/usage.html

    # All org teams, last 30 days (completes in seconds)
    python -m managertools.tools.team_usage_generate -t org 30d ~/usage.html

    # Single user, today only
    python -m managertools.tools.team_usage_generate -u bob@cvent.com 1d usage-today.html

OUTPUT:
    - Interactive HTML report with:
      * Two tables: Team Summary and Users (individual contributors)
      * Sticky column headers (stays visible while scrolling)
      * Default sorting by Team/Name (ascending) on page load
      * Click any column header to sort ascending/descending
      * Team filtering with multi-select checkboxes
    - Per-person metrics: Name, Team, Email, Total Cost
      (Note: Requests/Sessions show 0; Cloud Cost API provides costs only)
    - Cost breakdown by Claude model (claude-sonnet-5, claude-opus-4-8, etc.)
    - Cost breakdown by Anthropic product (chat, claude-code, voice-mode, research, etc.)
    - Grouped column headers with distinct colors (blue for Model, purple for Product)
    - Tinted data cells matching their group headers for easy visual scanning
    - Dark mode support (auto-adapts to browser preference)
    - Self-contained HTML (no external assets or dependencies)
    - JSON summary to stdout with metadata

FEATURES:
    - Complete Anthropic product coverage (chat, claude-code, voice-mode, research, etc.)
    - Single efficient Cloud Cost Management API query
    - Fast execution (completes in seconds, not minutes)
    - User-friendly interactive report with sorting and filtering
    - Multi-dimensional cost analysis by user, model, and product
    - Mobile responsive design

TROUBLESHOOTING:
    - "User not found": Email doesn't exist in any team roster
    - "orgTeams not configured": Need to set orgTeams in ~/.managerTools.cfg
    - "datadogPAT not configured": Need to set datadogPAT in ~/.managerTools.cfg
    - PAT scope errors (403 Forbidden): Ensure your PAT has all 4 required scopes:
      * cloud_cost_management_read
      * logs_read_data
      * logs_read_index_data
      * timeseries_query
      Generate a new PAT at: Datadog → Settings → Organization → API Keys → Personal Access Tokens
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
        time_period in ('mtd', 'last-month', 'past-month') or
        (time_period.endswith('d') and time_period[:-1].isdigit() and 1 <= int(time_period[:-1]) <= 30)
    )
    if not valid:
        raise ValueError("time_period must be 'mtd', 'last-month', 'past-month', or 'Nd' (where N is 1-30)")

    output_path = os.path.expanduser(output_path)

    # Load config
    config_mgr = ConfigFileManager('.managerTools.cfg')

    # Parse teams or user
    if user_email:
        print(f"📧 Filtering by user: {user_email}", file=sys.stderr)
        member = find_user_in_rosters(user_email, config_mgr)
        roster = [member]
        teams = [member['team']]
        print(f"   ✓ Found: {member['name']} ({member['email']}) on team {member['team']}", file=sys.stderr)
    else:
        if teams_str.lower() == 'org':
            if not config_mgr.contains_key('orgTeams'):
                raise RuntimeError("orgTeams not configured in ~/.managerTools.cfg")
            teams = config_mgr.get_value('orgTeams')
        else:
            teams = [t.strip() for t in teams_str.split(',')]

        print(f"👥 Filtering by teams: {', '.join(teams)}", file=sys.stderr)
        roster = fetch_rosters(teams, config_mgr)
        print(f"   ✓ Found {len(roster)} team member(s)", file=sys.stderr)

    # Get date range
    start_ms, end_ms, start_date, end_date = get_date_range(time_period)
    print(f"📅 Time period: {start_date} to {end_date}", file=sys.stderr)

    # Show detailed Datadog query info
    from datetime import datetime as dt_module
    start_utc = dt_module.fromtimestamp(start_ms/1000)
    end_utc = dt_module.fromtimestamp(end_ms/1000)
    print(f"   Query: sum:custom.cost.amortized{{providername:Anthropic}} by {{display_user_email,product,servicename}}", file=sys.stderr)
    print(f"   Timestamps: {start_utc.isoformat()}Z to {end_utc.isoformat()}Z", file=sys.stderr)

    # Query Datadog Cloud Cost API
    usage_data = query_datadog(config_mgr, start_ms, end_ms, roster=roster)
    active_users = [u['email'] for u in usage_data if u['cost'] > 0]
    print(f"📊 Query results: {len(active_users)} active user(s) from {len(roster)} roster member(s)", file=sys.stderr)

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
        params['products'],
        params['forecast_by_email']
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
