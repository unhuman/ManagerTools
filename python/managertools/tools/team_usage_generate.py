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
import urllib.request
import urllib.parse
import time
from datetime import date, datetime, timedelta

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.tools.team_usage_report import generate_html


def get_date_range(time_period):
    """Get start and end timestamps for the period (milliseconds since epoch)."""
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
    else:
        raise ValueError("time_period must be 'mtd' or 'past-month'")

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


def query_datadog(config_mgr, start_ms, end_ms):
    """Query Datadog for Claude Code usage by email and model."""
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")

    pat = config_mgr.get_value('datadogPAT')

    # Datadog logs query API endpoint
    query = "service:claude-code @event.name:api_request"

    headers = {
        'Authorization': f'Bearer {pat}',
        'Content-Type': 'application/json'
    }

    usage_by_email_model = {}
    page = 0
    cursor = None
    total_logs = 0

    try:
        while True:
            page += 1

            # Build URL with pagination
            base_url = f"https://api.datadoghq.com/api/v2/logs/events?filter[query]={urllib.parse.quote(query)}&filter[from]={start_ms}&filter[to]={end_ms}&page[limit]=1000"
            if cursor:
                url = f"{base_url}&page[cursor]={urllib.parse.quote(cursor)}"
            else:
                url = base_url

            # Retry logic for connection issues
            max_retries = 5
            retry_count = 0
            data = None

            while retry_count < max_retries and data is None:
                try:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as response:
                        data = json.loads(response.read().decode('utf-8'))
                except (urllib.error.URLError, ConnectionResetError, BrokenPipeError) as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count  # Exponential backoff: 2, 4, 8, 16, 32 seconds
                        print(f"\n⚠ Connection error on page {page}, retrying in {wait_time}s (attempt {retry_count}/{max_retries})...", file=sys.stderr, flush=True)
                        time.sleep(wait_time)
                    else:
                        raise RuntimeError(f"Failed to fetch page {page} after {max_retries} retries: {e}")

            if data is None:
                break

                if not data.get('data'):
                    break

                logs_in_page = len(data['data'])
                total_logs += logs_in_page

                print(f"\rPage {page}: Fetched {logs_in_page} log entries (total: {total_logs})", file=sys.stderr, end='', flush=True)

                # Process logs
                for log in data['data']:
                    attrs = log.get('attributes', {}).get('attributes', {})

                    # Extract email from user object
                    user = attrs.get('user', {})
                    email = (user.get('normalized_email') or user.get('email') or '').lower()

                    # Extract model and cost
                    model = attrs.get('model', '')
                    cost = float(attrs.get('cost_usd', 0) or 0)

                    # Extract session ID
                    session_id = attrs.get('session', {}).get('id', '')

                    if email and model:
                        key = (email, model)
                        if key not in usage_by_email_model:
                            usage_by_email_model[key] = {
                                'cost': 0,
                                'requests': 0,
                                'sessions': set()
                            }

                        usage_by_email_model[key]['cost'] += cost
                        usage_by_email_model[key]['requests'] += 1

                        if session_id:
                            usage_by_email_model[key]['sessions'].add(session_id)

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

    # Convert to flat array
    usage_data = []
    for (email, model), data in usage_by_email_model.items():
        usage_data.append({
            'email': email,
            'model': model,
            'cost': round(data['cost'], 2),
            'requests': data['requests'],
            'sessions': len(data['sessions'])
        })

    print(f"\nPagination complete: Fetched {total_logs} total log entries across {page} page(s)", file=sys.stderr)

    return usage_data


def build_params(roster, usage_data, teams, time_period):
    """Build params from roster and usage data."""
    usage_by_email = {}
    models = set()

    # Initialize from roster
    for member in roster:
        email = member['email']
        usage_by_email[email] = {
            'cost': 0,
            'requests': 0,
            'sessions': 0,
            'model_costs': {}
        }

    # Merge usage data
    for row in usage_data:
        email = row.get('email', '').lower()
        model = row.get('model', '')
        cost = row.get('cost', 0)
        requests = row.get('requests', 0)
        sessions = row.get('sessions', 0)

        if email in usage_by_email:
            usage_by_email[email]['cost'] += cost
            usage_by_email[email]['requests'] += requests
            usage_by_email[email]['sessions'] = max(usage_by_email[email]['sessions'], sessions)

            if model:
                usage_by_email[email]['model_costs'][model] = cost
                models.add(model)

    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': sorted(list(models))
    }

    return params


def main(teams_str, time_period, output_path=None):
    """Main entry point."""
    if time_period not in ('mtd', 'past-month'):
        raise ValueError("time_period must be 'mtd' or 'past-month'")

    if output_path is None:
        output_path = os.path.expanduser('~/claude_team_usage.html')
    else:
        output_path = os.path.expanduser(output_path)

    # Load config
    config_mgr = ConfigFileManager('.managerTools.cfg')

    # Parse teams
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

    # Query Datadog
    usage_data = query_datadog(config_mgr, start_ms, end_ms)
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
        params['models']
    )

    # Write output
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    print(json.dumps({
        'success': True,
        'output': output_path,
        'teams': teams,
        'members': len(roster),
        'active_users': sum(1 for u in params['usage_by_email'].values() if u['cost'] > 0),
        'total_cost': round(sum(u['cost'] for u in params['usage_by_email'].values()), 2),
        'models': params['models']
    }))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_generate TEAMS TIME_PERIOD [--output PATH]"
        }), file=sys.stderr)
        sys.exit(1)

    teams = sys.argv[1]
    time_period = sys.argv[2]
    output_path = None

    if len(sys.argv) > 3 and sys.argv[3] == '--output' and len(sys.argv) > 4:
        output_path = sys.argv[4]

    try:
        main(teams, time_period, output_path)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
