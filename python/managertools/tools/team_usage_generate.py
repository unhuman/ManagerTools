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
from datetime import date, datetime, timedelta

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.tools.team_usage_report import generate_html


def get_date_range(time_period):
    """Get start and end ISO timestamps for the period."""
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

    # Convert to ISO 8601 timestamps
    start_iso = f"{start_date}T00:00:00Z"
    end_iso = f"{end_date}T23:59:59Z"

    return start_iso, end_iso, start_date, end_date


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


def query_datadog(config_mgr, start_iso, end_iso):
    """Query Datadog for Claude Code usage by email and model."""
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")

    pat = config_mgr.get_value('datadogPAT')

    # Datadog logs query API endpoint
    # Query: service:claude-code @event.name:api_request
    query = "service:claude-code @event.name:api_request"

    url = f"https://api.datadoghq.com/api/v2/logs/events?filter[query]={urllib.parse.quote(query)}&filter[from]={start_iso}&filter[to]={end_iso}&page[limit]=1000"

    headers = {
        'Authorization': f'Bearer {pat}',
        'Content-Type': 'application/json'
    }

    usage_data = []

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))

            # Aggregate logs by email and model
            usage_by_email_model = {}

            if 'data' in data:
                print(f"Datadog returned {len(data['data'])} log entries", file=sys.stderr)

                # Show first log entry for debugging
                if data['data']:
                    print(f"Sample log entry: {json.dumps(data['data'][0], indent=2)}", file=sys.stderr)

                for log in data['data']:
                    attributes = log.get('attributes', {})
                    email = attributes.get('attributes', {}).get('email', '').lower()
                    model = attributes.get('attributes', {}).get('model', '')
                    cost = float(attributes.get('attributes', {}).get('cost_usd', 0) or 0)

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

                        session_id = attributes.get('attributes', {}).get('session_id', '')
                        if session_id:
                            usage_by_email_model[key]['sessions'].add(session_id)

            # Convert to flat array
            for (email, model), data in usage_by_email_model.items():
                usage_data.append({
                    'email': email,
                    'model': model,
                    'cost': round(data['cost'], 2),
                    'requests': data['requests'],
                    'sessions': len(data['sessions'])
                })

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        print(f"Datadog API error ({e.code}): {error_body}", file=sys.stderr)
        raise RuntimeError(f"Failed to query Datadog: {e}")
    except Exception as e:
        print(f"Error querying Datadog: {e}", file=sys.stderr)
        raise

    if not usage_data:
        print(f"Warning: No usage data returned from Datadog", file=sys.stderr)
        print(f"Query: {query}", file=sys.stderr)
        print(f"Period: {start_iso} to {end_iso}", file=sys.stderr)

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
    start_ts, end_ts, start_date, end_date = get_date_range(time_period)
    print(f"Querying Datadog for {start_date} to {end_date}", file=sys.stderr)

    # Query Datadog
    usage_data = query_datadog(config_mgr, start_ts, end_ts)
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
