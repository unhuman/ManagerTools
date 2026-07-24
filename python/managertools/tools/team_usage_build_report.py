#!/usr/bin/env python3
"""
Build complete report params from roster and Datadog usage data.

Usage:
  python -m managertools.tools.team_usage_build_report ROSTER_JSON TIME_PERIOD USAGE_JSON OUTPUT_JSON

Arguments:
  ROSTER_JSON: JSON from team_usage_roster (array of team members)
  TIME_PERIOD: 'mtd' or 'past-month'
  USAGE_JSON: Datadog aggregated usage by email and model (required - no zero usage reports)
  OUTPUT_JSON: Path to write complete params for team_usage_report

Usage JSON format (from Datadog MCP aggregation):
  [
    {"email": "user@cvent.com", "model": "claude-sonnet-4-5", "cost": 45.50, "requests": 127, "sessions": 8},
    {"email": "user@cvent.com", "model": "claude-opus-4-5", "cost": 13.50, "requests": 32, "sessions": 4}
  ]
"""
import sys
import json
from datetime import date


def get_period_label(time_period):
    """Generate a human-readable period label."""
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


def build_params(roster_file, time_period, usage_file):
    """Build params from roster and usage data."""
    with open(roster_file, 'r') as f:
        roster = json.load(f)

    if not isinstance(roster, list):
        raise ValueError("Roster must be a JSON array")

    with open(usage_file, 'r') as f:
        usage_list = json.load(f)

    if not isinstance(usage_list, list):
        raise ValueError("Usage data must be a JSON array")

    # Extract unique teams from roster
    teams = []
    seen_teams = set()
    for member in roster:
        team = member.get('team', '')
        if team and team not in seen_teams:
            teams.append(team)
            seen_teams.add(team)

    # Build usage_by_email from flat usage list
    usage_by_email = {}
    models = set()

    for row in usage_list:
        email = row.get('email', '').lower()
        model = row.get('model', '')
        cost = row.get('cost', 0) or 0
        requests = row.get('requests', 0) or 0
        sessions = row.get('sessions', 0) or 0

        if email:
            if email not in usage_by_email:
                usage_by_email[email] = {
                    'cost': 0,
                    'requests': 0,
                    'sessions': 0,
                    'model_costs': {}
                }

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


def main(roster_file, time_period, usage_file, output_file):
    """Main entry point."""
    if time_period not in ('mtd', 'past-month'):
        raise ValueError(f"time_period must be 'mtd' or 'past-month'")

    params = build_params(roster_file, time_period, usage_file)

    with open(output_file, 'w') as f:
        json.dump(params, f, indent=2)

    total_cost = sum(u['cost'] for u in params['usage_by_email'].values())
    print(json.dumps({
        'success': True,
        'output': output_file,
        'teams': params['teams'],
        'members': len(params['members']),
        'total_cost': round(total_cost, 2),
        'models': params['models']
    }))


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_build_report ROSTER_JSON TIME_PERIOD USAGE_JSON OUTPUT_JSON"
        }), file=sys.stderr)
        sys.exit(1)

    try:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
