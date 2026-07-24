#!/usr/bin/env python3
"""
Build a complete report params JSON from a roster, optionally with Datadog usage data.

Usage (with Datadog data):
  python -m managertools.tools.team_usage_build_report ROSTER_JSON TIME_PERIOD OUTPUT_JSON [--datadog-json DATADOG_JSON]

Arguments:
  ROSTER_JSON: JSON file with roster (output from team_usage_roster)
  TIME_PERIOD: 'mtd' or 'past-month'
  OUTPUT_JSON: Path to write the complete params JSON
  --datadog-json: Optional path to Datadog query results JSON

The Datadog JSON should have the structure from Datadog MCP queries:
  {
    "by_email_model": [
      {"@email": "user@cvent.com", "@model": "claude-sonnet-4-5", "sum_cost": 45.50, "request_count": 127, "session_count": 8}
    ],
    "by_email": [
      {"@email": "user@cvent.com", "event_count": 250}
    ]
  }

If no Datadog JSON provided, initializes usage_by_email with zero values.
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


def build_params(roster_file, time_period, datadog_file=None):
    """
    Build complete params from roster and optional Datadog data.

    Args:
        roster_file: Path to roster JSON (from team_usage_roster)
        time_period: 'mtd' or 'past-month'
        datadog_file: Optional path to Datadog query results JSON

    Returns:
        Dict with params structure ready for team_usage_report
    """
    # Read roster
    with open(roster_file, 'r') as f:
        roster = json.load(f)

    if not isinstance(roster, list):
        raise ValueError("Roster file must contain a JSON array")

    # Extract unique teams and initialize usage template
    teams = []
    usage_by_email = {}
    seen_teams = set()

    for member in roster:
        email = member.get('email', '').lower()
        team = member.get('team', '')

        if team and team not in seen_teams:
            teams.append(team)
            seen_teams.add(team)

        if email:
            if email not in usage_by_email:
                usage_by_email[email] = {
                    'cost': 0,
                    'requests': 0,
                    'sessions': 0,
                    'model_costs': {}
                }

    # Merge Datadog data if provided
    models = set()
    if datadog_file:
        with open(datadog_file, 'r') as f:
            datadog = json.load(f)

        # Process per-email-model usage data
        by_email_model = datadog.get('by_email_model', [])
        for row in by_email_model:
            email = row.get('@email', '').lower()
            model = row.get('@model', '')
            cost = row.get('sum_cost', 0) or row.get('cost', 0) or 0
            requests = row.get('request_count', 0) or row.get('count', 0) or 0
            sessions = row.get('session_count', 0) or 0

            if email in usage_by_email:
                # Add to totals
                usage_by_email[email]['cost'] += cost
                usage_by_email[email]['requests'] += requests
                usage_by_email[email]['sessions'] = max(usage_by_email[email]['sessions'], sessions)

                # Add per-model cost
                if model:
                    usage_by_email[email]['model_costs'][model] = cost
                    models.add(model)

    # Build params
    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': sorted(list(models))
    }

    return params


def main(roster_file, time_period, output_file, datadog_file=None):
    """Main entry point."""
    if time_period not in ('mtd', 'past-month'):
        raise ValueError(f"time_period must be 'mtd' or 'past-month', got '{time_period}'")

    params = build_params(roster_file, time_period, datadog_file)

    with open(output_file, 'w') as f:
        json.dump(params, f, indent=2)

    active_users = sum(1 for usage in params['usage_by_email'].values() if usage['cost'] > 0)
    total_cost = sum(usage['cost'] for usage in params['usage_by_email'].values())

    print(json.dumps({
        'success': True,
        'output': output_file,
        'teams': params['teams'],
        'members': len(params['members']),
        'active_users': active_users,
        'total_cost': total_cost,
        'models': params['models']
    }))


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_build_report ROSTER_JSON TIME_PERIOD OUTPUT_JSON [--datadog-json DATADOG_JSON]"
        }), file=sys.stderr)
        sys.exit(1)

    roster_file = sys.argv[1]
    time_period = sys.argv[2]
    output_file = sys.argv[3]
    datadog_file = None

    if len(sys.argv) > 4 and sys.argv[4] == '--datadog-json' and len(sys.argv) > 5:
        datadog_file = sys.argv[5]

    try:
        main(roster_file, time_period, output_file, datadog_file)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
