#!/usr/bin/env python3
"""
Convert a roster JSON file into a complete params JSON for team_usage_report.

Usage:
  python -m managertools.tools.team_usage_params_builder ROSTER_JSON TIME_PERIOD OUTPUT_JSON

Args:
  ROSTER_JSON: JSON file with roster (output from team_usage_roster)
  TIME_PERIOD: 'mtd' or 'past-month'
  OUTPUT_JSON: Path to write the params JSON

This creates a template with zero usage data. You would then fill in
usage_by_email with real Datadog query results.
"""
import sys
import json
from datetime import datetime, date
from collections import defaultdict


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


def build_params(roster_file, time_period):
    """
    Build params from roster.

    Args:
        roster_file: Path to roster JSON (from team_usage_roster)
        time_period: 'mtd' or 'past-month'

    Returns:
        Dict with params structure ready for team_usage_report
    """
    # Read roster
    with open(roster_file, 'r') as f:
        roster = json.load(f)

    if not isinstance(roster, list):
        raise ValueError("Roster file must contain a JSON array")

    # Extract unique teams and create usage template
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
            # Initialize empty usage for this email
            if email not in usage_by_email:
                usage_by_email[email] = {
                    'cost': 0,
                    'requests': 0,
                    'sessions': 0,
                    'model_costs': {}
                }

    # Build params
    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': []
    }

    return params


def main(roster_file, time_period, output_file):
    """Main entry point."""
    if time_period not in ('mtd', 'past-month'):
        raise ValueError(f"time_period must be 'mtd' or 'past-month', got '{time_period}'")

    params = build_params(roster_file, time_period)

    with open(output_file, 'w') as f:
        json.dump(params, f, indent=2)

    print(json.dumps({
        'success': True,
        'output': output_file,
        'teams': params['teams'],
        'members': len(params['members']),
        'note': 'usage_by_email is initialized with zero values. Fill in with Datadog query results.'
    }))


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_params_builder ROSTER_JSON TIME_PERIOD OUTPUT_JSON"
        }), file=sys.stderr)
        sys.exit(1)

    try:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
