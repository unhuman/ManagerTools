#!/usr/bin/env python3
"""Report all principal-level developers from configured orgTeams."""

import argparse
import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache


def normalize_title(title: str) -> str:
    """Normalize title by removing contractor markers and standardizing capitalization."""
    if not title:
        return title
    normalized = title.replace(' [C]', '').strip()
    if re.search(r'\bintern\b', normalized, re.IGNORECASE):
        return "Intern"
    if normalized:
        normalized = normalized.title()
        normalized = re.sub(r'Ii+', lambda m: m.group(0).upper(), normalized)
    return normalized


def matches_title_filter(role: str, title_filter: str) -> bool:
    """Check if normalized role contains the title filter (case-insensitive)."""
    if not role:
        return False
    normalized = normalize_title(role)
    return title_filter.lower() in normalized.lower()


def fetch_team_roster_cached(backstage_rest: BackstageREST, cache: BackstageCache, team_name: str):
    """Fetch team roster from cache first, then API."""
    cached_roster = cache.get(team_name)
    if cached_roster:
        return cached_roster

    roster = backstage_rest.get_team_roster(team_name)
    if roster:
        cache.put(team_name, roster)
    return roster


def main():
    parser = argparse.ArgumentParser(description='Report principals by team from orgTeams')
    parser.add_argument('--title-filter', default='principal',
                        help='Filter roles by substring (default: principal)')
    args = parser.parse_args()

    config = ConfigFileManager('.managerTools.cfg')
    if not config.contains_key('backstageServer'):
        print("ERROR: backstageServer not configured in ~/.managerTools.cfg", file=sys.stderr)
        sys.exit(1)
    if not config.contains_key('orgTeams'):
        print("ERROR: orgTeams not configured in ~/.managerTools.cfg", file=sys.stderr)
        sys.exit(1)

    server = config.get_value('backstageServer')
    auth_token = config.get_value('backstageAuth') if config.contains_key('backstageAuth') else None
    org_teams = config.get_value('orgTeams')

    backstage_rest = BackstageREST(server, auth_token)
    cache = BackstageCache()

    team_results = {}
    total_principals = 0

    for team_name in org_teams:
        roster = fetch_team_roster_cached(backstage_rest, cache, team_name)
        principals = []

        for member in roster:
            role = member.get('raw_entity', {}).get('spec', {}).get('profile', {}).get('role', '')
            if matches_title_filter(role, args.title_filter):
                principals.append({
                    'display_name': member.get('display_name', ''),
                    'user_ref': member.get('user_ref', ''),
                    'role': normalize_title(role)
                })

        if principals:
            team_results[team_name] = principals
            total_principals += len(principals)

    if not team_results:
        print("No principals found.", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'=== Principals Report ===':^60}\n")

    for team_name in sorted(team_results.keys()):
        principals = team_results[team_name]
        print(f"Team: {team_name} ({len(principals)} principal{'s' if len(principals) != 1 else ''})")
        for p in sorted(principals, key=lambda x: x['display_name']):
            print(f"  {p['display_name']:<30} ({p['user_ref']:<18}) {p['role']}")
        print()

    print(f"\n{'--- Summary ---':^60}")
    print(f"Total: {total_principals} principals across {len(team_results)} teams")


if __name__ == '__main__':
    main()
