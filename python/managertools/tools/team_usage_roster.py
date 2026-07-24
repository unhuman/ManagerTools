#!/usr/bin/env python3
"""
Fetch Backstage team rosters and extract member emails.

Usage:
  python -m managertools.tools.team_usage_roster TEAM1 TEAM2 ...

Output: JSON array to stdout
"""
import sys
import json

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache


def fetch_team_rosters(teams):
    """
    Fetch rosters for specified teams from Backstage.

    Args:
        teams: List of team names, or ["org"] to use orgTeams from config

    Returns:
        List of dicts with 'name', 'email', 'team' keys, deduplicated by email
    """
    config_mgr = ConfigFileManager('.managerTools.cfg')

    if not config_mgr.contains_key('backstageServer'):
        raise RuntimeError("backstageServer not configured in ~/.managerTools.cfg")

    # If "org" is passed, read teams from orgTeams config
    if teams == ["org"]:
        if not config_mgr.contains_key('orgTeams'):
            raise RuntimeError("orgTeams not configured in ~/.managerTools.cfg (required when using 'org' parameter)")
        teams = config_mgr.get_value('orgTeams')
        if not isinstance(teams, list):
            raise RuntimeError("orgTeams must be an array of team names")

    backstage_server = config_mgr.get_value('backstageServer')
    backstage_auth = config_mgr.get_value('backstageAuth') if config_mgr.contains_key('backstageAuth') else None
    backstage_cache_days = int(config_mgr.get_value('backstageCacheDays')) if config_mgr.contains_key('backstageCacheDays') else 7

    backstage = BackstageREST(backstage_server, backstage_auth)
    cache = BackstageCache(cache_ttl_days=backstage_cache_days)

    members_by_email = {}  # Deduplicate by email, keep first team occurrence

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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python -m managertools.tools.team_usage_roster TEAM1 TEAM2 ... (or 'org' to use orgTeams from config)"}), file=sys.stderr)
        sys.exit(1)

    try:
        result = fetch_team_rosters(sys.argv[1:])
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
