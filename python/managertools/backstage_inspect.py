#!/usr/bin/env python3
"""Backstage catalog inspection tool.

Helps discover the exact field names and authentication requirements for Backstage integration.

Usage:
    python -m managertools.backstage_inspect -t Rebel-Intelligence
"""

import sys
import json
import argparse
import re
from typing import Any, Dict, List

from managertools.util.command_line_helper import CommandLineHelper
from managertools.rest.backstage_rest import BackstageREST


def find_possible_title_fields(obj: Any, path: str = "", matches: Dict[str, Any] = None) -> Dict[str, Any]:
    """Recursively search object for likely title/role field names.

    Returns:
        Dict mapping field path -> value for likely title fields
    """
    if matches is None:
        matches = {}

    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            # Look for common title/role field names
            if any(pattern in key.lower() for pattern in ['title', 'role', 'position', 'jobtitle', 'grade', 'level', 'rank']):
                matches[current_path] = value
            # Recurse into nested structures
            if isinstance(value, (dict, list)):
                find_possible_title_fields(value, current_path, matches)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            current_path = f"{path}[{i}]"
            find_possible_title_fields(item, current_path, matches)

    return matches


def truncate_json(obj: Any, max_length: int = 2000) -> str:
    """Render object as JSON, truncated if needed."""
    full_json = json.dumps(obj, indent=2)
    if len(full_json) > max_length:
        return full_json[:max_length] + f"\n... [truncated, total {len(full_json)} chars]"
    return full_json


def main():
    parser = argparse.ArgumentParser(description='Backstage catalog inspection tool')
    parser.add_argument('-t', '--team', required=True, help='Team name to inspect (e.g., Rebel-Intelligence)')
    parser.add_argument('--show-raw-group', action='store_true', help='Show full raw group entity JSON')
    parser.add_argument('--show-raw-user', action='store_true', help='Show full raw user entity JSON')
    args = parser.parse_args()

    # Load config and prompt for Backstage server/auth if needed
    config_helper = CommandLineHelper(".managerTools.cfg")

    print(f"🔍 Backstage Catalog Inspection Tool")
    print(f"{'='*60}")
    print(f"Team: {args.team}\n")

    try:
        backstage_server = config_helper.get_backstage_server()
        backstage_auth = config_helper.get_backstage_auth()
        print(f"Server: {backstage_server}")
        print(f"Auth: {'configured' if backstage_auth else 'none (unauthenticated)'}\n")
    except Exception as e:
        print(f"❌ Failed to load Backstage configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Initialize Backstage client
    backstage = BackstageREST(backstage_server, backstage_auth)

    # Fetch group
    print(f"📋 Fetching group '{args.team}'...")
    group = backstage.get_group(args.team)
    if not group:
        print(f"❌ Failed to fetch group (check auth, server, or team name)", file=sys.stderr)
        sys.exit(1)

    print(f"✓ Group found\n")

    if args.show_raw_group:
        print("Raw group entity (truncated):")
        print(truncate_json(group))
        print()

    # Extract and fetch a sample member
    members = group.get('spec', {}).get('members', [])
    if not members:
        print(f"⚠️  No members found in this group", file=sys.stderr)
        sys.exit(0)

    sample_member_ref = members[0]
    if sample_member_ref.startswith('user:default/'):
        sample_user_ref = sample_member_ref.split('/')[-1]
    else:
        sample_user_ref = sample_member_ref

    print(f"👤 Fetching sample member: {sample_user_ref}")
    user_entity = backstage.get_user(sample_user_ref)
    if not user_entity:
        print(f"❌ Failed to fetch user entity", file=sys.stderr)
        sys.exit(1)

    print(f"✓ User entity found\n")

    if args.show_raw_user:
        print("Raw user entity (truncated):")
        print(truncate_json(user_entity))
        print()

    # Search for title/role fields
    print(f"🔎 Scanning for possible title/role fields...\n")
    title_matches = find_possible_title_fields(user_entity)

    if title_matches:
        print("Possible title fields found:")
        for path, value in sorted(title_matches.items()):
            value_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            if len(value_str) > 100:
                value_str = value_str[:100] + "..."
            print(f"  • {path} = {value_str}")
    else:
        print("No obvious title/role fields found.")
        print("Try running with --show-raw-user to inspect the full user entity structure.")

    print(f"\n{'='*60}")
    print("Report:")
    print(f"  - Group fetch: {'✓ Success (no auth needed)' if group else '✗ Failed (check auth)'}")
    print(f"  - User fetch: {'✓ Success' if user_entity else '✗ Failed'}")
    print(f"  - Title fields found: {len(title_matches)}")

    if title_matches:
        recommended = sorted(title_matches.keys())[0]
        print(f"\nTo complete the integration, run with:")
        print(f"  python -m managertools.backstage_inspect -t {args.team} --show-raw-user")
        print(f"Then report back the exact field path for the title/role field (e.g., '{recommended}')")


if __name__ == '__main__':
    main()
