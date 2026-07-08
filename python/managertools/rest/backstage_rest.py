"""Backstage catalog REST client for fetching team member data."""

from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from .rest_service import RestService
from .auth_info import AuthInfo, AuthType


class BackstageREST(RestService):
    """REST client for Backstage software catalog API."""

    def __init__(self, backstage_server: str, auth_token: Optional[str] = None):
        """Initialize Backstage client.

        Args:
            backstage_server: Server FQDN (e.g., "backstage.core.cvent.org")
            auth_token: Optional auth token or cookies. If empty/None, uses unauthenticated access.
                       Auto-detects Cookies (contains '=') vs Bearer token.
        """
        if auth_token and auth_token.strip():
            auth_type = AuthType.Cookies if '=' in auth_token else AuthType.Bearer
            auth_info = AuthInfo(auth_type, auth_token)
        else:
            auth_info = AuthInfo(AuthType.NoAuth)

        super().__init__(auth_info)
        self.backstage_server = backstage_server

    def get_group(self, team_name: str) -> Optional[Dict[str, Any]]:
        """Fetch a group entity from the catalog.

        Args:
            team_name: Group name (e.g., "Rebel-Intelligence")

        Returns:
            Raw group entity JSON, or None if not found / auth failed
        """
        uri = f"https://{self.backstage_server}/api/catalog/entities/by-name/group/default/{team_name}"
        try:
            return self.get_request(uri)
        except Exception as e:
            import sys
            print(f"[WARN] Failed to fetch group {team_name}: {e}", file=sys.stderr)
            return None

    def get_user(self, user_ref: str) -> Optional[Dict[str, Any]]:
        """Fetch a user entity from the catalog.

        Args:
            user_ref: User short name (e.g., "alice.smith")

        Returns:
            Raw user entity JSON, or None if not found
        """
        uri = f"https://{self.backstage_server}/api/catalog/entities/by-name/user/default/{user_ref}"
        try:
            return self.get_request(uri)
        except Exception:
            return None

    def get_team_roster(self, team_name: str, max_workers: int = 5) -> List[Dict[str, Any]]:
        """Fetch all members of a team with their user data.

        Args:
            team_name: Group name (e.g., "Rebel-Intelligence")
            max_workers: Thread pool size for fetching member details

        Returns:
            List of dicts with keys:
            - user_ref: Short username (e.g., "alice.smith")
            - display_name: Displayable name
            - raw_entity: Full user entity JSON from catalog
        """
        group = self.get_group(team_name)
        if not group:
            return []

        # Extract member refs from spec.members (format: "user:default/{name}")
        members = group.get('spec', {}).get('members', [])
        member_refs = []
        for member_ref in members:
            if member_ref.startswith('user:default/'):
                user_ref = member_ref.split('/')[-1]
                member_refs.append(user_ref)

        if not member_refs:
            return []

        # Fetch each member's user entity in parallel
        roster = []

        def fetch_and_build_entry(user_ref):
            user_entity = self.get_user(user_ref)
            if user_entity:
                display_name = (user_entity.get('spec', {}).get('profile', {}).get('displayName')
                              or user_entity.get('metadata', {}).get('name', user_ref))
                return {
                    'user_ref': user_ref,
                    'display_name': display_name,
                    'raw_entity': user_entity
                }
            return None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_and_build_entry, member_refs))

        # Filter out None results (failed fetches)
        return [r for r in results if r is not None]
