"""Backstage catalog REST client for fetching team member data."""

from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import json
import os

from .rest_service import RestService
from .auth_info import AuthInfo, AuthType


class BackstageREST(RestService):
    """REST client for Backstage software catalog API."""

    TEAM_NAME_CACHE_FILE = "cacheData/backstage_team_names.json"

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
        self._team_name_cache = self._load_team_name_cache()

    @staticmethod
    def _load_team_name_cache() -> Dict[str, str]:
        """Load cached team name mappings (original -> successful variant).

        Returns:
            Dict mapping team names to their working Backstage names
        """
        if os.path.exists(BackstageREST.TEAM_NAME_CACHE_FILE):
            try:
                with open(BackstageREST.TEAM_NAME_CACHE_FILE, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_team_name_cache(self):
        """Save team name mappings to cache file."""
        os.makedirs(os.path.dirname(self.TEAM_NAME_CACHE_FILE), exist_ok=True)
        try:
            with open(self.TEAM_NAME_CACHE_FILE, 'w') as f:
                json.dump(self._team_name_cache, f, indent=2)
        except IOError:
            pass  # Silent fail if can't write cache

    @staticmethod
    def _camel_to_kebab(name: str) -> str:
        """Convert camelCase to kebab-case.

        Example: DumbledoreArmy -> Dumbledore-Army
        """
        import re
        return re.sub(r'([a-z])([A-Z])', r'\1-\2', name)

    def get_group(self, team_name: str) -> Optional[Dict[str, Any]]:
        """Fetch a group entity from the catalog.

        Args:
            team_name: Group name (e.g., "Rebel-Intelligence" or "DumbledoreArmy")

        Returns:
            Raw group entity JSON, or None if not found / auth failed
        """
        import sys

        # Check cache first
        if team_name in self._team_name_cache:
            cached_name = self._team_name_cache[team_name]
            uri = f"https://{self.backstage_server}/api/catalog/entities/by-name/group/default/{cached_name}"
            try:
                return self.get_request(uri)
            except Exception:
                # Cache was stale, fall through to retry
                pass

        # Try the team name as provided
        uri = f"https://{self.backstage_server}/api/catalog/entities/by-name/group/default/{team_name}"
        try:
            return self.get_request(uri)
        except Exception as e:
            error_msg = str(e)
            # If 404, try converting camelCase to kebab-case
            if '404' in error_msg or 'Not Found' in error_msg:
                kebab_name = self._camel_to_kebab(team_name)
                if kebab_name != team_name:
                    uri_kebab = f"https://{self.backstage_server}/api/catalog/entities/by-name/group/default/{kebab_name}"
                    try:
                        result = self.get_request(uri_kebab)
                        # Cache the successful variant
                        self._team_name_cache[team_name] = kebab_name
                        self._save_team_name_cache()
                        print(f"[INFO] Team '{team_name}' found as '{kebab_name}' (cached for future use)", file=sys.stderr)
                        return result
                    except Exception:
                        pass

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
