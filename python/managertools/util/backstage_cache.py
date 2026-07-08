"""Simple caching for Backstage team roster data."""

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any


class BackstageCache:
    """Cache for Backstage team rosters with TTL support."""

    CACHE_DIR = "cacheData/backstage"

    def __init__(self, cache_ttl_days: int = 7):
        """Initialize cache with optional TTL.

        Args:
            cache_ttl_days: Cache validity in days (default 7)
        """
        self.cache_ttl_days = cache_ttl_days
        os.makedirs(self.CACHE_DIR, exist_ok=True)

    def _cache_path(self, team_name: str) -> str:
        """Get cache file path for a team."""
        return os.path.join(self.CACHE_DIR, f"{team_name}.json")

    def get(self, team_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached roster for a team, if valid.

        Args:
            team_name: Team name (e.g., "Rebel-Intelligence")

        Returns:
            Cached roster list, or None if missing or expired
        """
        cache_file = self._cache_path(team_name)
        if not os.path.exists(cache_file):
            return None

        try:
            with open(cache_file) as f:
                cache_data = json.load(f)

            fetched_at_str = cache_data.get('fetched_at')
            if not fetched_at_str:
                return None

            fetched_at = datetime.fromisoformat(fetched_at_str)
            now = datetime.now(timezone.utc)
            age = now - fetched_at.replace(tzinfo=timezone.utc) if fetched_at.tzinfo else (now.replace(tzinfo=None) - fetched_at)

            if age > timedelta(days=self.cache_ttl_days):
                # Cache expired
                return None

            return cache_data.get('roster')
        except Exception:
            return None

    def put(self, team_name: str, roster: List[Dict[str, Any]]) -> None:
        """Cache a team roster.

        Args:
            team_name: Team name
            roster: Roster list from BackstageREST.get_team_roster()
        """
        cache_file = self._cache_path(team_name)
        cache_data = {
            'fetched_at': datetime.now(timezone.utc).isoformat(),
            'roster': roster
        }
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            import sys
            print(f"[WARN] Failed to write cache for {team_name}: {e}", file=sys.stderr)

    def clear(self, team_name: Optional[str] = None) -> None:
        """Clear cache entries.

        Args:
            team_name: Team to clear, or None to clear all
        """
        if team_name:
            cache_file = self._cache_path(team_name)
            if os.path.exists(cache_file):
                os.remove(cache_file)
        else:
            if os.path.exists(self.CACHE_DIR):
                for f in os.listdir(self.CACHE_DIR):
                    os.remove(os.path.join(self.CACHE_DIR, f))
