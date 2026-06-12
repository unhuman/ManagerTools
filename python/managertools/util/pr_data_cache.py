import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

from .log_util import debug_print


class PRDataCache:
    """Cache for GitHub PR data keyed by team + ticket + PR ID. Only caches merged PRs (immutable)."""
    CACHE_VERSION = "1.1"

    def __init__(self, team_name: str, cache_base_dir: str = "cacheData"):
        """Initialize cache with team-namespaced directory as a run lock.

        Args:
            team_name: The team name (will be sanitized to create directory)
            cache_base_dir: Base cache directory (default: cacheData)

        Raises:
            RuntimeError: If the team cache directory already exists (another run in progress)
        """
        safe = re.sub(r'[^a-z0-9]', '_', team_name.lower()).strip('_')
        self._cache_dir = os.path.join(cache_base_dir, "pr", safe)

        if os.path.isdir(self._cache_dir):
            raise RuntimeError(
                f"Team '{team_name}' appears to already be running. "
                f"If this is an error, delete: {self._cache_dir}"
            )
        os.makedirs(self._cache_dir)

    @staticmethod
    def _make_key(ticket: str, pr_id: str) -> str:
        """Create a cache key from ticket and PR ID.

        Example: PROJ-123, 42 -> proj_123_42
        """
        def sanitize(s: str) -> str:
            return re.sub(r'[^a-z0-9]', '_', str(s).lower()).strip('_')
        return f"{sanitize(ticket)}_{sanitize(pr_id)}"

    def load(self, ticket: str, pr_id: str) -> Optional[Dict[str, Any]]:
        """Load cached PR data if it exists and version matches."""
        key = self._make_key(ticket, pr_id)
        path = os.path.join(self._cache_dir, f"{key}.json")

        if not os.path.exists(path):
            return None

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            if data.get("version") != PRDataCache.CACHE_VERSION:
                debug_print(f"PR cache version mismatch for {ticket}/{pr_id}: expected {PRDataCache.CACHE_VERSION}, got {data.get('version')}")
                return None

            return data.get("pr_full")
        except Exception as e:
            debug_print(f"Error loading PR cache for {ticket}/{pr_id}: {e}")
            return None

    def save(self, ticket: str, pr_id: str, pr_url: str, pr_full: Dict[str, Any]) -> None:
        """Save merged PR data to disk cache.

        Only caches PRs that have been merged (merged_ms > 0), as their data
        is immutable. Open PRs should not be persisted.
        """
        if pr_full.get("merged_ms", 0) <= 0:
            return

        try:
            key = self._make_key(ticket, pr_id)
            path = os.path.join(self._cache_dir, f"{key}.json")

            cache_data = {
                "version": PRDataCache.CACHE_VERSION,
                "pr_url": pr_url,
                "pr_full": pr_full
            }

            with open(path, 'w') as f:
                json.dump(cache_data, f, indent=2)

            debug_print(f"Cached merged PR data: {ticket}/{pr_id}")
        except Exception as e:
            debug_print(f"Error saving PR cache for {ticket}/{pr_id}: {e}")

    def release(self) -> None:
        """Delete the team cache directory, releasing the run lock."""
        if os.path.isdir(self._cache_dir):
            shutil.rmtree(self._cache_dir)
            debug_print(f"PR cache released: {self._cache_dir}")
