import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

from .log_util import debug_print


class PRDataCache:
    """Cache for GitHub PR data keyed by PR URL. Only caches merged PRs (immutable)."""
    CACHE_DIR = "cacheData/pr"
    CACHE_VERSION = "1.0"

    @staticmethod
    def _url_to_key(pr_url: str) -> str:
        """Convert PR URL to a safe filename.

        Example: https://api.github.com/repos/org/repo/pulls/123 -> org_repo_pulls_123
        """
        sanitized = re.sub(r'[^a-z0-9]', '_', pr_url.lower()).strip('_')
        return sanitized

    @staticmethod
    def load(pr_url: str) -> Optional[Dict[str, Any]]:
        """Load cached PR data if it exists and version matches."""
        key = PRDataCache._url_to_key(pr_url)
        path = f"{PRDataCache.CACHE_DIR}/{key}.json"

        if not os.path.exists(path):
            return None

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            if data.get("version") != PRDataCache.CACHE_VERSION:
                debug_print(f"PR cache version mismatch for {pr_url}: expected {PRDataCache.CACHE_VERSION}, got {data.get('version')}")
                return None

            return data.get("pr_full")
        except Exception as e:
            debug_print(f"Error loading PR cache for {pr_url}: {e}")
            return None

    @staticmethod
    def save(pr_url: str, pr_full: Dict[str, Any]) -> None:
        """Save merged PR data to disk cache.

        Only caches PRs that have been merged (merged_ms > 0), as their data
        is immutable. Open PRs should not be persisted.
        """
        if pr_full.get("merged_ms", 0) <= 0:
            return

        try:
            Path(PRDataCache.CACHE_DIR).mkdir(parents=True, exist_ok=True)

            key = PRDataCache._url_to_key(pr_url)
            path = f"{PRDataCache.CACHE_DIR}/{key}.json"

            cache_data = {
                "version": PRDataCache.CACHE_VERSION,
                "pr_url": pr_url,
                "pr_full": pr_full
            }

            with open(path, 'w') as f:
                json.dump(cache_data, f, indent=2)

            debug_print(f"Cached merged PR data: {pr_url}")
        except Exception as e:
            debug_print(f"Error saving PR cache for {pr_url}: {e}")
