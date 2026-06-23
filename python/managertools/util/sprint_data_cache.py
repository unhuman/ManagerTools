import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

from .log_util import debug_print


class SprintDataCache:
    CACHE_DIR = "cacheData"
    CACHE_VERSION = "2.8"

    @staticmethod
    def _sanitize(s: str) -> str:
        return re.sub(r'[^a-z0-9]', '', (s or '').lower())

    @staticmethod
    def _is_version_compatible(file_version: str) -> bool:
        try:
            file_major, file_minor = map(int, file_version.split('.'))
            own_major, own_minor = map(int, SprintDataCache.CACHE_VERSION.split('.'))
            return file_major == own_major and file_minor >= own_minor
        except (ValueError, AttributeError, IndexError):
            return False

    @staticmethod
    def generate_cache_key(team_name: str, sprint_name: str, start_date: str, end_date: str) -> str:
        prefix = [p for p in [SprintDataCache._sanitize(team_name), SprintDataCache._sanitize(sprint_name)] if p]
        date_range = f"{SprintDataCache._sanitize(start_date)}-{SprintDataCache._sanitize(end_date)}"
        return '_'.join(prefix + [date_range])

    @staticmethod
    def get_cache_file_path(cache_key: str) -> str:
        return f"{SprintDataCache.CACHE_DIR}/{cache_key}.json"

    @staticmethod
    def ensure_cache_directory_exists() -> None:
        Path(SprintDataCache.CACHE_DIR).mkdir(exist_ok=True)

    @staticmethod
    def has_cached_data(team_name: str, sprint_name: str, start_date: str, end_date: str) -> bool:
        cache_key = SprintDataCache.generate_cache_key(team_name, sprint_name, start_date, end_date)
        file_path = SprintDataCache.get_cache_file_path(cache_key)

        if not os.path.exists(file_path):
            debug_print(f"Cache file not found: {file_path}")
            return False

        debug_print(f"Cache file found: {file_path}")
        try:
            with open(file_path, 'r') as f:
                cached_data = json.load(f)
            is_compatible = SprintDataCache._is_version_compatible(cached_data.get("version", ""))
            debug_print(f"Cache version compatible: {is_compatible} (file version: {cached_data.get('version')})")
            return is_compatible
        except Exception as e:
            print(f"Error reading cache file {file_path}: {e}", file=__import__('sys').stderr)
            return False

    @staticmethod
    def is_cache_complete(team_name: str, sprint_name: str, start_date: str, end_date: str,
                          required_sources: Optional[set] = None) -> bool:
        """True when the cache satisfies every required work source. A source is satisfied when it
        is present in the file's `sources` map AND its record is `complete`. Missing/incompatible
        file → True (treated as "nothing outstanding to flag"; the caller fetches fresh).

        required_sources is a set of WorkSource values (e.g. {"pr"}, {"pr","commit"}). When None,
        falls back to "any present source is complete" for backward-compatible callers."""
        meta = SprintDataCache.load_cache_meta(team_name, sprint_name, start_date, end_date)
        if meta is None:
            return True
        sources = meta.get("sources", {})
        if required_sources is None:
            return all(rec.get("complete", True) for rec in sources.values()) if sources else True
        for s in required_sources:
            rec = sources.get(s)
            if rec is None or not rec.get("complete", False):
                return False
        return True

    @staticmethod
    def load_cache_meta(team_name: str, sprint_name: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """Single read of the cache header. Returns the parsed file (incl. `sources` and `data`),
        or None if the file is absent or version-incompatible. The orchestrator uses this for all
        cache decisions so the file is read once rather than three times."""
        cache_key = SprintDataCache.generate_cache_key(team_name, sprint_name, start_date, end_date)
        file_path = SprintDataCache.get_cache_file_path(cache_key)

        if not os.path.exists(file_path):
            return None
        try:
            with open(file_path, 'r') as f:
                cached_data = json.load(f)
        except Exception as e:
            print(f"Error reading cache file {file_path}: {e}", file=__import__('sys').stderr)
            return None

        if not SprintDataCache._is_version_compatible(cached_data.get("version", "")):
            debug_print(f"Cache version incompatible ({cached_data.get('version')}); ignoring {file_path}")
            return None
        cached_data.setdefault("sources", {})
        cached_data.setdefault("data", {})
        return cached_data

    @staticmethod
    def load_cached_data(team_name: str, sprint_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """Return just the cached `data` ({'rows': [...]}). Raises if absent/incompatible — callers
        should gate on has_cached_data/load_cache_meta first."""
        meta = SprintDataCache.load_cache_meta(team_name, sprint_name, start_date, end_date)
        if meta is None:
            raise RuntimeError(f"No compatible cache for {team_name}/{sprint_name} {start_date}-{end_date}")
        return meta.get("data", {})

    @staticmethod
    def save_to_cache(team_name: str, sprint_name: str, start_date: str, end_date: str,
                      data: Dict[str, Any], sources: Optional[Dict[str, Any]] = None) -> None:
        """Persist the sprint's row union plus per-source completeness records.

        `sources` maps a work-source value ("pr"/"commit") to {complete, failed_issues, failed_prs}.
        The caller is responsible for carrying forward records of sources it did not touch this run."""
        if sources is None:
            sources = {}

        SprintDataCache.ensure_cache_directory_exists()

        cache_key = SprintDataCache.generate_cache_key(team_name, sprint_name, start_date, end_date)
        file_path = SprintDataCache.get_cache_file_path(cache_key)

        import time
        cache_data = {
            "version": SprintDataCache.CACHE_VERSION,
            "sources": sources,
            "teamName": team_name,
            "sprintName": sprint_name,
            "startDate": start_date,
            "endDate": end_date,
            "timestamp": int(time.time() * 1000),
            "data": data
        }

        with open(file_path, 'w') as f:
            json.dump(cache_data, f, indent=2)

        debug_print(f"Saved data to cache: {file_path} (sources: {sorted(sources.keys())})")
