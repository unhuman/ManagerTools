import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


class SprintDataCache:
    CACHE_DIR = "cacheData"
    CACHE_VERSION = "1.0"

    @staticmethod
    def generate_cache_key(team_name: str, sprint_name: str, start_date: str, end_date: str) -> str:
        input_str = f"{team_name.lower() if team_name else ''}|{sprint_name.lower() if sprint_name else ''}|{start_date.lower() if start_date else ''}|{end_date.lower() if end_date else ''}"
        digest = hashlib.md5(input_str.encode("utf-8")).hexdigest()
        return digest

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
            return False

        try:
            with open(file_path, 'r') as f:
                cached_data = json.load(f)
            return cached_data.get("version") == SprintDataCache.CACHE_VERSION
        except Exception as e:
            print(f"Error reading cache file {file_path}: {e}", file=__import__('sys').stderr)
            return False

    @staticmethod
    def load_cached_data(team_name: str, sprint_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
        cache_key = SprintDataCache.generate_cache_key(team_name, sprint_name, start_date, end_date)
        file_path = SprintDataCache.get_cache_file_path(cache_key)

        print(f"Loading cached data from: {file_path}")

        with open(file_path, 'r') as f:
            cached_data = json.load(f)

        if cached_data.get("version") != SprintDataCache.CACHE_VERSION:
            raise RuntimeError(f"Cache version mismatch. Expected {SprintDataCache.CACHE_VERSION}, found {cached_data.get('version')}")

        return cached_data.get("data", {})

    @staticmethod
    def save_to_cache(team_name: str, sprint_name: str, start_date: str, end_date: str, data: Dict[str, Any]) -> None:
        SprintDataCache.ensure_cache_directory_exists()

        cache_key = SprintDataCache.generate_cache_key(team_name, sprint_name, start_date, end_date)
        file_path = SprintDataCache.get_cache_file_path(cache_key)

        import time
        cache_data = {
            "version": SprintDataCache.CACHE_VERSION,
            "teamName": team_name,
            "sprintName": sprint_name,
            "startDate": start_date,
            "endDate": end_date,
            "timestamp": int(time.time() * 1000),
            "data": data
        }

        with open(file_path, 'w') as f:
            json.dump(cache_data, f, indent=2)

        print(f"Saved data to cache: {file_path}")
