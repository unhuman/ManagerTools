import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional


class PRDataCache:
    CACHE_DIR = "cacheData/prs"
    CACHE_VERSION = "1.0"

    @staticmethod
    def _url_to_key(pr_url: str) -> str:
        return hashlib.md5(pr_url.encode()).hexdigest()

    @staticmethod
    def _get_path(pr_url: str) -> str:
        return f"{PRDataCache.CACHE_DIR}/{PRDataCache._url_to_key(pr_url)}.json"

    @staticmethod
    def has_cached_pr(pr_url: str) -> bool:
        path = PRDataCache._get_path(pr_url)
        if not os.path.exists(path):
            return False
        try:
            with open(path) as f:
                data = json.load(f)
            return data.get('version') == PRDataCache.CACHE_VERSION
        except Exception:
            return False

    @staticmethod
    def load_cached_pr(pr_url: str) -> Dict[str, Any]:
        with open(PRDataCache._get_path(pr_url)) as f:
            data = json.load(f)
        return data.get('data', {})

    @staticmethod
    def save_pr(pr_url: str, data: Dict[str, Any]) -> None:
        Path(PRDataCache.CACHE_DIR).mkdir(parents=True, exist_ok=True)
        path = PRDataCache._get_path(pr_url)
        cache_data = {
            'version': PRDataCache.CACHE_VERSION,
            'url': pr_url,
            'timestamp': int(time.time() * 1000),
            'data': data,
        }
        with open(path, 'w') as f:
            json.dump(cache_data, f, indent=2)
