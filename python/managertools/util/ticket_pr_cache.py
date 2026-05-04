import json
import os
import time
from pathlib import Path
from typing import Any, List


class TicketPRCache:
    CACHE_DIR = "cacheData/ticket_prs"
    CACHE_VERSION = "1.0"

    @staticmethod
    def _get_path(issue_id: str) -> str:
        return f"{TicketPRCache.CACHE_DIR}/{issue_id}.json"

    @staticmethod
    def has_cached(issue_id: str) -> bool:
        path = TicketPRCache._get_path(issue_id)
        if not os.path.exists(path):
            return False
        try:
            with open(path) as f:
                data = json.load(f)
            return data.get('version') == TicketPRCache.CACHE_VERSION
        except Exception:
            return False

    @staticmethod
    def load_cached(issue_id: str) -> List[Any]:
        with open(TicketPRCache._get_path(issue_id)) as f:
            data = json.load(f)
        return data.get('pull_requests', [])

    @staticmethod
    def save(issue_id: str, pull_requests: List[Any]) -> None:
        Path(TicketPRCache.CACHE_DIR).mkdir(parents=True, exist_ok=True)
        cache_data = {
            'version': TicketPRCache.CACHE_VERSION,
            'issue_id': issue_id,
            'timestamp': int(time.time() * 1000),
            'pull_requests': pull_requests,
        }
        with open(TicketPRCache._get_path(issue_id), 'w') as f:
            json.dump(cache_data, f, indent=2)
