import json
import os
from pathlib import Path
from typing import Any, Optional


class ConfigFileManager:
    def __init__(self, filename: str):
        home = os.path.expanduser("~")
        self.filename = os.path.join(home, filename)
        self.state = {}

        try:
            with open(self.filename, 'r') as f:
                self.state = json.load(f)
        except FileNotFoundError:
            self.state = {}

    def contains_key(self, key: str) -> bool:
        keys = key.split(".")
        current = self.state

        for k in keys:
            matched_key = self._find_case_insensitive_key(current, k)
            if matched_key is None:
                return False
            current = current[matched_key]

        return True

    def get_value(self, key: str) -> Any:
        keys = key.split(".")
        current = self.state

        for k in keys:
            matched_key = self._find_case_insensitive_key(current, k)
            if matched_key is None:
                raise RuntimeError(f"Could not find key: {key}")
            current = current[matched_key]

        return current

    def store_value(self, key: str, value: Any) -> None:
        self.update_value(key, value)

    def update_value(self, key: str, value: Any) -> None:
        keys = key.split(".")
        current = self.state

        for i, subkey in enumerate(keys):
            if i == len(keys) - 1:
                matched_key = self._find_case_insensitive_key(current, subkey)
                current[(matched_key if matched_key else subkey)] = value
            else:
                matched_key = self._find_case_insensitive_key(current, subkey)
                if matched_key is None:
                    raise RuntimeError(f"Could not find key: {key}")
                current = current[matched_key]

        Path(self.filename).parent.mkdir(parents=True, exist_ok=True)
        with open(self.filename, 'w') as f:
            json.dump(self.state, f, indent=2)

    @staticmethod
    def _find_case_insensitive_key(d: dict, key: str) -> Optional[str]:
        for k in d.keys():
            if k.lower() == key.lower():
                return k
        return None
