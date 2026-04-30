import csv
import io
from typing import List, Any


class FlexiDBRow(dict):
    _defaults: dict = {}

    def __init__(self, initial_capacity: int = 0, other: 'FlexiDBRow' = None):
        if other is not None:
            super().__init__(other)
        else:
            super().__init__()

    @classmethod
    def set_default(cls, key: str, value: Any):
        cls._defaults[key] = value

    @classmethod
    def get_default(cls, key: str) -> Any:
        return cls._defaults.get(key)

    def __missing__(self, key):
        if key in self._defaults:
            return self._defaults[key]
        return None

    def get(self, key, default=None):
        if key in self:
            return super().__getitem__(key)
        if key in self._defaults:
            return self._defaults[key]
        return default

    @staticmethod
    def headings_to_csv(column_order: List[str]) -> str:
        parts = []
        for col in column_order:
            parts.append(col if col is not None else "")
        return ",".join(parts)

    def to_csv(self, column_order: List[str], output_rules: list = None) -> str:
        if output_rules is None:
            output_rules = []
        parts = []
        for col in column_order:
            if col is None:
                parts.append("")
                continue

            value = self.get(col)

            for rule in output_rules:
                new_value = rule.apply(col, value)
                if new_value is not value:
                    value = new_value
                    break

            if value is None:
                parts.append("")
                continue

            if isinstance(value, list):
                value = "\n".join(str(v) for v in value)

            parts.append(_escape_csv(str(value)))

        return ",".join(parts)


def _escape_csv(value: str) -> str:
    if any(c in value for c in (',', '"', '\n', '\r')):
        return '"' + value.replace('"', '""') + '"'
    return value
