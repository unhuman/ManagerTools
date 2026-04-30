from typing import Any

from managertools.flexidb.output.convert_empty_to_zero_output_filter import ConvertEmptyToZeroOutputFilter
from managertools.data.user_activity import UserActivity


class ConvertSelfMetricsEmptyToZeroOutputFilter(ConvertEmptyToZeroOutputFilter):
    def apply(self, column_name: str, value: Any) -> Any:
        if column_name == UserActivity.OTHERS_COMMENTED.name:
            return super().apply(column_name, value)
        return value
