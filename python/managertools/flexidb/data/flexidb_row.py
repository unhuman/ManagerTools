import csv
from io import StringIO
from typing import Any, Dict, List, Optional

from managertools.flexidb.output.output_filter import OutputFilter


class FlexiDBRow(dict):
    defaults: Dict[str, Any] = {}

    CSV_SEPARATOR = ','
    SPACE_STRING = " "

    def __init__(self, other=None):
        super().__init__()
        if other is not None and isinstance(other, dict):
            self.update(other)

    @classmethod
    def set_default(cls, key: str, value: Any) -> None:
        cls.defaults[key] = value

    @classmethod
    def get_default(cls, key: str) -> Any:
        return cls.defaults.get(key)

    def get(self, key: str, default=None) -> Any:
        if key in self:
            return super().get(key)
        return self.defaults.get(key, default)

    @staticmethod
    def headings_to_csv(column_order: List[Optional[str]]) -> str:
        fields = []
        for column_name in column_order:
            if column_name is None:
                fields.append("")
            else:
                fields.append(column_name)

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(fields)
        return output.getvalue().rstrip('\r\n')

    def to_csv(self, column_order: List[Optional[str]], output_rules: List[OutputFilter] = None) -> str:
        if output_rules is None:
            output_rules = []

        fields = []
        for column_name in column_order:
            if column_name is None:
                fields.append("")
            else:
                value = self.get(column_name)

                # Apply output rules
                for output_rule in output_rules:
                    check_value = output_rule.apply(column_name, value)
                    if check_value != value:
                        value = check_value
                        break

                # null values format as a space
                if value is None:
                    value = self.SPACE_STRING

                # Handle lists
                if isinstance(value, list):
                    value = "\n".join(str(item) for item in value)

                # Ensure we output something for formatting
                if value == "":
                    value = self.SPACE_STRING

                fields.append(str(value))

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(fields)
        return output.getvalue().rstrip('\r\n')
