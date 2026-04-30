class FlexiDBQueryColumn:
    def __init__(self, name: str, match_value):
        self._name = name
        self._match_value = match_value

    def get_name(self) -> str:
        return self._name

    def get_match_value(self):
        return self._match_value

    def __repr__(self) -> str:
        q = "'" if isinstance(self._match_value, str) else ""
        return f"FlexiDBQueryColumn{{'{self._name}':{q}{self._match_value}{q}}}"
