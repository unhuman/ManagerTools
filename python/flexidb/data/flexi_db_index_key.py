class FlexiDBIndexKey:
    def __init__(self, key: str, value: str):
        self._key = key
        self._value = value

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, FlexiDBIndexKey):
            return False
        return self._key == other._key and self._value == other._value

    def __hash__(self):
        return hash((self._key, self._value))
