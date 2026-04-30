class FlexiDBIndexKey:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, FlexiDBIndexKey):
            return False
        return self.key == other.key and self.value == other.value

    def __hash__(self):
        return hash((self.key, self.value))

    def __repr__(self):
        return f"FlexiDBIndexKey('{self.key}':'{self.value}')"
