class FlexiDBQueryColumn:
    def __init__(self, name, match_value):
        self.name = name
        self.match_value = match_value

    def get_name(self):
        return self.name

    def get_match_value(self):
        return self.match_value

    def __repr__(self):
        if isinstance(self.match_value, str):
            return f"FlexiDBQueryColumn{{'{self.name}':'{self.match_value}'}}"
        return f"FlexiDBQueryColumn{{'{self.name}':{self.match_value}}}"
