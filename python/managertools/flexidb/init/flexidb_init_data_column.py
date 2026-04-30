from .abstract_flexidb_init_column import AbstractFlexiDBInitColumn


class FlexiDBInitDataColumn(AbstractFlexiDBInitColumn):
    def __init__(self, name, default_value):
        super().__init__(name)
        self.default_value = default_value

    def get_default_value(self):
        return self.default_value
