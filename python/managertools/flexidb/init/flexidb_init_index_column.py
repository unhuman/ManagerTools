from .abstract_flexidb_init_column import AbstractFlexiDBInitColumn


class FlexiDBInitIndexColumn(AbstractFlexiDBInitColumn):
    def __init__(self, name):
        super().__init__(name)
