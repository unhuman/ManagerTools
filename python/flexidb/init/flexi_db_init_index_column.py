from .abstract_flexi_db_init_column import AbstractFlexiDBInitColumn


class FlexiDBInitIndexColumn(AbstractFlexiDBInitColumn):
    def __init__(self, name: str):
        super().__init__(name)
