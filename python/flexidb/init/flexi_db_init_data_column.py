from .abstract_flexi_db_init_column import AbstractFlexiDBInitColumn


class FlexiDBInitDataColumn(AbstractFlexiDBInitColumn):
    def __init__(self, name: str, default_value):
        super().__init__(name)
        self._default_value = default_value

    def get_default_value(self):
        return self._default_value
