from .output_filter import OutputFilter


class ConvertZerosToEmptyOutputFilter(OutputFilter):
    EMPTY_INCREMENTOR = " "

    def apply(self, column_name, value):
        return self.EMPTY_INCREMENTOR if value == 0 else value
