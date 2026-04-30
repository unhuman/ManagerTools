from .output_filter import OutputFilter


class ConvertEmptyToZeroOutputFilter(OutputFilter):
    EMPTY_INCREMENTOR = " "

    def apply(self, column_name, value):
        return 0 if value == self.EMPTY_INCREMENTOR else value
