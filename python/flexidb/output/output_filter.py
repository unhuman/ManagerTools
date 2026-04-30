from abc import ABC, abstractmethod


class OutputFilter(ABC):
    @abstractmethod
    def apply(self, column_name: str, value):
        pass
