from enum import Enum
from typing import List


class SimpleEnum(Enum):
    def __str__(self):
        return self.value

    def __add__(self, other: str):
        return str.__add__(self.value, other)

    @staticmethod
    def to_list(val_list: List[Enum], delimiter: str = "-"):
        return delimiter.join([val.value if hasattr(val, 'value') else val for val in val_list])