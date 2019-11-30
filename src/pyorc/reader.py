from typing import Union, Optional, List, BinaryIO

from pyorc._pyorc import reader
from .enums import StructRepr
from .converters import DEFAULT_CONVERTERS


class Reader(reader):
    def __init__(
        self,
        fileo,
        batch_size: int = 1024,
        column_indices: Optional[List[int]] = None,
        column_names: Optional[List[str]] = None,
        struct_repr: StructRepr = StructRepr.TUPLE,
        converters: Optional[dict] = None,
    ) -> None:
        if column_indices is None:
            column_indices = []
        if column_names is None:
            column_names = []
        struct_repr = StructRepr(struct_repr)
        if converters:
            conv = DEFAULT_CONVERTERS.copy()
            conv.update(converters)
        else:
            conv = converters
        super().__init__(
            fileo, batch_size, column_indices, column_names, struct_repr, conv
        )

    def iter_stripes(self):
        for num in range(self.num_of_stripes):
            yield self.read_stripe(num)
