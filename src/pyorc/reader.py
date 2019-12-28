from collections import defaultdict
from typing import Union, Optional, List, BinaryIO

from pyorc._pyorc import reader, stripe
from .enums import StructRepr, TypeKind
from .converters import DEFAULT_CONVERTERS


class Column:
    def __init__(self, stream, index):
        self.index = index
        self.stream = stream
        self._stats = self.stream._statistics(self.index)

    @property
    def statistics(self):
        result = {}
        result_list = defaultdict(list)
        for stat in self._stats:
            for key, val in stat.items():
                result_list[key].append(val)
        for key, values in result_list.items():
            if key in (
                "number_of_values",
                "sum",
                "false_count",
                "true_count",
                "total_length",
            ):
                result[key] = sum(values)
            elif key in ("minimum", "lower_bound"):
                result[key] = min(values)
            elif key in ("maximum", "upper_bound"):
                result[key] = max(values)
            elif key == "has_null":
                result[key] = any(values)
        result["kind"] = TypeKind(result_list["kind"][0])
        return result


class Stripe(stripe):
    def __getitem__(self, col_idx):
        return Column(self, col_idx)


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

    def __getitem__(self, col_idx):
        return Column(self, col_idx)

    def read_stripe(self, stripe_idx):
        return Stripe(self, stripe_idx)

    def iter_stripes(self):
        for num in range(self.num_of_stripes):
            yield self.read_stripe(num)
