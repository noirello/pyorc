from collections import defaultdict
from collections.abc import Iterator, Iterable
from typing import Any, BinaryIO, Generic, TYPE_CHECKING

from pyorc._pyorc import reader, stripe

from .converters import DEFAULT_CONVERTERS, ORCConverter
from .enums import CompressionKind, StructRepr, TypeKind, WriterVersion
from .predicates import Predicate

import zoneinfo


if TYPE_CHECKING:
    from typing_extensions import TypeVar
    T = TypeVar("T", default=Any)
else:
    from typing import TypeVar
    T = TypeVar("T")

class Column(Generic[T]):
    def __init__(self, stream: "Reader[T] | Stripe[T]", index: int):
        self.index = index
        self.stream = stream
        self._stats = self.stream._statistics(self.index)

    @property
    def statistics(self) -> dict[str, Any]:
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


class Stripe(stripe[T], Generic[T]):
    def __getitem__(self, col_idx: int) -> "Column[T]":
        return Column(self, col_idx)


class Reader(reader[T], Generic[T]):
    def __init__(
        self,
        fileo: BinaryIO,
        batch_size: int = 1024,
        column_indices: Iterable[int] | None = None,
        column_names: Iterable[str] | None = None,
        timezone: zoneinfo.ZoneInfo = zoneinfo.ZoneInfo("UTC"),
        struct_repr: StructRepr = StructRepr.TUPLE,
        converters: dict[TypeKind, type[ORCConverter]] | None = None,
        predicate: Predicate | None = None,
        null_value: Any = None,
    ) -> None:
        if column_indices is None:
            column_indices = []
        if column_names is None:
            column_names = []
        struct_repr = StructRepr(struct_repr)
        conv = None
        if converters:
            conv = DEFAULT_CONVERTERS.copy()
            conv.update(converters)
        else:
            conv = converters
        super().__init__(
            fileo,
            batch_size,
            column_indices,
            column_names,
            timezone,
            struct_repr,
            conv,
            predicate,
            null_value,
        )

    def __getitem__(self, col_idx: int) -> Column[T]:
        return Column(self, col_idx)

    def read_stripe(self, stripe_idx: int) -> Stripe[T]:
        return Stripe(self, stripe_idx)

    def iter_stripes(self) -> Iterator[Stripe[T]]:
        for num in range(self.num_of_stripes):
            yield self.read_stripe(num)

    @property
    def compression(self) -> CompressionKind:
        return CompressionKind(super().compression)

    @property
    def writer_id(self) -> str:  # type: ignore[override]
        wid = super().writer_id
        if wid == 0:
            return "ORC_JAVA_WRITER"
        elif wid == 1:
            return "ORC_CPP_WRITER"
        elif wid == 2:
            return "PRESTO_WRITER"
        elif wid == 3:
            return "SCRITCHLEY_GO"
        elif wid == 4:
            return "TRINO_WRITER"
        elif wid == 5:
            return "CUDF_WRITER"
        else:
            return "UNKNOWN_WRITER"

    @property
    def writer_version(self) -> WriterVersion:
        return WriterVersion(super().writer_version)
