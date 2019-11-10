from typing import Union, Optional, List, BinaryIO, Iterable

from pyorc._pyorc import writer, typedescription
from .enums import CompressionKind, CompressionStrategy, StructRepr


class Writer(writer):
    def __init__(
        self,
        fileo: BinaryIO,
        schema: Union[str, typedescription],
        batch_size: int = 1024,
        stripe_size: int = 67108864,
        compression: CompressionKind = CompressionKind.ZLIB,
        compression_strategy: CompressionStrategy = CompressionStrategy.SPEED,
        compression_block_size: int = 65536,
        bloom_filter_columns: Optional[List] = None,
        bloom_filter_fpp: float = 0.05,
        struct_repr: StructRepr = StructRepr.TUPLE,
    ) -> None:
        if isinstance(schema, str):
            schema = typedescription(schema)
        elif not isinstance(schema, typedescription):
            raise TypeError("Invalid `schema` type, must be string or typedescription")
        if 0.0 >= bloom_filter_fpp or bloom_filter_fpp >= 1.0:
            raise ValueError("False positive probability should be > 0.0 & < 1.0")
        self.__schema = schema
        comp = CompressionKind(compression)
        comp_strat = CompressionStrategy(compression_strategy)
        bf_set = set()
        if bloom_filter_columns:
            if any(not isinstance(item, (int, str)) for item in bloom_filter_columns):
                raise ValueError(
                    "All items in `bloom_filter_columns` mut be string or int"
                )
            for item in bloom_filter_columns:
                if isinstance(item, int):
                    bf_set.add(item)
                elif isinstance(item, str):
                    bf_set.add(self.__schema.find_column_id(item))
        super().__init__(
            fileo,
            self.__schema,
            batch_size,
            stripe_size,
            comp,
            comp_strat,
            compression_block_size,
            bf_set,
            bloom_filter_fpp,
            struct_repr,
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        super().close()

    @property
    def schema(self) -> typedescription:
        return self.__schema

    def writerows(self, rows: Iterable):
        num = 0
        for row in rows:
            self.write(row)
            num += 1
        return num
