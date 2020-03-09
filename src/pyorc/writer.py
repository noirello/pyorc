from typing import Union, Optional, List, BinaryIO, Iterable

from pyorc._pyorc import writer
from .converters import DEFAULT_CONVERTERS
from .enums import CompressionKind, CompressionStrategy, StructRepr
from .typedescription import TypeDescription


class Writer(writer):
    def __init__(
        self,
        fileo: BinaryIO,
        schema: Union[str, TypeDescription],
        batch_size: int = 1024,
        stripe_size: int = 67108864,
        compression: CompressionKind = CompressionKind.ZLIB,
        compression_strategy: CompressionStrategy = CompressionStrategy.SPEED,
        compression_block_size: int = 65536,
        bloom_filter_columns: Optional[List] = None,
        bloom_filter_fpp: float = 0.05,
        struct_repr: StructRepr = StructRepr.TUPLE,
        converters: Optional[dict] = None,
    ) -> None:
        if isinstance(schema, str):
            schema = TypeDescription.from_string(schema)
        elif not isinstance(schema, TypeDescription):
            raise TypeError("Invalid `schema` type, must be string or TypeDescription")
        if 0.0 >= bloom_filter_fpp or bloom_filter_fpp >= 1.0:
            raise ValueError("False positive probability should be > 0.0 & < 1.0")
        self.__schema = schema
        self.__metadata = {}
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
        if converters:
            conv = DEFAULT_CONVERTERS.copy()
            conv.update(converters)
        else:
            conv = converters
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
            conv,
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self) -> None:
        for key, val in self.__metadata.items():
            super()._add_metadata(key, val)
        super().close()

    @property
    def schema(self) -> TypeDescription:
        return self.__schema

    def set_metadata(self, **kwargs) -> None:
        for key, val in kwargs.items():
            if not isinstance(val, bytes):
                raise TypeError(
                    "All values must be bytes, key '{0}' is {1}".format(key, type(val))
                )
            self.__metadata[key] = val

    def writerows(self, rows: Iterable) -> int:
        num = 0
        for row in rows:
            self.write(row)
            num += 1
        return num
