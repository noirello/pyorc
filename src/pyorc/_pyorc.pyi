"""_pyorc c++ extension"""
import typing

from .enums import CompressionKind, CompressionStrategy, StructRepr
from .typedescription import TypeDescription

__all__ = ["reader", "stripe", "writer"]

class reader:
    def __init__(
        self,
        fileo: object,
        batch_size: int = 1024,
        col_indices: typing.List[int] = None,
        col_names: typing.List[str] = None,
        timezone: object = None,
        struct_repr: int = StructRepr.TUPLE,
        conv: object = None,
        predicate: object = None,
        null_value: object = None,
    ) -> None: ...
    def __iter__(self) -> reader: ...
    def __len__(self) -> int: ...
    def __next__(self) -> object: ...
    def _statistics(self, col_idx: int) -> tuple: ...
    def read(self, num: int = -1) -> list: ...
    def seek(self, row: int, whence: int = 0) -> int: ...
    @property
    def bytes_lengths(self) -> dict:
        """
        :type: dict
        """
    @property
    def compression(self) -> int:
        """
        :type: int
        """
    @property
    def compression_block_size(self) -> int:
        """
        :type: int
        """
    @property
    def current_row(self) -> int:
        """
        :type: int
        """
    @property
    def format_version(self) -> tuple:
        """
        :type: tuple
        """
    @property
    def num_of_stripes(self) -> int:
        """
        :type: int
        """
    @property
    def row_index_stride(self) -> int:
        """
        :type: int
        """
    @property
    def schema(self) -> object:
        """
        :type: object
        """
    @property
    def selected_schema(self) -> object:
        """
        :type: object
        """
    @property
    def software_version(self) -> str:
        """
        :type: str
        """
    @property
    def user_metadata(self) -> dict:
        """
        :type: dict
        """
    @property
    def writer_id(self) -> int:
        """
        :type: int
        """
    @property
    def writer_version(self) -> int:
        """
        :type: int
        """
    pass

class stripe:
    def __init__(self, reader: reader, stripe_idx: int) -> None: ...
    def __iter__(self) -> stripe: ...
    def __len__(self) -> int: ...
    def __next__(self) -> object: ...
    def _statistics(self, col_idx: int) -> tuple: ...
    def read(self, num: int = -1) -> list: ...
    def seek(self, row: int, whence: int = 0) -> int: ...
    @property
    def bloom_filter_columns(self) -> tuple:
        """
        :type: tuple
        """
    @property
    def bytes_length(self) -> int:
        """
        :type: int
        """
    @property
    def bytes_offset(self) -> int:
        """
        :type: int
        """
    @property
    def current_row(self) -> int:
        """
        :type: int
        """
    @property
    def row_offset(self) -> int:
        """
        :type: int
        """
    @property
    def writer_timezone(self) -> str:
        """
        :type: str
        """
    pass

class writer:
    def __init__(
        self,
        fileo: object,
        schema: object,
        batch_size: int = 1024,
        stripe_size: int = 67108864,
        row_index_stride: int = 10000,
        compression: int = CompressionKind.ZLIB,
        compression_strategy: int = CompressionStrategy.SPEED,
        compression_block_size: int = 65536,
        bloom_filter_columns: typing.Set[int] = None,
        bloom_filter_fpp: float = 0.05,
        timezone: object = None,
        struct_repr: int = StructRepr.TUPLE,
        conv: object = None,
        padding_tolerance: float = 0.0,
        dict_key_size_threshold: float = 0.0,
        null_value: object = None,
    ) -> None: ...
    def _add_user_metadata(self, key: str, value: bytes) -> None: ...
    def close(self) -> None: ...
    def write(self, row: object) -> None: ...
    def writerows(self, rows: typing.Iterable) -> int: ...
    @property
    def current_row(self) -> int:
        """
        :type: int
        """
    pass

def _orc_version() -> str:
    pass

def _schema_from_string(arg0: str) -> TypeDescription:
    pass
