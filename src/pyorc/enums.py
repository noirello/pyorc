import enum


class CompressionKind(enum.IntEnum):
    """ The compression kind for the ORC file. """

    NONE = 0
    ZLIB = 1
    SNAPPY = 2
    LZO = 3
    LZ4 = 4
    ZSTD = 5


class CompressionStrategy(enum.IntEnum):
    """ Compression strategy for the ORC file. """

    SPEED = 0
    COMPRESSION = 1


class TypeKind(enum.IntEnum):
    """ The type kinds for an ORC schema. """

    BOOLEAN = 0
    BYTE = 1
    SHORT = 2
    INT = 3
    LONG = 4
    FLOAT = 5
    DOUBLE = 6
    STRING = 7
    BINARY = 8
    TIMESTAMP = 9
    LIST = 10
    MAP = 11
    STRUCT = 12
    UNION = 13
    DECIMAL = 14
    DATE = 15
    VARCHAR = 16
    CHAR = 17


class StructRepr(enum.IntEnum):
    """ Enumeration for ORC struct representation. """

    TUPLE = 0  #: For tuple.
    DICT = 1  #: For dictionary.

