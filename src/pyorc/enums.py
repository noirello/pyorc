import enum

class CompressionKind(enum.IntEnum):
    NONE = 0
    ZLIB = 1
    SNAPPY = 2
    LZO = 3
    LZ4 = 4
    ZSTD = 5

class CompressionStrategy(enum.IntEnum):
    SPEED = 0
    COMPRESSION = 1