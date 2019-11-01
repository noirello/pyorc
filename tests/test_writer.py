import pytest

import io
import tempfile
from decimal import Decimal

from pyorc import Writer, Reader, typedescription, ParseError


def test_open_file():
    with tempfile.NamedTemporaryFile(mode="wt") as fp:
        with pytest.raises(ParseError):
            _ = Writer(fp, "int")
        with open(fp.name, "rb") as fp2:
            with pytest.raises(io.UnsupportedOperation):
                _ = Writer(fp2, "int")
    with tempfile.NamedTemporaryFile(mode="wb") as fp:
        writer = Writer(fp, "int")
        assert isinstance(writer, Writer)
    with pytest.raises(TypeError):
        _ = Writer(0, "int")


def test_init():
    data = io.BytesIO()
    with pytest.raises(TypeError):
        _ = Writer(data, 0)
    with pytest.raises(TypeError):
        _ = Writer(data, "int", batch_size=-1)
    with pytest.raises(TypeError):
        _ = Writer(data, "int", batch_size="fail")
    with pytest.raises(TypeError):
        _ = Writer(data, "int", batch_size=1000, stripe_size=-1)
    with pytest.raises(TypeError):
        _ = Writer(data, "int", batch_size=1000, stripe_size="fail")
    with pytest.raises(ValueError):
        _ = Writer(data, "int", batch_size=1000, stripe_size=5000, compression=-1)
    with pytest.raises(ValueError):
        _ = Writer(data, "int", batch_size=1000, stripe_size=5000, compression="wrong")
    with pytest.raises(ValueError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=-1,
        )
    with pytest.raises(ValueError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy="fail",
        )
    with pytest.raises(TypeError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=0,
            compression_block_size=-1,
        )
    with pytest.raises(ValueError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=0,
            compression_block_size=1,
            bloom_filter_columns=["0", 1, 3.4],
        )
    with pytest.raises(KeyError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=0,
            compression_block_size=1,
            bloom_filter_columns=["0"],
        )
    with pytest.raises(TypeError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=0,
            compression_block_size=1,
            bloom_filter_columns=[0],
            bloom_filter_fpp="wrong",
        )
    with pytest.raises(ValueError):
        _ = Writer(
            data,
            "int",
            batch_size=1000,
            stripe_size=5000,
            compression=0,
            compression_strategy=0,
            compression_block_size=1,
            bloom_filter_columns=[0],
            bloom_filter_fpp=2.0,
        )
    writer = Writer(
        data,
        "int",
        batch_size=1000,
        stripe_size=5000,
        compression=0,
        compression_strategy=0,
        compression_block_size=1,
        bloom_filter_columns=[0],
        bloom_filter_fpp=0.5,
    )
    assert isinstance(writer, Writer)


def test_write():
    data = io.BytesIO()
    writer = Writer(data, "struct<col0:int,col1:string,col2:double>")
    records = [
        {"col0": 1, "col1": "Test A", "col2": 2.13},
        {"col0": 2, "col1": "Test B", "col2": 0.123213},
        {"col0": 3, "col1": "Test C", "col2": 123.011234},
    ]
    for rec in records:
        writer.write(rec)
    writer.close()
    data.seek(0)
    reader = Reader(data)
    assert reader.read() == records

TESTDATA = [
    ("string", 0),
    ("string", b"\x10\x13"),
    ("int", "str example"),
    ("bigint", 3.14),
    ("binary", "str example"),
    ("binary", 12),
    ("float", "str example"),
    ("double", b"\x42\x32"),
    ("boolean", "str example"),
    ("timestamp", "str example"),
    ("timestamp", 102112),
    ("date", "str example"),
    ("date", 123),
    ("decimal(10,5)", "str example"),
    ("decimal(36,8)", 1024),
]


@pytest.mark.parametrize("orc_type,value", TESTDATA)
def test_write_wrong_primitive_type(orc_type, value):
    data = io.BytesIO()
    writer = Writer(data, orc_type)
    with pytest.raises(TypeError):
        writer.write(value)


def test_context_manager():
    data = io.BytesIO()
    records = [
            {"col0": 1, "col1": "Test A", "col2": 2.13},
            {"col0": 2, "col1": "Test B", "col2": 0.123213},
            {"col0": 3, "col1": "Test C", "col2": 123.011234},
        ]
    with Writer(data, "struct<col0:int,col1:string,col2:double>") as writer:
        for rec in records:
            writer.write(rec)
    data.seek(0)
    reader = Reader(data)
    assert reader.read() == records

def test_current_row():
    data = io.BytesIO()
    writer = Writer(data, "struct<col0:int,col1:string,col2:double>")
    assert writer.current_row == 0
    writer.write({"col0": 0, "col1": "Test A", "col2": 0.0001})
    assert writer.current_row == 1
    for i in range(10):
        writer.write({"col0": i, "col1": "Test A", "col2": 0.0001})
    assert writer.current_row == 11
    writer.close()
    data.seek(0)
    reader = Reader(data)
    assert writer.current_row == len(reader)