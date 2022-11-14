import pytest

import io
import math
import string
from datetime import datetime, date, timezone
from decimal import Decimal

import pyorc

from pyorc import (
    Reader,
    Writer,
    TypeDescription,
    TypeKind,
    StructRepr,
    ParseError,
    PredicateColumn,
    Stripe,
    CompressionKind,
    WriterVersion,
    orc_version,
)
from pyorc.converters import ORCConverter

from conftest import output_file, NullValue


@pytest.fixture
def orc_data():
    def _init(row):
        data = io.BytesIO()
        with Writer(
            data, "struct<col0:int,col1:string>", struct_repr=StructRepr.DICT
        ) as writer:
            for i in range(row):
                writer.write(
                    {
                        "col0": i,
                        "col1": "Test {0}".format(string.ascii_uppercase[i % 26]),
                    }
                )
        data.seek(0)
        return data

    return _init


@pytest.fixture
def striped_orc_data():
    def _init(row):
        data = io.BytesIO()
        with Writer(
            data,
            "struct<col0:int>",
            batch_size=65535,
            stripe_size=128,
            compression_block_size=128,
        ) as writer:
            for i in range(row):
                writer.write((i,))
        data.seek(0)
        return data

    return _init


def test_init(orc_data):
    with pytest.raises(TypeError):
        _ = Reader(0)
    with pytest.raises(TypeError):
        _ = Reader(orc_data(1), "fail")
    reader = Reader(orc_data(2), 1)
    assert reader is not None


def test_open_file(output_file):
    output_file.close()
    with open(output_file.name, "wb") as fp:
        with pytest.raises(ParseError):
            _ = Reader(fp)
        # Write invalid bytes:
        fp.write(b"TESTTORC\x08\x03\x10\x03")
    with open(output_file.name, "rb") as fp:
        with pytest.raises(ParseError):
            _ = Reader(fp)
    with open(output_file.name, "wb") as fp:
        fp.write(b'ORC\x08\x03\x10\x03"k\x08\x0c\x12\x0c\x01\x02\x03')
    with open(output_file.name, "rt") as fp:
        with pytest.raises(ParseError):
            _ = Reader(fp)
    with open(output_file.name, "rb") as fp:
        with pytest.raises(ParseError):
            _ = Reader(fp)
    with open(output_file.name, "wb") as fp:
        Writer(fp, "struct<col0:int,col1:string>").close()
    with open(output_file.name, "ab") as fp:
        with pytest.raises(io.UnsupportedOperation):
            _ = Reader(fp)
    with open(output_file.name, "rb") as fp:
        reader = Reader(fp)
        assert reader is not None
        assert len(reader) == 0


def test_next():
    data = io.BytesIO()
    Writer(data, "struct<col0:int,col1:string>").close()
    with pytest.raises(StopIteration):
        reader = Reader(data)
        next(reader)
    expected = (0, "Test A")
    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        writer.write(expected)
    reader = Reader(data)
    assert next(reader) == expected
    with pytest.raises(StopIteration):
        next(reader)


def test_iter(orc_data):
    reader = Reader(orc_data(20))
    result = [row for row in reader]
    assert len(result) == 20
    assert (0, "Test A") == result[0]
    assert (19, "Test T") == result[-1]
    assert (12, "Test M") in result


def test_len():
    data = io.BytesIO()
    Writer(data, "struct<col0:int,col1:string>").close()
    reader = Reader(data)
    assert len(reader) == 0

    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        writer.write((0, "Test A"))
    reader = Reader(data)
    assert len(reader) == 1

    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        for i in range(10):
            writer.write((i, "Test"))
    reader = Reader(data)
    assert len(reader) == 10


def test_schema():
    schema_str = "struct<col0:int,col1:string>"
    data = io.BytesIO()
    Writer(data, schema_str).close()
    reader = Reader(data)

    assert str(reader.schema) == schema_str
    with pytest.raises(AttributeError):
        reader.schema = "fail"
    with pytest.raises(AttributeError):
        del reader.schema

    schema = reader.schema
    del reader
    assert isinstance(schema, TypeDescription)
    assert schema.kind == TypeKind.STRUCT


def test_selected_schema():
    schema_str = "struct<col0:int,col1:string>"
    data = io.BytesIO()
    Writer(data, schema_str).close()
    reader = Reader(data, column_names=("col1",))

    assert str(reader.schema) == schema_str
    assert str(reader.selected_schema) != str(reader.schema)
    with pytest.raises(AttributeError):
        reader.selected_schema = "fail"
    with pytest.raises(AttributeError):
        del reader.selected_schema

    schema = reader.selected_schema
    del reader
    assert isinstance(schema, TypeDescription)
    assert schema.kind == TypeKind.STRUCT
    assert str(schema) == "struct<col1:string>"


def test_current_row(orc_data):
    reader = Reader(orc_data(20))
    assert reader.current_row == 0
    for _ in range(10):
        _ = next(reader)
    assert reader.current_row == 10
    res = next(reader)
    assert reader.current_row == 11
    assert res[0] == 10
    _ = [_ for _ in reader]
    assert reader.current_row == len(reader)

    with pytest.raises(AttributeError):
        reader.current_row = "fail"
    with pytest.raises(AttributeError):
        del reader.current_row


def test_seek(orc_data):
    reader = Reader(orc_data(50))
    assert reader.seek(0) == 0
    assert reader.current_row == 0
    assert reader.seek(10) == 10
    assert reader.current_row == 10
    assert next(reader)[0] == 10
    assert reader.seek(0, 2) == len(reader)
    with pytest.raises(StopIteration):
        _ = next(reader)[0]
    assert reader.seek(-1, 2) == 49
    assert next(reader)[0] == 49
    assert reader.seek(-10, 2) == 40
    assert reader.seek(1, 1) == 41
    assert next(reader)[0] == 41
    reader.seek(10)
    assert reader.seek(8, 1) == 18
    assert reader.seek(-5, 1) == 13
    assert next(reader)[0] == 13
    with pytest.raises(ValueError):
        reader.seek(-1, 0)
    with pytest.raises(ValueError):
        reader.seek(10, 10)


def test_read(orc_data):
    reader = Reader(orc_data(80))
    result = reader.read()
    assert len(result) == len(reader)
    assert (0, "Test A") == result[0]
    assert (25, "Test Z") == result[25]
    assert result[-1][0] == 79
    assert reader.current_row == 80
    with pytest.raises(StopIteration):
        _ = next(reader)
    result = reader.read()
    assert result == []

    reader = Reader(orc_data(80))
    with pytest.raises(ValueError):
        _ = reader.read(-5)
    with pytest.raises(TypeError):
        _ = reader.read("a")
    result = reader.read(10)
    assert len(result) == 10
    assert (0, "Test A") == result[0]
    assert (9, "Test J") == result[-1]

    result = reader.read(15)
    assert len(result) == 15
    assert (10, "Test K") == result[0]
    assert (24, "Test Y") == result[-1]
    assert reader.current_row == 25

    result = reader.read()
    assert len(result) == 55
    assert (25, "Test Z") == result[0]

    reader = Reader(orc_data(80))
    result = reader.read(0)
    assert result == []
    result = reader.read(-1)
    assert len(result) == len(reader)


def test_include():
    data = io.BytesIO()
    record = {"col0": 1, "col1": "Test A", "col2": 3.14}
    with Writer(
        data, "struct<col0:int,col1:string,col2:double>", struct_repr=StructRepr.DICT
    ) as writer:
        writer.write(record)
    data.seek(0)
    reader = Reader(data, column_indices=[0], struct_repr=StructRepr.DICT)
    assert next(reader) == {"col0": 1}
    reader = Reader(data, column_indices=[0, 2], struct_repr=StructRepr.DICT)
    assert next(reader) == {"col0": 1, "col2": 3.14}
    with pytest.raises(TypeError):
        _ = Reader(data, column_indices=[0, "2"], struct_repr=StructRepr.DICT)
    reader = Reader(data, column_names=["col0"], struct_repr=StructRepr.DICT)
    assert next(reader) == {"col0": 1}
    reader = Reader(data, column_names=["col1", "col2"], struct_repr=StructRepr.DICT)
    assert next(reader) == {"col1": "Test A", "col2": 3.14}
    with pytest.raises(TypeError):
        _ = Reader(data, column_names=["col1", 2], struct_repr=StructRepr.DICT)
    with pytest.raises(ValueError):
        _ = Reader(data, column_indices=[10], struct_repr=StructRepr.DICT)
    with pytest.raises(ValueError):
        _ = Reader(data, column_names=["col5"], struct_repr=StructRepr.DICT)
    with pytest.raises(ValueError):
        _ = Reader(
            data, column_names=["col1"], column_indices=[2], struct_repr=StructRepr.DICT
        )


def test_num_of_stripes(striped_orc_data):
    reader = Reader(striped_orc_data(655))
    assert reader.num_of_stripes == 1
    reader = Reader(striped_orc_data(655350))
    assert reader.num_of_stripes == 10


def test_read_stripe(striped_orc_data):
    reader = Reader(striped_orc_data(655350))
    stripe = reader.read_stripe(0)
    assert isinstance(stripe, Stripe)
    with pytest.raises(IndexError):
        _ = reader.read_stripe(11)
    with pytest.raises(TypeError):
        _ = reader.read_stripe(-1)
    with pytest.raises(IndexError):
        _ = reader.read_stripe(10)
    stripe = reader.read_stripe(9)
    assert isinstance(stripe, Stripe)


def test_iter_stripe(striped_orc_data):
    reader = Reader(striped_orc_data(655350))
    stripes = list(reader.iter_stripes())
    assert len(stripes) == reader.num_of_stripes
    assert all(isinstance(stripe, Stripe) for stripe in reader.iter_stripes())


class TestConverter(ORCConverter):
    @staticmethod
    def to_orc(*args):
        pass

    @staticmethod
    def from_orc(seconds, nanoseconds, timezone):
        return (seconds, nanoseconds)


def test_converter():
    data = io.BytesIO()
    seconds = 1500000
    nanoseconds = 101000
    exp_date = date(2000, 1, 1)
    record = {
        "col0": datetime.fromtimestamp(seconds, timezone.utc).replace(
            microsecond=nanoseconds // 1000
        ),
        "col1": exp_date,
    }
    with Writer(
        data, "struct<col0:timestamp,col1:date>", struct_repr=StructRepr.DICT
    ) as writer:
        writer.write(record)
    data.seek(0)
    reader = Reader(data, converters={TypeKind.TIMESTAMP: TestConverter})
    assert next(reader) == ((seconds, nanoseconds), exp_date)


@pytest.mark.parametrize(
    "kind", (CompressionKind.NONE, CompressionKind.ZLIB, CompressionKind.ZSTD)
)
def test_compression(kind):
    data = io.BytesIO()
    with Writer(data, "int", compression=kind) as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    with pytest.raises(AttributeError):
        reader.compression = "fail"
    with pytest.raises(AttributeError):
        del reader.compression
    assert reader.compression == kind


@pytest.mark.parametrize("block_size", (10000, 20000, 30000))
def test_compression_block_size(block_size):
    data = io.BytesIO()
    with Writer(data, "int", compression_block_size=block_size) as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    assert reader.compression_block_size == block_size


@pytest.mark.parametrize("stride", (10000, 20000, 30000))
def test_row_index_stride(stride):
    data = io.BytesIO()
    with Writer(data, "int", row_index_stride=stride) as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    assert reader.row_index_stride == stride


def test_writer_id():
    data = io.BytesIO()
    with Writer(data, "int") as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    with pytest.raises(AttributeError):
        reader.writer_id = "fail"
    with pytest.raises(AttributeError):
        del reader.writer_id
    assert reader.writer_id == "ORC_CPP_WRITER"


def test_writer_version():
    data = io.BytesIO()
    with Writer(data, "int") as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    assert reader.writer_version == WriterVersion.ORC_135


def test_bytes_lengths():
    data = io.BytesIO()
    Writer(data, "string", compression=0).close()
    reader = Reader(data)
    assert reader.bytes_lengths["content_length"] == 0
    assert reader.bytes_lengths["file_footer_length"] == 38
    assert reader.bytes_lengths["file_postscript_length"] == 23
    assert reader.bytes_lengths["file_length"] == 65
    assert reader.bytes_lengths["stripe_statistics_length"] == 0

    expected_content_length = (
        76
        if pyorc.orc_version_info.major == 1 and pyorc.orc_version_info.minor < 8
        else 63
    )

    data = io.BytesIO()
    with Writer(data, "int") as writer:
        writer.writerows(range(100))
    reader = Reader(data)
    assert reader.bytes_lengths["content_length"] == expected_content_length
    assert reader.bytes_lengths["file_footer_length"] == 59
    assert reader.bytes_lengths["file_postscript_length"] == 23
    assert reader.bytes_lengths["file_length"] == len(data.getvalue())
    assert reader.bytes_lengths["stripe_statistics_length"] == 21


def test_software_version():
    data = io.BytesIO()
    with Writer(data, "int") as writer:
        writer.writerows(range(10))
    reader = Reader(data)
    assert reader.software_version == f"ORC C++ {orc_version}"


def test_wrong_predicate():
    data = io.BytesIO()
    with Writer(data, "struct<c0:int,c1:string>", row_index_stride=100) as writer:
        writer.writerows((i, "Even") if i % 2 == 0 else (i, "Odd") for i in range(1000))
    data.seek(0)
    with pytest.raises(TypeError):
        reader = Reader(data, predicate="wrong")


def test_empty_predicate_result():
    data = io.BytesIO()
    with Writer(data, "struct<c0:int,c1:string>", row_index_stride=100) as writer:
        writer.writerows((i, "Even") if i % 2 == 0 else (i, "Odd") for i in range(1000))
    data.seek(0)
    reader = Reader(data, predicate=PredicateColumn(TypeKind.INT, "c0") < 0)
    assert len(reader) != 0
    assert list(reader) == []


def test_simple_predicate_results():
    data = io.BytesIO()
    with Writer(data, "struct<c0:int,c1:string>", row_index_stride=100) as writer:
        writer.writerows((i, "Even") if i % 2 == 0 else (i, "Odd") for i in range(1000))
    data.seek(0)
    for args in ({"name": "c0"}, {"index": 1}):
        reader = Reader(data, predicate=PredicateColumn(TypeKind.INT, **args) < 100)
        result = list(reader)
        assert len(result) == 100
        assert result[99] == (99, "Odd")
    for args in ({"name": "c0"}, {"index": 1}):
        reader = Reader(data, predicate=PredicateColumn(TypeKind.INT, **args) <= 100)
        result = list(reader)
        assert len(result) == 200
    for args in ({"name": "c1"}, {"index": 2}):
        reader = Reader(
            data, predicate=PredicateColumn(TypeKind.STRING, **args) == "Even"
        )
        result = list(reader)
        assert len(result) == len(reader)


def test_complex_predicate_results():
    data = io.BytesIO()
    with Writer(data, "struct<c0:int,c1:string>", row_index_stride=100) as writer:
        writer.writerows(
            (i, "A") if i > 300 and i <= 450 else (i, "B") for i in range(1000)
        )
    data.seek(0)
    reader = Reader(
        data,
        predicate=(PredicateColumn(TypeKind.INT, "c0") < 100)
        & (PredicateColumn(TypeKind.STRING, "c1") == "A"),
    )
    assert list(reader) == []
    reader = Reader(
        data,
        predicate=(PredicateColumn(TypeKind.INT, "c0") > 300)
        & (PredicateColumn(TypeKind.STRING, "c1") == "A"),
    )
    result = list(reader)
    assert len(result) == 200
    assert sum(1 if row[1] == "A" else 0 for row in result) == 150
    reader = Reader(
        data,
        predicate=(PredicateColumn(TypeKind.INT, "c0") >= 400)
        & (PredicateColumn(TypeKind.STRING, "c1") != "A"),
    )
    result = list(reader)
    assert len(result) == 600
    reader = Reader(
        data,
        predicate=(PredicateColumn(TypeKind.INT, "c0") < 100)
        | (PredicateColumn(TypeKind.STRING, index=2) != "B"),
    )
    result = list(reader)
    assert len(result) == 300


def test_converting_predicate_error():
    data = io.BytesIO()
    with Writer(data, f"struct<c0:string>", row_index_stride=1) as writer:
        writer.write(("test",))
    with pytest.raises(TypeError):
        _ = Reader(
            data,
            predicate=(PredicateColumn(TypeKind.STRING) < "test"),
        )
    with pytest.raises(TypeError):
        _ = Reader(
            data,
            predicate=(PredicateColumn(TypeKind.STRING) == "test"),
        )
    with pytest.raises(TypeError):
        _ = Reader(
            data,
            predicate=(PredicateColumn(TypeKind.STRING) <= "test"),
        )


@pytest.mark.parametrize(
    "orc_type,value",
    [
        (pyorc.typedescription.Boolean(), True),
        (pyorc.typedescription.Int(), 42),
        (pyorc.typedescription.BigInt(), 42000001),
        (pyorc.typedescription.Double(), 3.14),
        (pyorc.typedescription.VarChar(5), "AAAAA"),
        (pyorc.typedescription.Date(), date(2021, 10, 12)),
        (
            pyorc.typedescription.Timestamp(),
            datetime(2021, 10, 12, 20, 0, 0, tzinfo=timezone.utc),
        ),
        (pyorc.typedescription.Decimal(10, 3), Decimal("1001.1")),
    ],
)
def test_converting_predicate(orc_type, value):
    if orc_type.kind == TypeKind.DECIMAL:
        pred_col = PredicateColumn(
            orc_type.kind, "c0", precision=orc_type.precision, scale=orc_type.scale
        )
    else:
        pred_col = PredicateColumn(orc_type.kind, "c0")
    data = io.BytesIO()
    with Writer(data, f"struct<c0:{orc_type}>", row_index_stride=1) as writer:
        writer.write((value,))
        writer.write((None,))
    reader = Reader(data, predicate=(pred_col == value))
    result = list(reader)
    assert result == [(value,)]
    reader = Reader(data, predicate=(pred_col == None))
    result = list(reader)
    assert result == [(None,)]


TESTDATA = [
    ("int", 42),
    ("bigint", 560000000000001),
    ("float", 3.14),
    ("double", math.e),
    ("string", "test"),
    ("binary", b"\x23\x45\x45"),
    ("varchar(4)", "four"),
    ("timestamp", datetime(2019, 11, 10, 12, 59, 59, 100, tzinfo=timezone.utc)),
    ("date", date(2010, 9, 1)),
    ("decimal(10,0)", Decimal("1000000000")),
    ("array<int>", [0, 1, 2, 3]),
    ("map<string,string>", {"test": "example"}),
    ("struct<col0:int,col1:string>", (0, "test")),
]


@pytest.mark.parametrize("orc_type,value", TESTDATA)
def test_read_custom_null_value(orc_type, value):
    data = io.BytesIO()
    with Writer(data, orc_type) as writer:
        writer.write(value)
        writer.write(None)
    reader = Reader(data, null_value=NullValue())
    if orc_type in ("float", "double"):
        assert math.isclose(next(reader), value, rel_tol=1e-07, abs_tol=0.0)
    else:
        assert next(reader) == value
    assert next(reader) is NullValue()
