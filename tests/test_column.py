import pytest

import io
import math

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from pyorc import (
    Reader,
    Writer,
    TypeKind,
    StructRepr,
    ParseError,
    Column,
    Stripe,
)


@pytest.fixture
def striped_orc_data():
    def _init(schema, rows, bfc=tuple()):
        data = io.BytesIO()
        with Writer(
            data,
            schema,
            batch_size=65535,
            stripe_size=128,
            compression_block_size=128,
            bloom_filter_columns=bfc,
        ) as writer:
            writer.writerows(rows)
        data.seek(0)
        return data

    return _init


def test_init(striped_orc_data):
    data = striped_orc_data("struct<a:int,b:int>", ((i, i * 5) for i in range(100000)))
    reader = Reader(data, column_indices=(1,))
    stripe = Stripe(reader, 0)
    with pytest.raises(TypeError):
        _ = Column(stripe, "0")
    with pytest.raises(IndexError):
        _ = Column(stripe, 100)
    with pytest.raises(IndexError):
        _ = Column(reader, 100)
    with pytest.raises(IndexError):
        _ = Column(reader, 1)
    col = Column(stripe, 0)
    assert col is not None
    col = Column(reader, 0)
    assert col is not None


def test_getitem(striped_orc_data):
    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    col = reader[0]
    assert col is not None
    col = stripe[0]
    assert col is not None


def test_statistics_bool(striped_orc_data):
    data = striped_orc_data(
        "struct<a:boolean>", (((True, False, None)[i % 3],) for i in range(100000))
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 65535
    assert stat["kind"] == TypeKind.STRUCT
    stat = stripe[1].statistics
    assert stat["has_null"] is True
    assert stat["kind"] == TypeKind.BOOLEAN
    assert stat["number_of_values"] == 43690
    assert stat["false_count"] == 21845
    assert stat["true_count"] == len([i for i, in stripe if i is True])
    stat = reader[1].statistics
    assert stat["has_null"] is True
    assert stat["number_of_values"] == 66667
    assert stat["false_count"] == len([i for i, in reader if i is False])
    assert stat["true_count"] == 33334
    assert reader[0].statistics["number_of_values"] == 100000


def test_statistics_int(striped_orc_data):
    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 65535
    assert stat["kind"] == TypeKind.INT
    assert stat["minimum"] == 0
    assert stat["maximum"] == 65534
    assert stat["sum"] == sum(i for i in range(len(stripe)))
    stat = reader[0].statistics
    assert stat["minimum"] == 0
    assert stat["maximum"] == 99999
    assert stat["sum"] == sum(i for i in range(100000))
    assert reader.read_stripe(1)[0].statistics["minimum"] == 65535


def test_statistics_double(striped_orc_data):
    data = striped_orc_data("double", (i * 0.1 for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 65535
    assert stat["kind"] == TypeKind.DOUBLE
    assert stat["minimum"] == 0
    assert math.isclose(stat["maximum"], 6553.4)
    assert stat["sum"] == sum(i * 0.1 for i in range(len(stripe)))
    stat = reader[0].statistics
    assert stat["minimum"] == 0
    assert math.isclose(stat["maximum"], 9999.9)
    assert stat["sum"] == sum(i * 0.1 for i in range(100000))
    assert reader.read_stripe(1)[0].statistics["minimum"] == 6553.5


def test_statistics_binary(striped_orc_data):
    data = striped_orc_data("binary", (b"\x4D\x45\x34\x01" for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["kind"] == TypeKind.BINARY
    assert stat["number_of_values"] == 65535
    assert stat["total_length"] == sum(len(i) for i in stripe)
    stat = reader[0].statistics
    assert stat["total_length"] == sum(len(i) for i in reader)


def test_statistics_string(striped_orc_data):
    data = striped_orc_data(
        "string", ("Test String {0}".format(i + 1) for i in range(100000))
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["kind"] == TypeKind.STRING
    assert stat["number_of_values"] == 65535
    assert stat["total_length"] == sum(len(i) for i in stripe)
    assert stat["minimum"] == "Test String 1"
    assert stat["maximum"] == max(i for i in Stripe(reader, 0))
    stat = reader[0].statistics
    assert stat["maximum"] == max(i for i in reader)
    assert reader.read_stripe(1)[0].statistics["minimum"] == "Test String 100000"


def test_statistics_date(striped_orc_data):
    data = striped_orc_data(
        "date", (date(1900, 1, 1) + timedelta(days=i) for i in range(100000))
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["kind"] == TypeKind.DATE
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 65535
    assert stat["minimum"] == date(1900, 1, 1)
    assert stat["maximum"] == date(2079, 6, 5)
    stat = reader[0].statistics
    assert stat["maximum"] == max(i for i in reader)


def test_statistics_timestamp(striped_orc_data):
    data = striped_orc_data(
        "timestamp",
        (
            datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc) + timedelta(minutes=i)
            for i in range(100000)
        ),
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["kind"] == TypeKind.TIMESTAMP
    assert stat["has_null"] is False
    assert stat["number_of_values"] == len(stripe)
    assert stat["minimum"] == datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert stat["maximum"] == max(i for i in stripe)
    assert stat["lower_bound"] == datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert stat["upper_bound"] == datetime(
        2000, 2, 16, 0, 14, 0, 1000, tzinfo=timezone.utc
    )
    stat = reader[0].statistics
    assert stat["maximum"] == max(i for i in reader)
    assert stat["upper_bound"] == datetime(
        2000, 3, 10, 22, 39, 0, 1000, tzinfo=timezone.utc
    )


def test_statistics_decimal(striped_orc_data):
    data = striped_orc_data(
        "decimal(10,3)",
        (Decimal("1000.1") + Decimal((i + 100) * 0.1) for i in range(100000)),
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["kind"] == TypeKind.DECIMAL
    assert stat["has_null"] is False
    assert stat["number_of_values"] == len(stripe)
    assert stat["minimum"] == Decimal("1010.100")
    assert stat["maximum"] == Decimal("7563.500")
    assert stat["sum"] == sum(
        Decimal("1000.1") + Decimal((i + 100) * 0.1) for i in range(len(stripe))
    ).quantize(Decimal("1.000"))
    stat = reader[0].statistics
    assert stat["sum"] == sum(
        Decimal("1000.1") + Decimal((i + 100) * 0.1) for i in range(100000)
    ).quantize(Decimal("1.000"))
    assert reader.read_stripe(1)[0].statistics["minimum"] == Decimal("7563.600")


def test_statistics_array_int(striped_orc_data):
    data = striped_orc_data(
        "struct<list:array<int>>",
        (([j + i for j in range(30)],) for i in range(100000)),
    )
    reader = Reader(data)
    stripe = reader.read_stripe(0)
    stat = stripe[2].statistics
    assert stripe[1].statistics["kind"] == TypeKind.LIST
    assert stat["kind"] == TypeKind.INT
    assert sum(i for col in reader.read_stripe(0) for i in col[0]) == stat["sum"]
    assert min(i for col in reader.read_stripe(0) for i in col[0]) == stat["minimum"]
    assert max(i for col in reader.read_stripe(0) for i in col[0]) == stat["maximum"]
    stat = reader[2].statistics
    assert max(i for col in reader for i in col[0]) == stat["maximum"]
