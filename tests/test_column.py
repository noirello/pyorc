import pytest

import io
import math

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from pyorc import (
    Reader,
    Writer,
    typedescription,
    TypeKind,
    StructRepr,
    ParseError,
    column,
    stripe as Stripe,
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
    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    with pytest.raises(TypeError):
        _ = column(stripe, "0")
    with pytest.raises(IndexError):
        _ = column(stripe, 100)
    col = column(stripe, 0)
    assert col is not None


def test_statistics(striped_orc_data):
    data = striped_orc_data(
        "struct<a:int,b:boolean,c:double,d:binary,e:string,f:date,g:timestamp,h:decimal(10,3)>",
        (
            (
                i,
                (True, False, None)[i % 3],
                i * 0.1,
                b"\x4D\x45\x34\x01",
                "Test String {0}".format(i + 1),
                date(1900, 1, 1) + timedelta(days=i),
                datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                Decimal("1000.1") + Decimal((i + 100) * 0.1),
            )
            for i in range(10000)
        ),
    )
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    stat = stripe[0].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert set(stat.keys()) == {"has_null", "number_of_values"}
    stat = stripe[1].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["minimum"] == 0
    assert stat["maximum"] == 9999
    assert stat["sum"] == sum(i for i in range(10000))
    stat = stripe[2].statistics
    assert stat["has_null"] is True
    assert stat["number_of_values"] == 6667
    assert stat["false_count"] == 3333
    assert stat["true_count"] == 3334
    stat = stripe[3].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["minimum"] == 0
    assert math.isclose(stat["maximum"], 999.9)
    assert stat["sum"] == sum(i * 0.1 for i in range(10000))
    stat = stripe[4].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["total_length"] == 40000
    stat = stripe[5].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["total_length"] == 158894
    assert stat["minimum"] == "Test String 1"
    assert stat["maximum"] == "Test String 9999"
    stat = stripe[6].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["minimum"] == date(1900, 1, 1)
    assert stat["maximum"] == date(1927, 5, 19)
    stat = stripe[7].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["minimum"] == datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert stat["maximum"] == datetime(2000, 1, 8, 10, 39, tzinfo=timezone.utc)
    assert stat["lower_bound"] == datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert stat["upper_bound"] == datetime(
        2000, 1, 8, 10, 39, 0, 1000, tzinfo=timezone.utc
    )
    stat = stripe[8].statistics
    assert stat["has_null"] is False
    assert stat["number_of_values"] == 10000
    assert stat["minimum"] == Decimal("1010.100")
    assert stat["maximum"] == Decimal("2010.000")
    assert stat["sum"] == sum(
        Decimal("1000.1") + Decimal((i + 100) * 0.1) for i in range(10000)
    ).quantize(Decimal("1.000"))


def test_contains(striped_orc_data):
    data = striped_orc_data(
        "struct<a:int,b:array<int>>", ((i, range(10)) for i in range(10000))
    )
    reader = Reader(data, batch_size=10)
    stripe = reader.read_stripe(0)
    col = stripe[1]
    assert 100 in col
    _ = [next(col) for _ in range(10)]
    assert 1 in col
    col = stripe[3]
    assert next(col) == 0
    assert next(col) == 1
    assert 9 in col
    assert next(col) == 2
    assert (10 in col) is False
