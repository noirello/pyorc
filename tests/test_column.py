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
    assert stat["kind"] == TypeKind.STRUCT
    assert set(stat.keys()) == {"has_null", "number_of_values", "kind"}
    stat = stripe[1].statistics
    assert stat["kind"] == TypeKind.INT
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


def test_uniontype(striped_orc_data):
    data = striped_orc_data(
        "uniontype<int,string>", ((i, "a")[i % 2] for i in range(1000))
    )
    reader = Reader(data)
    stripe = reader.read_stripe(0)
    nums = list(stripe[1])
    strs = list(stripe[2])
    assert len(nums) == 500
    assert len(strs) == 500
    assert set(strs) == {"a"}
    assert all(isinstance(n, int) for n in nums)
    assert stripe[0].statistics["number_of_values"] == 1000


def test_map():
    data = io.BytesIO()
    with Writer(data, "map<string,int>") as writer:
        writer.write({"a": 0, "b": 1, "c": 2})
        writer.write({"d": 3, "c": 4})
    reader = Reader(data)
    stripe = reader.read_stripe(0)
    keys = stripe[1]
    vals = stripe[2]
    assert len(list(stripe[1])) == 5
    assert len(list(stripe[2])) == 5
    assert set(keys) == {"a", "b", "c", "d"}
    assert set(vals) == {0, 1, 2, 3, 4}


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


def test_bloom_filter_int():
    data = io.BytesIO()
    with Writer(data, "int", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write(num)
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert 999 in col
    assert (5000 in col) is False


def test_bloom_filter_double():
    data = io.BytesIO()
    with Writer(data, "double", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write(num * 0.12)
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert (999 * 0.12) in col
    assert (999.12 in col) is False


def test_bloom_filter_string():
    data = io.BytesIO()
    with Writer(data, "string", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write("Test {0}".format(num + 1))
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert "Test 500" in col
    assert ("Test 2000" in col) is False


def test_bloom_filter_date():
    data = io.BytesIO()
    with Writer(data, "date", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write(date(2010, 9, 1) + timedelta(days=num))
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert date(2010, 10, 25) in col
    assert (date(2016, 12, 1) in col) is False


def test_bloom_filter_timestamp():
    data = io.BytesIO()
    with Writer(data, "timestamp", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write(
                datetime(2010, 9, 1, 8, tzinfo=timezone.utc) + timedelta(minutes=num)
            )
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert datetime(2010, 9, 1, 15, tzinfo=timezone.utc) in col
    assert (datetime(2011, 9, 1, 15, tzinfo=timezone.utc) in col) is False


def test_bloom_filter_decimal():
    data = io.BytesIO()
    with Writer(data, "decimal(8,3)", bloom_filter_columns=(0,)) as writer:
        for num in range(1000):
            writer.write(Decimal("1000.23") + Decimal("0.{0}".format(num)))
    reader = Reader(data)
    col = reader.read_stripe(0)[0]
    assert Decimal("1001.199") in col
    assert (Decimal("1001.235") in col) is False


def test_array_int_stat(striped_orc_data):
    data = striped_orc_data(
        "struct<list:array<int>>", (([j + i for j in range(10)],) for i in range(10000))
    )
    reader = Reader(data)
    stripe = reader.read_stripe(0)
    col = stripe[2]
    assert sum(stripe[2]) == col.statistics["sum"]
    assert min(stripe[2]) == col.statistics["minimum"]
    assert max(stripe[2]) == col.statistics["maximum"]
