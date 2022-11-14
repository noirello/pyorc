import pytest

import io
import sys

from datetime import datetime, timedelta, timezone

from pyorc import (
    Reader,
    Writer,
    Stripe,
    orc_version_info,
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
    with pytest.raises(TypeError):
        _ = Stripe(None, 0)
    with pytest.raises(TypeError):
        _ = Stripe("reader", 0)
    with pytest.raises(IndexError):
        _ = Stripe(reader, 3)
    with pytest.raises(TypeError):
        _ = Stripe(reader, "col")
    assert Stripe(reader, 0) is not None


def test_len(striped_orc_data):
    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)

    assert len(reader) != len(stripe)
    assert len(stripe) == 65535


def test_bytes_length(striped_orc_data):
    expected_bytes_length = (
        392 if orc_version_info.major == 1 and orc_version_info.minor < 8 else 359
    )  # Bold, hardcoded length values.

    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 1)

    assert stripe.bytes_length == expected_bytes_length
    with pytest.raises(AttributeError):
        stripe.bytes_length = "false"


def test_bytes_offset(striped_orc_data):
    expected_bytes_offset = (
        658 if orc_version_info.major == 1 and orc_version_info.minor < 8 else 614
    )  # Bold, hardcoded offset value.

    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 1)

    assert stripe.bytes_offset == expected_bytes_offset
    with pytest.raises(AttributeError):
        stripe.bytes_offset = 5


def test_bloom_filter_columns(striped_orc_data):
    expected = (0, 1)
    data = striped_orc_data(
        "struct<col0:int,col1:string>",
        ((i, "Test {}".format(i + 1)) for i in range(100000)),
        bfc=expected,
    )
    reader = Reader(data)
    assert Stripe(reader, 0).bloom_filter_columns == expected
    assert Stripe(reader, 1).bloom_filter_columns == expected

    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe = Stripe(reader, 0)
    assert stripe.bloom_filter_columns == tuple()
    with pytest.raises(AttributeError):
        stripe.bloom_filter_columns = (0,)


def test_row_offset(striped_orc_data):
    data = striped_orc_data("int", (i for i in range(100000)))
    reader = Reader(data)
    stripe0 = Stripe(reader, 0)

    assert stripe0.row_offset == 0
    assert Stripe(reader, 1).row_offset == len(stripe0)
    with pytest.raises(AttributeError):
        stripe0.row_offset = 5


def test_writer_timezone(striped_orc_data):
    def get_dt():
        start = datetime(2010, 9, 1, 7, 0, 0, 0, timezone.utc)
        end = datetime(2010, 9, 10, 12, 0, 0, 0, timezone.utc)
        while start <= end:
            yield start
            start += timedelta(seconds=10)

    data = striped_orc_data("timestamp", get_dt())
    reader = Reader(data)
    stripe = Stripe(reader, 1)

    assert stripe.writer_timezone == "UTC"
    with pytest.raises(AttributeError):
        stripe.writer_timezone = "UTC-9:00"


@pytest.mark.skipif(sys.platform == "win32", reason="Seeking fails on Windows")
def test_seek_and_read(striped_orc_data):
    data = striped_orc_data(
        "struct<col0:int,col1:string>",
        ((i, "Test {}".format(i + 1)) for i in range(100000)),
    )
    reader = Reader(data)
    stripe = reader.read_stripe(1)
    assert next(stripe) == (65535, "Test 65536")
    stripe.seek(10000)
    assert next(stripe) == (75535, "Test 75536")
    stripe.seek(-1, 2)
    assert next(stripe) == (99999, "Test 100000")
    stripe = reader.read_stripe(0)
    stripe.seek(-1, 2)
    assert next(stripe) == (65534, "Test 65535")
    stripe.seek(0)
    next(stripe)
    stripe.seek(10000, 1)
    assert next(stripe) == (10001, "Test 10002")
    expected = reader.read()
    result = stripe.read()
    assert result == expected[10002:65535]
    stripe = reader.read_stripe(1)
    assert stripe.read() == expected[65535:]
