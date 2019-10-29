import pytest

import io

from pyorc import Reader, Writer, typedescription, TypeKind


def test_next():
    data = io.BytesIO()
    Writer(data, "struct<col0:int,col1:string>").close()
    with pytest.raises(StopIteration):
        reader = Reader(data)
        next(reader)
    expected = {"col0": 0, "col1": "Test A"}
    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        writer.write(expected)
    reader = Reader(data)
    assert next(reader) == expected
    with pytest.raises(StopIteration):
        next(reader)


def test_iter():
    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        for i in range(20):
            writer.write({"col0": i, "col1": "Test"})
    reader = Reader(data)
    result = [row for row in reader]
    assert len(result) == 20
    assert {"col0": 0, "col1": "Test"} == result[0]
    assert {"col0": 19, "col1": "Test"} == result[-1]
    assert {"col0": 12, "col1": "Test"} in result


def test_len():
    data = io.BytesIO()
    Writer(data, "struct<col0:int,col1:string>").close()
    reader = Reader(data)
    assert len(reader) == 0

    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        writer.write({"col0": 0, "col1": "Test A"})
    reader = Reader(data)
    assert len(reader) == 1

    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        for i in range(10):
            writer.write({"col0": i, "col1": "Test"})
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
    assert isinstance(schema, typedescription)
    assert schema.kind == TypeKind.STRUCT


def test_current_row():
    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        for i in range(20):
            writer.write({"col0": i, "col1": "Test"})
    data.seek(0)

    reader = Reader(data)
    assert reader.current_row == 0
    for _ in range(10):
        _ = next(reader)
    assert reader.current_row == 10
    res = next(reader)
    assert reader.current_row == 11
    assert res["col0"] == 10
    _ = [_ for _ in reader]
    assert reader.current_row == len(reader)

    with pytest.raises(AttributeError):
        reader.current_row = "fail"
    with pytest.raises(AttributeError):
        del reader.current_row


def test_seek():
    data = io.BytesIO()
    with Writer(data, "struct<col0:int,col1:string>") as writer:
        for i in range(50):
            writer.write({"col0": i, "col1": "Test"})
    data.seek(0)
    reader = Reader(data)
    assert reader.seek(0) == 0
    assert reader.current_row == 0
    assert reader.seek(10) == 10
    assert reader.current_row == 10
    assert next(reader)["col0"] == 10
    assert reader.seek(0, 2) == len(reader)
    with pytest.raises(StopIteration):
        _ = next(reader)["col0"]
    assert reader.seek(-1, 2) == 49
    assert next(reader)["col0"] == 49
    assert reader.seek(-10, 2) == 40
    assert reader.seek(1, 1) == 41
    assert next(reader)["col0"] == 41
    reader.seek(10)
    assert reader.seek(8, 1) == 18
    assert reader.seek(-5, 1) == 13
    assert next(reader)["col0"] == 13
    with pytest.raises(ValueError):
        reader.seek(-1, 0)
    with pytest.raises(ValueError):
        reader.seek(10, 10)
