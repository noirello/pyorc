import pytest

import gzip
import json
import os
import tempfile
import subprocess

from decimal import Decimal
from datetime import date, datetime, timezone

import pyorc
from pyorc.enums import TypeKind


@pytest.fixture
def output_file():
    testfile = tempfile.NamedTemporaryFile(
        mode="wb", delete=False, prefix="pyorc_", suffix=".orc"
    )
    yield testfile
    os.remove(testfile.name)


def transform(schema, value):
    if schema.kind < 8:
        # Primitive types, no transformation.
        return value
    elif schema.kind == TypeKind.STRUCT:
        return {
            col: transform(schema.fields[col], field) for col, field in value.items()
        }
    elif schema.kind == TypeKind.MAP:
        return {
            keypair["key"]: transform(schema.container_types[1], keypair["value"])
            for keypair in value
        }
    elif schema.kind == TypeKind.LIST:
        return [transform(schema.container_types[0], item) for item in value]
    elif schema.kind == TypeKind.TIMESTAMP:
        try:
            ts = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            ts = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return ts.replace(tzinfo=timezone.utc)
    elif schema.kind == TypeKind.DATE:
        return datetime.strptime(value, "%Y-%m-%d").date()
    elif schema.kind == TypeKind.BINARY:
        return bytes(value)
    elif schema.kind == TypeKind.DECIMAL:
        if value is None:
            return value
        elif isinstance(value, float):
            return Decimal.from_float(value)
        else:
            return Decimal(value)
    else:
        return value


def read_expected_json_record(path):
    with gzip.open(path, "rb") as fileo:
        for line in fileo:
            yield json.loads(line)


def get_full_path(path):
    curdir = os.path.abspath(os.path.dirname(__file__))
    projdir = os.path.abspath(os.path.join(curdir, os.pardir))
    return os.path.join(projdir, "deps", "examples", "expected", path)


def idfn(val):
    return val[:40]


TESTDATA = [
    (
        "TestOrcFile.test1.jsn.gz",
        "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,middle:struct<list:array<struct<int1:int,string1:string>>>,list:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>>",
    ),
    ("TestOrcFile.testDate1900.jsn.gz", "struct<time:timestamp,date:date>"),
    ("TestOrcFile.testDate2038.jsn.gz", "struct<time:timestamp,date:date>"),
    (
        "TestOrcFile.testSeek.jsn.gz",
        "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,middle:struct<list:array<struct<int1:int,string1:string>>>,list:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>>",
    ),
    ("TestOrcFile.testSnappy.jsn.gz", "struct<int1:int,string1:string>"),
    (
        "nulls-at-end-snappy.jsn.gz",
        "struct<_col0:tinyint,_col1:smallint,_col2:int,_col3:bigint,_col4:float,_col5:double,_col6:boolean>",
    ),
    (
        "demo-12-zlib.jsn.gz",
        "struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>",
    ),
    ("decimal.jsn.gz", "struct<_col0:decimal(10,5)>"),
]


@pytest.mark.parametrize("expected,schema", TESTDATA, ids=idfn)
def test_examples(expected, schema, output_file):
    schema = pyorc.typedescription(schema)
    writer = pyorc.writer(output_file, schema)
    num = 0
    for row in read_expected_json_record(get_full_path(expected)):
        orc_row = transform(schema, row)
        writer.write(orc_row)
        num += 1
    assert num == writer.current_row
    writer.close()
    exp_res = read_expected_json_record(get_full_path(expected))
    with subprocess.Popen(
        ["deps/tools/orc-contents", output_file.name], stdout=subprocess.PIPE
    ) as proc:
        for line in proc.stdout:
            assert json.loads(line) == next(exp_res)
    with pytest.raises(StopIteration):
        next(exp_res)


def test_example_timestamp(output_file):
    writer = pyorc.writer(output_file, pyorc.typedescription("timestamp"))
    num = 0
    for row in read_expected_json_record(
        get_full_path("TestOrcFile.testTimestamp.jsn.gz")
    ):
        ts = datetime.strptime(row[:26], "%Y-%m-%d %H:%M:%S.%f")
        orc_row = ts.replace(tzinfo=timezone.utc)
        writer.write(orc_row)
        num += 1
    assert num == writer.current_row
    writer.close()
    exp_res = read_expected_json_record(
        get_full_path("TestOrcFile.testTimestamp.jsn.gz")
    )
    with subprocess.Popen(
        ["deps/tools/orc-contents", output_file.name], stdout=subprocess.PIPE
    ) as proc:
        for line in proc.stdout:
            assert datetime.strptime(
                json.loads(line)[:26], "%Y-%m-%d %H:%M:%S.%f"
            ) == datetime.strptime(next(exp_res)[:26], "%Y-%m-%d %H:%M:%S.%f")
    with pytest.raises(StopIteration):
        next(exp_res)