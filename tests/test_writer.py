import pytest

import gzip
import json
import os
import tempfile
import subprocess

from datetime import date, datetime, timezone

import _pyorc


@pytest.fixture
def output_file():
    testfile = tempfile.NamedTemporaryFile(
        mode="wb", delete=False, prefix="pyorc_", suffix=".orc"
    )
    yield testfile
    os.remove(testfile.name)


def find_type(schema, col_name):
    start_pos = schema.find("{0}:".format(col_name))
    end_pos = schema.find(",", start_pos)
    if "<" in schema[start_pos:end_pos]:
        op = 1
        cl = 0
        for i, ch in enumerate(schema[start_pos:]):
            if ch == "<":
                op += 1
            if ch == ">":
                cl += 1
            if cl == op:
                return schema[start_pos : start_pos + i].split(":", 1)[-1]
    else:
        return schema[start_pos:end_pos].split(":")[-1]


def transform_json(schema, row):
    if isinstance(row, dict):
        transformed = {}
        for col, value in row.items():
            orc_type = find_type(schema, col)
            if isinstance(value, dict) and orc_type.startswith("struct<"):
                transformed[col] = transform_json("{0}:{1}".format(col, schema), value)
            elif isinstance(value, list) and orc_type.startswith("map<"):
                transformed[col] = {
                    keypair["key"]: transform_json(
                        "{0}:{1}".format(col, schema), keypair["value"]
                    )
                    for keypair in value
                }
            elif isinstance(value, list) and orc_type.startswith("array<"):
                transformed[col] = [
                    transform_json("{0}:{1}".format(col, schema), item)
                    for item in value
                ]
            elif isinstance(value, str) and orc_type == "timestamp":
                try:
                    ts = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    ts = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                transformed[col] = ts.replace(tzinfo=timezone.utc)
            elif isinstance(value, str) and orc_type == "date":
                transformed[col] = datetime.strptime(value, "%Y-%m-%d").date()
            elif isinstance(value, list) and orc_type == "binary":
                transformed[col] = bytes(value)
            else:
                transformed[col] = value
        return transformed
    else:
        return row


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
]


@pytest.mark.parametrize("expected,schema", TESTDATA, ids=idfn)
def test_examples(expected, schema, output_file):
    writer = _pyorc.writer(output_file, schema)
    num = 0
    for row in read_expected_json_record(get_full_path(expected)):
        orc_row = transform_json(schema, row)
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
    writer = _pyorc.writer(output_file, "timestamp")
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
