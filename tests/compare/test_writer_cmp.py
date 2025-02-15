import pytest

import gzip
import json
import os
import subprocess
import sys

from decimal import Decimal
from datetime import datetime, timezone

import pyorc._pyorc
from pyorc.enums import TypeKind, StructRepr
from pyorc.typedescription import TypeDescription, Timestamp

from conftest import output_file

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="No orc-tools on Windows"
)

ORC_CONTENTS_PATH = "deps/bin/orc-contents"


def transform(schema, value):
    if schema.kind < 8:
        # Primitive types, no transformation.
        return value
    elif schema.kind == TypeKind.STRUCT:
        return {col: transform(schema[col], field) for col, field in value.items()}
    elif schema.kind == TypeKind.MAP:
        return {
            keypair["key"]: transform(schema.value, keypair["value"])
            for keypair in value
        }
    elif schema.kind == TypeKind.LIST:
        return [transform(schema.type, item) for item in value]
    elif schema.kind == TypeKind.TIMESTAMP:
        if value is None:
            return value
        try:
            ts = datetime.strptime(value[:26], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            ts = datetime.strptime(value[:26], "%Y-%m-%d %H:%M:%S.%f")
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
    projdir = os.path.abspath(os.path.join(curdir, os.pardir, os.pardir))
    return os.path.join(projdir, "deps", "examples", "expected", path)


def idfn(val):
    return val[:40]


def create_orc_output_for_test(schema, file_out, file_in):
    writer = pyorc._pyorc.writer(file_out, schema, struct_repr=StructRepr.DICT)
    num = 0
    for row in read_expected_json_record(get_full_path(file_in)):
        orc_row = transform(schema, row)
        writer.write(orc_row)
        num += 1
    assert num == writer.current_row
    writer.close()


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
def test_write(expected, schema, output_file):
    create_orc_output_for_test(
        TypeDescription.from_string(schema), output_file, expected
    )
    exp_res = read_expected_json_record(get_full_path(expected))
    with subprocess.Popen(
        [ORC_CONTENTS_PATH, output_file.name], stdout=subprocess.PIPE
    ) as proc:
        for line in proc.stdout:
            assert json.loads(line) == next(exp_res)
    with pytest.raises(StopIteration):
        next(exp_res)


def test_write_decimal(output_file):
    input_filename = "decimal.jsn.gz"
    create_orc_output_for_test(
        TypeDescription.from_string("struct<_col0:decimal(10,5)>"),
        output_file,
        input_filename,
    )
    exp_res = read_expected_json_record(get_full_path(input_filename))
    with subprocess.Popen(
        [ORC_CONTENTS_PATH, output_file.name], stdout=subprocess.PIPE
    ) as proc:
        for line in proc.stdout:
            data = next(exp_res)
            if pyorc.orc_version_info.major >= 2 and pyorc.orc_version_info.minor > 0:
                # From 2.1.0, orc-content returns decimals as string to the output,
                # whilte the example json has floats in it.
                data["_col0"] = (
                    data["_col0"] if data["_col0"] is None else str(data["_col0"])
                )
            assert json.loads(line) == data
    with pytest.raises(StopIteration):
        next(exp_res)


def test_write_timestamp(output_file):
    input_filename = "TestOrcFile.testTimestamp.jsn.gz"
    create_orc_output_for_test(Timestamp(), output_file, input_filename)
    exp_res = read_expected_json_record(get_full_path(input_filename))
    with subprocess.Popen(
        [ORC_CONTENTS_PATH, output_file.name], stdout=subprocess.PIPE
    ) as proc:
        for line in proc.stdout:
            assert datetime.strptime(
                json.loads(line)[:26], "%Y-%m-%d %H:%M:%S.%f"
            ) == datetime.strptime(next(exp_res)[:26], "%Y-%m-%d %H:%M:%S.%f")
    with pytest.raises(StopIteration):
        next(exp_res)
