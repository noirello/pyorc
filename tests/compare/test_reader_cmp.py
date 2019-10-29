import pytest

import json
import gzip
import os
import math

import _pyorc

from datetime import date, datetime
from decimal import Decimal


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


def traverse_json_row(schema, row, parent=""):
    for col, value in row.items():
        orc_type = find_type(schema, col)
        if isinstance(value, dict):
            yield from traverse_json_row(
                "{0}:{1}".format(col, schema), value, "{0}.{1}".format(parent, col)
            )
        elif isinstance(value, list) and orc_type.startswith("map<"):
            for keypair in value:
                yield from traverse_json_row(
                    "{0}:{1}".format(col, schema),
                    keypair["value"],
                    "{0}.{1}['{2}']".format(parent, col, keypair["key"]),
                )
        elif isinstance(value, list) and orc_type.startswith("array<"):
            for idx, item in enumerate(value):
                yield from traverse_json_row(
                    "{0}:{1}".format(col, schema),
                    item,
                    "{0}.{1}[{2}]".format(parent, col, idx),
                )
        else:
            yield orc_type, "{0}.{1}".format(parent, col), value


def traverse_orc_row(schema, row, parent=""):
    for col, value in row.items():
        orc_type = find_type(schema, col)
        if isinstance(value, dict) and orc_type.startswith("struct<"):
            yield from traverse_orc_row(
                "{0}:{1}".format(col, schema), value, "{0}.{1}".format(parent, col)
            )
        elif isinstance(value, dict) and orc_type.startswith("map<"):
            for key, val in value.items():
                yield from traverse_orc_row(
                    "{0}:{1}".format(col, schema),
                    val,
                    "{0}.{1}['{2}']".format(parent, col, key),
                )
        elif isinstance(value, list) and orc_type.startswith("array<"):
            for idx, item in enumerate(value):
                yield from traverse_orc_row(
                    "{0}:{1}".format(col, schema),
                    item,
                    "{0}.{1}[{2}]".format(parent, col, idx),
                )
        else:
            yield orc_type, "{0}.{1}".format(parent, col), value


def get_full_path(path):
    curdir = os.path.abspath(os.path.dirname(__file__))
    projdir = os.path.abspath(os.path.join(curdir, os.pardir, os.pardir))
    return os.path.join(projdir, "deps", "examples", path)


def idfn(val):
    return val.split("/")[-1]


TESTDATA = [
    ("TestOrcFile.emptyFile.orc", "expected/TestOrcFile.emptyFile.jsn.gz"),
    ("TestOrcFile.test1.orc", "expected/TestOrcFile.test1.jsn.gz"),
    ("TestOrcFile.testDate1900.orc", "expected/TestOrcFile.testDate1900.jsn.gz"),
    ("TestOrcFile.testDate2038.orc", "expected/TestOrcFile.testDate2038.jsn.gz"),
    ("TestOrcFile.testSeek.orc", "expected/TestOrcFile.testSeek.jsn.gz"),
    ("TestOrcFile.testSnappy.orc", "expected/TestOrcFile.testSnappy.jsn.gz"),
    ("nulls-at-end-snappy.orc", "expected/nulls-at-end-snappy.jsn.gz"),
    ("demo-12-zlib.orc", "expected/demo-12-zlib.jsn.gz"),
    ("decimal.orc", "expected/decimal.jsn.gz"),
]


@pytest.mark.parametrize("example,expected", TESTDATA, ids=idfn)
def test_read(example, expected):
    exp_res = gzip.open(get_full_path(expected), "rb")
    with open(get_full_path(example), "rb") as fileo:
        orc_res = _pyorc.reader(fileo)
        length = 0
        str_schema = str(orc_res.schema)
        for num, line in enumerate(exp_res):
            json_row = traverse_json_row(str_schema, json.loads(line))
            orc_row = traverse_orc_row(str_schema, next(orc_res))
            for _, exp_path, exp_val in json_row:
                otype, act_path, act_val = next(orc_row)
                assert exp_path == act_path
                if exp_val is None:
                    assert act_val is None
                elif otype == "binary":
                    assert exp_val == [
                        int(i) for i in act_val
                    ], "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == "double" or otype == "float":
                    assert math.isclose(
                        exp_val,
                        act_val,
                        abs_tol=0.005,  # Extermely permissive float comparing.
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == "timestamp":
                    assert exp_val == act_val.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip(
                        "0"
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == "date":
                    assert (
                        exp_val == act_val.isoformat()
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif "decimal(" in otype:
                    assert (
                        exp_val == float(act_val)  # Not the best comparing.
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                else:
                    assert exp_val == act_val, "Row #{num}, Column: `{path}`".format(
                        num=num + 1, path=act_path
                    )
            length = num + 1
        assert len(orc_res) == length, "ORC file has a different number of row"
    exp_res.close()


def test_read_timestamp():
    exp_res = gzip.open(
        get_full_path("expected/TestOrcFile.testTimestamp.jsn.gz"), "rb"
    )
    with open(get_full_path("TestOrcFile.testTimestamp.orc"), "rb") as fileo:
        orc_res = _pyorc.reader(fileo)
        length = 0
        for num, line in enumerate(exp_res):
            json_row = datetime.strptime(json.loads(line)[:26], "%Y-%m-%d %H:%M:%S.%f")
            orc_row = next(orc_res)
            assert json_row == orc_row
            length = num + 1
        assert len(orc_res) == length
