import pytest

import json
import gzip
import os
import math

from pyorc import TypeKind, StructRepr
import pyorc._pyorc

from datetime import date, datetime
from decimal import Decimal


def traverse_json_row(schema, value, parent=""):
    if schema.kind < 8:
        # Primitive types, no transformation.
        yield schema.kind, parent, value
    elif schema.kind == TypeKind.STRUCT:
        for key, val in value.items():
            yield from traverse_json_row(
                schema[key], val, "{0}.{1}".format(parent, key)
            )
    elif schema.kind == TypeKind.MAP:
        for keypair in value:
            yield from traverse_json_row(
                schema.value,
                keypair["value"],
                "{0}['{1}']".format(parent, keypair["key"]),
            )
    elif schema.kind == TypeKind.LIST:
        for idx, item in enumerate(value):
            yield from traverse_json_row(
                schema.type, item, "{0}[{1}]".format(parent, idx)
            )
    elif schema.kind == TypeKind.UNION:
        yield schema.kind, parent, value["value"] if value is not None else None


def traverse_orc_row(schema, value, parent=""):
    if schema.kind < 8 or schema.kind == TypeKind.UNION:
        # Primitive types, no transformation.
        yield schema.kind, parent, value
    elif schema.kind == TypeKind.STRUCT:
        for key, val in value.items():
            yield from traverse_orc_row(
                schema[key], val, "{0}.{1}".format(parent, key)
            )
    elif schema.kind == TypeKind.MAP:
        for key, val in value.items():
            yield from traverse_orc_row(
                schema.value, val, "{0}['{1}']".format(parent, key)
            )
    elif schema.kind == TypeKind.LIST:
        for idx, item in enumerate(value):
            yield from traverse_orc_row(
                schema.type, item, "{0}[{1}]".format(parent, idx)
            )


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
    ("TestOrcFile.testTimestamp.orc", "expected/TestOrcFile.testTimestamp.jsn.gz"),
    (
        "TestOrcFile.testUnionAndTimestamp.orc",
        "expected/TestOrcFile.testUnionAndTimestamp.jsn.gz",
    ),
    ("nulls-at-end-snappy.orc", "expected/nulls-at-end-snappy.jsn.gz"),
    ("demo-12-zlib.orc", "expected/demo-12-zlib.jsn.gz"),
    ("decimal.orc", "expected/decimal.jsn.gz"),
]


@pytest.mark.parametrize("example,expected", TESTDATA, ids=idfn)
def test_read(example, expected):
    exp_res = gzip.open(get_full_path(expected), "rb")
    with open(get_full_path(example), "rb") as fileo:
        orc_res = pyorc._pyorc.reader(fileo, struct_repr=StructRepr.DICT)
        length = 0
        for num, line in enumerate(exp_res):
            json_row = traverse_json_row(orc_res.schema, json.loads(line))
            orc_row = traverse_orc_row(orc_res.schema, next(orc_res))
            for _, exp_path, exp_val in json_row:
                otype, act_path, act_val = next(orc_row)
                assert exp_path == act_path
                if exp_val is None:
                    assert act_val is None
                elif otype == TypeKind.BINARY:
                    assert exp_val == [
                        int(i) for i in act_val
                    ], "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == TypeKind.DOUBLE or otype == TypeKind.FLOAT:
                    assert math.isclose(
                        exp_val,
                        act_val,
                        abs_tol=0.005,  # Extermely permissive float comparing.
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == TypeKind.TIMESTAMP:
                    assert exp_val == act_val.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip(
                        "0"
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == TypeKind.DATE:
                    assert (
                        exp_val == act_val.isoformat()
                    ), "Row #{num}, Column: `{path}`".format(num=num + 1, path=act_path)
                elif otype == TypeKind.DECIMAL:
                    assert exp_val == float(
                        act_val
                    ), "Row #{num}, Column: `{path}`".format(  # Not the best comparing.
                        num=num + 1, path=act_path
                    )
                else:
                    assert exp_val == act_val, "Row #{num}, Column: `{path}`".format(
                        num=num + 1, path=act_path
                    )
            length = num + 1
        assert len(orc_res) == length, "ORC file has a different number of row"
    exp_res.close()


def test_metadata_read():
    with open(get_full_path("TestOrcFile.emptyFile.orc"), "rb") as fileo:
        res = pyorc._pyorc.reader(fileo, struct_repr=StructRepr.DICT)
        assert res.metadata == {}
    with open(get_full_path("TestOrcFile.metaData.orc"), "rb") as fileo:
        res = pyorc._pyorc.reader(fileo, struct_repr=StructRepr.DICT)
        assert res.metadata["clobber"] == b"\x05\x07\x0b\r\x11\x13"
        assert (
            res.metadata["my.meta"] == b"\x01\x02\x03\x04\x05\x06\x07\xff\xfe\x7f\x80"
        )


def test_format_version():
    with open(get_full_path("demo-11-zlib.orc"), "rb") as fileo:
        res = pyorc._pyorc.reader(fileo)
        assert res.format_version == (0, 11)
    with open(get_full_path("demo-12-zlib.orc"), "rb") as fileo:
        res = pyorc._pyorc.reader(fileo)
        assert res.format_version == (0, 12)


def test_writer_id():
    with open(get_full_path("demo-12-zlib.orc"), "rb") as fileo:
        res = pyorc.reader.Reader(fileo)
        assert res.writer_id == "ORC_JAVA_WRITER"


def test_writer_version():
    with open(get_full_path("demo-12-zlib.orc"), "rb") as fileo:
        res = pyorc.reader.Reader(fileo)
        assert res.writer_version == 1
    with open(get_full_path("decimal.orc"), "rb") as fileo:
        res = pyorc.reader.Reader(fileo)
        assert res.writer_version == 0
