import pytest

from pyorc.typedescription import *
from pyorc.enums import TypeKind


def test_from_str_schema():
    descr = TypeDescription.from_string(
        "struct<a:int,b:map<varchar(20),int>,c:struct<d:bigint,e:float,f:char(12)>>"
    )
    assert descr.kind == TypeKind.STRUCT
    assert len(descr.fields) == 3
    assert tuple(descr.fields.keys()) == ("a", "b", "c")
    assert descr.fields["a"].kind == TypeKind.INT
    assert descr.fields["b"].column_id == 2
    assert descr.fields["b"].key.kind == TypeKind.VARCHAR
    assert descr.fields["b"].key.max_length == 20
    assert descr.fields["b"].value.column_id == 4
    assert tuple(descr.fields["c"].fields.keys()) == ("d", "e", "f")
    assert descr.fields["c"].fields["d"].kind == TypeKind.LONG
    assert descr.fields["c"].fields["e"].column_id == 7
    assert descr.fields["c"].fields["f"].kind == TypeKind.CHAR
    assert descr.fields["c"].fields["f"].max_length == 12
    assert descr.fields["c"].fields["f"].column_id == 8


def test_from_str_schema_fail():
    with pytest.raises(ValueError):
        _ = TypeDescription.from_string(
            "struct<a:int,b:map<varchar(20),in>,c:struct<d:bigint,e:float>>"
        )
    with pytest.raises(ValueError):
        _ = TypeDescription.from_string("struct<a:int,")
    with pytest.raises(TypeError):
        _ = TypeDescription.from_string(2.5)


def test_find_column_id():
    descr = TypeDescription.from_string(
        "struct<a:struct<b:struct<c:int,d:string>,e:int>>"
    )
    assert descr.find_column_id("a") == 1
    assert descr.find_column_id("a.b.c") == 3
    assert descr.find_column_id("a.e") == 5
    with pytest.raises(TypeError):
        _ = descr.find_column_id(True)
    with pytest.raises(KeyError):
        _ = descr.find_column_id("f.z")
    descr = Struct(**{"a.b": Struct(c=Int()), "d": String()})
    assert descr.find_column_id("`a.b`") == 1
    assert descr.find_column_id("`a.b`.c") == 2
    assert descr.find_column_id("d") == 3
    with pytest.raises(KeyError):
        _ = descr.find_column_id("a.b")


TESTDATA = [
    (Boolean(), TypeKind.BOOLEAN, "boolean"),
    (TinyInt(), TypeKind.BYTE, "tinyint"),
    (SmallInt(), TypeKind.SHORT, "smallint"),
    (Int(), TypeKind.INT, "int"),
    (BigInt(), TypeKind.LONG, "bigint"),
    (Float(), TypeKind.FLOAT, "float"),
    (Double(), TypeKind.DOUBLE, "double"),
    (Date(), TypeKind.DATE, "date"),
    (Timestamp(), TypeKind.TIMESTAMP, "timestamp"),
    (TimestampInstant(), TypeKind.TIMESTAMP_INSTANT, "timestamp with local time zone"),
    (String(), TypeKind.STRING, "string"),
    (Binary(), TypeKind.BINARY, "binary"),
    (Decimal(precision=10, scale=3), TypeKind.DECIMAL, "decimal(10,3)"),
    (Char(16), TypeKind.CHAR, "char(16)"),
    (VarChar(140), TypeKind.VARCHAR, "varchar(140)"),
    (
        Union(Int(), Double(), Char(20)),
        TypeKind.UNION,
        "uniontype<int,double,char(20)>",
    ),
    (Array(Int()), TypeKind.LIST, "array<int>"),
    (Map(key=String(), value=Double()), TypeKind.MAP, "map<string,double>"),
    (Struct(a=String(), b=Date()), TypeKind.STRUCT, "struct<a:string,b:date>"),
    (
        Struct(a=Timestamp(), b=Struct(c=Int(), b=Array(Double()))),
        TypeKind.STRUCT,
        "struct<a:timestamp,b:struct<c:int,b:array<double>>>",
    ),
]


@pytest.mark.parametrize("orc_schema,kind,expected", TESTDATA)
def test_str(orc_schema, kind, expected):
    assert str(orc_schema) == expected


@pytest.mark.parametrize("orc_schema,kind,expected", TESTDATA)
def test_kind(orc_schema, kind, expected):
    assert orc_schema.kind == kind


def test_decimal():
    descr = Decimal(precision=5, scale=3)
    assert descr.precision == 5
    assert descr.scale == 3
    assert str(descr) == "decimal(5,3)"


def test_varchar():
    descr = TypeDescription.from_string("varchar(30)")
    assert descr.max_length == 30
    descr.max_length = 15
    assert descr.max_length == 15
    assert str(descr) == "varchar(15)"


def test_char():
    descr = Char(10)
    assert descr.max_length == 10
    descr.max_length = 1
    assert str(descr) == "char(1)"


TESTDATA = [
    lambda: Struct(field0=Int(), field1=True),
    lambda: Map(key=Int(), value=True),
    lambda: Map(key=0, value=Double()),
    lambda: Array("test"),
    lambda: Union(Int(), 0, Double()),
]


@pytest.mark.parametrize("orc_schema", TESTDATA)
def test_failed_complex_types(orc_schema):
    with pytest.raises(TypeError):
        _ = orc_schema()


def test_struct():
    schema = Struct(a0=Int(), b0=Double(), c0=Struct(a1=Date(), b1=Timestamp()))
    assert isinstance(schema["a0"], Int)
    assert schema["b0"].kind == TypeKind.DOUBLE
    assert schema["c0"].column_id == 3
    assert schema["c0"]["b1"].kind == TypeKind.TIMESTAMP


def test_union():
    schema = TypeDescription.from_string("uniontype<int,double,string>")
    assert schema[1].kind == TypeKind.DOUBLE
    with pytest.raises(IndexError):
        _ = schema[10]
    schema = Union(Float(), VarChar(120))
    assert schema[0].kind == TypeKind.FLOAT


def test_attributes():
    schema = Boolean()
    with pytest.raises(TypeError):
        _ = schema.set_attributes(0)
    with pytest.raises(TypeError):
        _ = schema.set_attributes({0: "1"})
    with pytest.raises(TypeError):
        _ = schema.set_attributes({"a": 1})
    attrs = {"a": "1", "b": "2"}
    schema.set_attributes(attrs)
    assert schema.attributes == attrs
