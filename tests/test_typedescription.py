import pytest

from pyorc import typedescription
from pyorc.enums import TypeKind


def test_from_str_schema():
    descr = typedescription(
        "struct<a:int,b:map<varchar(20),int>,c:struct<d:bigint,e:float>>"
    )
    assert descr.kind == TypeKind.STRUCT
    assert len(descr.fields) == 3
    assert tuple(descr.fields.keys()) == ("a", "b", "c")
    assert descr.fields["a"].kind == TypeKind.INT
    assert descr.fields["a"].precision is None
    assert descr.fields["b"].scale is None
    assert descr.fields["b"].column_id == 2
    assert len(descr.fields["b"].container_types) == 2
    assert descr.fields["b"].container_types[0].kind == TypeKind.VARCHAR
    assert descr.fields["b"].container_types[0].max_length == 20
    assert descr.fields["b"].container_types[1].column_id == 4
    assert descr.fields["c"].max_length is None
    assert tuple(descr.fields["c"].fields.keys()) == ("d", "e")
    assert descr.fields["c"].fields["d"].kind == TypeKind.LONG
    assert descr.fields["c"].fields["e"].column_id == 7


def test_from_str_schema_fail():
    with pytest.raises(ValueError):
        _ = typedescription(
            "struct<a:int,b:map<varchar(20),in>,c:struct<d:bigint,e:float>>"
        )
    with pytest.raises(ValueError):
        _ = typedescription("struct<a:int,")
    with pytest.raises(TypeError):
        _ = typedescription(2.5)


def test_find_column_id():
    descr = typedescription("struct<a:struct<b:struct<c:int,d:string>,e:int>>")
    assert descr.find_column_id("a") == 1
    assert descr.find_column_id("a.b.c") == 3
    assert descr.find_column_id("a.e") == 5
    with pytest.raises(TypeError):
        _ = descr.find_column_id(True)
    with pytest.raises(KeyError):
        _ = descr.find_column_id("f.z")


def test_decimal():
    descr = typedescription(TypeKind.DECIMAL)
    assert descr.precision is None
    assert descr.scale is None
    descr.precision = 5
    descr.scale = 3
    assert descr.precision == 5
    assert descr.scale == 3
    with pytest.raises(TypeError):
        descr.precision = "f"
    with pytest.raises(TypeError):
        descr.scale = 3.14
    assert str(descr) == "decimal(5,3)"


def test_decimal_fail():
    descr = typedescription(TypeKind.VARCHAR)
    with pytest.raises(ValueError):
        descr.precision = 3
    with pytest.raises(ValueError):
        descr.scale = 2


def test_varchar():
    descr = typedescription("varchar(30)")
    assert descr.max_length == 30
    descr.max_length = 15
    assert descr.max_length == 15
    with pytest.raises(TypeError):
        descr.max_length = "f"
    assert str(descr) == "varchar(15)"
    descr = typedescription(TypeKind.CHAR)
    assert descr.max_length is None
    descr.max_length = 1
    assert str(descr) == "char(1)"


def test_varchar_fail():
    descr = typedescription(TypeKind.LONG)
    with pytest.raises(ValueError):
        descr.max_length = 3


def test_container_types():
    descr = typedescription("map<string,int>")
    assert len(descr.container_types) == 2
    assert descr.container_types[0].kind == TypeKind.STRING
    descr.container_types = (
        typedescription(TypeKind.INT),
        typedescription(TypeKind.STRING),
    )
    assert str(descr) == "map<int,string>"
    assert descr.container_types[0].column_id == 1
    with pytest.raises(ValueError):
        descr.container_types = (typedescription(TypeKind.STRING),)
    with pytest.raises(ValueError):
        descr.container_types = (0, 1)
    descr = typedescription("array<string>")
    assert len(descr.container_types) == 1
    descr.container_types = (typedescription(TypeKind.INT),)
    assert str(descr) == "array<int>"
    with pytest.raises(ValueError):
        descr.container_types = (
            typedescription(TypeKind.STRING),
            typedescription(TypeKind.INT),
        )
    descr = typedescription("uniontype<string,int,double>")
    assert len(descr.container_types) == 3
    with pytest.raises(ValueError):
        descr.container_types = []
    descr = typedescription("string")
    assert descr.container_types == []
    with pytest.raises(ValueError):
        descr.container_types = [typedescription(TypeKind.STRING)]


def test_add_field():
    descr = typedescription("string")
    with pytest.raises(ValueError):
        descr.add_field("test", typedescription(TypeKind.INT))
    descr = typedescription(TypeKind.STRUCT)
    assert descr.fields == {}
    descr.add_field("a", typedescription(TypeKind.INT))
    assert tuple(descr.fields.keys()) == ("a",)
    assert descr.fields["a"].kind == TypeKind.INT
    descr.add_field("b", typedescription(TypeKind.STRING))
    assert tuple(descr.fields.keys()) == ("a", "b")
    assert descr.fields["b"].column_id == 2
    assert str(descr) == "struct<a:int,b:string>"
    descr.add_field("a", typedescription(TypeKind.DOUBLE))
    assert str(descr) == "struct<a:double,b:string>"


def test_remove_field():
    descr = typedescription("string")
    with pytest.raises(ValueError):
        descr.remove_field("test")
    descr = typedescription("struct<a:double,b:string>")
    assert descr.fields["b"].column_id == 2
    assert "a" in descr.fields
    descr.remove_field("a")
    assert "a" not in descr.fields
    with pytest.raises(KeyError):
        descr.remove_field("c")
    assert descr.fields["b"].column_id == 1 
    assert str(descr) == "struct<b:string>"
