import pytest

from datetime import datetime
from decimal import Decimal

from pyorc.predicates import *
from pyorc.enums import TypeKind


def test_column_type():
    with pytest.raises(TypeError):
        _ = PredicateColumn("a", "something")
    with pytest.raises(TypeError):
        _ = PredicateColumn(TypeKind.STRUCT, "something")
    with pytest.raises(TypeError):
        _ = PredicateColumn(TypeKind.LONG, name=0)
    with pytest.raises(TypeError):
        _ = PredicateColumn(TypeKind.LONG, index="a")
    with pytest.raises(TypeError):
        _ = PredicateColumn(TypeKind.LONG, name="a", index=0)
    col = PredicateColumn(TypeKind.LONG, "colname")
    assert col is not None


def test_column_fields():
    col = PredicateColumn(TypeKind.LONG, "colname")
    assert col.name == "colname"
    assert col.type_kind == 4
    assert col.precision == 0
    assert col.scale == 0
    col = PredicateColumn(TypeKind.DECIMAL, "colname", precision=2, scale=3)
    assert col.type_kind == TypeKind.DECIMAL
    assert col.precision == 2
    assert col.scale == 3


def test_equals():
    col = PredicateColumn(TypeKind.LONG, "colname")
    pred = col == 100
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.EQ, col, 100)


def test_not_equals():
    col = PredicateColumn(TypeKind.STRING, "colname")
    pred = col != "test"
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.EQ, col, "test"))


def test_less_than():
    col = PredicateColumn(TypeKind.INT, "colname")
    pred = col < 100
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.LT, col, 100)


def test_less_than_or_equals():
    col = PredicateColumn(TypeKind.LONG, "colname")
    pred = col <= 50
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.LE, col, 50)


def test_greater_than():
    col = PredicateColumn(TypeKind.DOUBLE, "colname")
    pred = col > 5.0
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.LE, col, 5.0))


def test_greater_than_or_equals():
    col = PredicateColumn(TypeKind.FLOAT, "colname")
    pred = col >= 10.0
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.LT, col, 10.0))


def test_and():
    col0 = PredicateColumn(TypeKind.LONG, "colname0")
    col1 = PredicateColumn(TypeKind.TIMESTAMP, "colname1")
    pred = (col0 < 100) & (col1 == datetime(2021, 3, 20))
    assert isinstance(pred, Predicate)
    assert pred.values == (
        Operator.AND,
        (Operator.LT, col0, 100),
        (Operator.EQ, col1, datetime(2021, 3, 20)),
    )


def test_or():
    col0 = PredicateColumn(TypeKind.SHORT, name="colname0")
    col1 = PredicateColumn(TypeKind.DECIMAL, name="colname1", precision=2, scale=2)
    pred = (col0 < 100) & (col1 >= Decimal("20.00"))
    assert isinstance(pred, Predicate)
    assert pred.values == (
        Operator.AND,
        (Operator.LT, col0, 100),
        (Operator.NOT, (Operator.LT, col1, Decimal("20.00"))),
    )


def test_not():
    col = PredicateColumn(TypeKind.FLOAT, "colname")
    pred = ~((col < 1.0) & (col > -1.0))
    assert isinstance(pred, Predicate)
    assert pred.values == (
        Operator.NOT,
        (
            Operator.AND,
            (Operator.LT, col, 1.0),
            (Operator.NOT, (Operator.LE, col, -1.0)),
        ),
    )


def test_decimal():
    with pytest.raises(ValueError):
        _ = PredicateColumn(TypeKind.DECIMAL, "something")
    col = PredicateColumn(TypeKind.DECIMAL, "colname", precision=10, scale=3)
    assert col is not None
