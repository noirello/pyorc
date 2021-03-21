import pytest

from datetime import datetime
from decimal import Decimal

from pyorc.predicates import *
from pyorc.enums import TypeKind


def test_column_type():
    with pytest.raises(TypeError):
        _ = PredicateColumn("something", "a")
    with pytest.raises(TypeError):
        _ = PredicateColumn("something", TypeKind.STRUCT)
    col = PredicateColumn("colname", TypeKind.LONG)
    assert col is not None


def test_column_fields():
    col = PredicateColumn("colname", TypeKind.LONG)
    assert col.name == "colname"
    assert col.type_kind == 4
    assert col.precision == 0
    assert col.scale == 0
    col = PredicateColumn("colname", TypeKind.DECIMAL, precision=2, scale=3)
    assert col.type_kind == TypeKind.DECIMAL
    assert col.precision == 2
    assert col.scale == 3


def test_equals():
    col = PredicateColumn("colname", TypeKind.LONG)
    pred = col == 100
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.EQ, col, 100)


def test_not_equals():
    col = PredicateColumn("colname", TypeKind.STRING)
    pred = col != "test"
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.EQ, col, "test"))


def test_less_than():
    col = PredicateColumn("colname", TypeKind.INT)
    pred = col < 100
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.LT, col, 100)


def test_less_than_or_equals():
    col = PredicateColumn("colname", TypeKind.LONG)
    pred = col <= 50
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.LE, col, 50)


def test_greater_than():
    col = PredicateColumn("colname", TypeKind.DOUBLE)
    pred = col > 5.0
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.LE, col, 5.0))


def test_greater_than_or_equals():
    col = PredicateColumn("colname", TypeKind.FLOAT)
    pred = col >= 10.0
    assert isinstance(pred, Predicate)
    assert pred.values == (Operator.NOT, (Operator.LT, col, 10.0))


def test_and():
    col0 = PredicateColumn("colname0", TypeKind.LONG)
    col1 = PredicateColumn("colname1", TypeKind.TIMESTAMP)
    pred = (col0 < 100) & (col1 == datetime(2021, 3, 20))
    assert isinstance(pred, Predicate)
    assert pred.values == (
        Operator.AND,
        (Operator.LT, col0, 100),
        (Operator.EQ, col1, datetime(2021, 3, 20)),
    )


def test_or():
    col0 = PredicateColumn("colname0", TypeKind.SHORT)
    col1 = PredicateColumn("colname1", TypeKind.DECIMAL, 2, 2)
    pred = (col0 < 100) & (col1 >= Decimal("20.00"))
    assert isinstance(pred, Predicate)
    assert pred.values == (
        Operator.AND,
        (Operator.LT, col0, 100),
        (Operator.NOT, (Operator.LT, col1, Decimal("20.00"))),
    )


def test_not():
    col = PredicateColumn("colname", TypeKind.FLOAT)
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

