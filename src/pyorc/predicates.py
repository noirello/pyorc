import enum
from typing import Any, Optional

from .enums import TypeKind


class Operator(enum.IntEnum):
    NOT = 0
    OR = 1
    AND = 2
    EQ = 3
    LT = 4
    LE = 5


class Predicate:
    def __init__(self, operator: Operator, left, right) -> None:
        self.values = (operator, left, right)

    def __or__(self, other) -> "Predicate":
        self.values = (Operator.OR, self.values, other.values)
        return self

    def __and__(self, other) -> "Predicate":
        self.values = (Operator.AND, self.values, other.values)
        return self

    def __invert__(self) -> "Predicate":
        self.values = (Operator.NOT, self.values)
        return self


class PredicateColumn:
    def __init__(
        self,
        type_kind: TypeKind,
        name: Optional[str] = None,
        index: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> None:
        if not TypeKind.has_value(type_kind) or type_kind in (
            TypeKind.BINARY,
            TypeKind.LIST,
            TypeKind.MAP,
            TypeKind.UNION,
            TypeKind.STRUCT,
        ):
            raise TypeError("Invalid type for PredicateColumn: %s" % type_kind)
        self.type_kind = type_kind
        if self.type_kind == TypeKind.DECIMAL and (precision is None or scale is None):
            raise ValueError("Both precision and scale must be set for Decimal type")
        if name is not None and index is not None:
            raise TypeError("Only one of the name or index parameter must be given")
        if name is not None and not isinstance(name, str):
            raise TypeError("Name parameter must be string")
        if index is not None and not isinstance(index, int):
            raise TypeError("Index parameter must be int")
        self.name = name
        self.index = index
        self.precision = precision if precision is not None else 0
        self.scale = scale if scale is not None else 0

    def __eq__(self, other: Any) -> Predicate:
        return Predicate(Operator.EQ, self, other)

    def __ne__(self, other: Any) -> Predicate:
        return ~Predicate(Operator.EQ, self, other)

    def __lt__(self, other: Any) -> Predicate:
        return Predicate(Operator.LT, self, other)

    def __le__(self, other: Any) -> Predicate:
        return Predicate(Operator.LE, self, other)

    def __gt__(self, other: Any) -> Predicate:
        return ~Predicate(Operator.LE, self, other)

    def __ge__(self, other: Any) -> Predicate:
        return ~Predicate(Operator.LT, self, other)
