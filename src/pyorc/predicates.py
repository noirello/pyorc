import enum

from .enums import TypeKind


class Operator(enum.IntEnum):
    NOT = 0
    OR = 1
    AND = 2
    EQ = 3
    LT = 4
    LE = 5


class Predicate:
    def __init__(self, operator, left, right):
        self.values = (operator, left, right)

    def __or__(self, other):
        self.values = (Operator.OR, self.values, other.values)
        return self

    def __and__(self, other):
        self.values = (Operator.AND, self.values, other.values)
        return self

    def __invert__(self):
        self.values = (Operator.NOT, self.values)
        return self


class PredicateColumn:
    def __init__(self, name, type_kind, precision=None, scale=None):
        self.name = name
        if not TypeKind.has_value(type_kind) or type_kind in (
            TypeKind.BINARY,
            TypeKind.LIST,
            TypeKind.MAP,
            TypeKind.UNION,
            TypeKind.STRUCT,
        ):
            raise TypeError("Invalid type for PredicateColumn: %s" % type_kind)
        self.type_kind = type_kind
        self.precision = precision if precision is not None else 0
        self.scale = scale if scale is not None else 0

    def __eq__(self, other):
        return Predicate(Operator.EQ, self, other)

    def __ne__(self, other):
        return ~Predicate(Operator.EQ, self, other)

    def __lt__(self, other):
        return Predicate(Operator.LT, self, other)

    def __le__(self, other):
        return Predicate(Operator.LE, self, other)

    def __gt__(self, other):
        return ~Predicate(Operator.LE, self, other)

    def __ge__(self, other):
        return ~Predicate(Operator.LT, self, other)
