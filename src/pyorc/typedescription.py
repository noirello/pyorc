import re
from types import MappingProxyType
from typing import Dict, Mapping, Tuple

from pyorc._pyorc import _schema_from_string

from .enums import TypeKind


class TypeDescription:
    name = ""
    kind = -1

    def __init__(self) -> None:
        self._column_id = 0
        self._attributes: Dict[str, str] = {}

    def __str__(self) -> str:
        return self.name

    @property
    def attributes(self) -> Dict[str, str]:
        return self._attributes

    def set_attributes(self, val) -> None:
        if isinstance(val, dict):
            if all(
                isinstance(key, str) and isinstance(val, str)
                for key, val in val.items()
            ):
                self._attributes = val
            else:
                raise TypeError(
                    "The all keys and values in the attributes dictionary must be string"
                )
        else:
            raise TypeError("The attributes must be a dictionary")

    @property
    def column_id(self) -> int:
        return self._column_id

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        return self._column_id

    def find_column_id(self, dotted_key: str) -> int:
        raise KeyError(dotted_key)

    @staticmethod
    def from_string(schema: str) -> "TypeDescription":
        return _schema_from_string(schema)


class Boolean(TypeDescription):
    name = "boolean"
    kind = TypeKind.BOOLEAN


class TinyInt(TypeDescription):
    name = "tinyint"
    kind = TypeKind.BYTE


class SmallInt(TypeDescription):
    name = "smallint"
    kind = TypeKind.SHORT


class Int(TypeDescription):
    name = "int"
    kind = TypeKind.INT


class BigInt(TypeDescription):
    name = "bigint"
    kind = TypeKind.LONG


class Float(TypeDescription):
    name = "float"
    kind = TypeKind.FLOAT


class Double(TypeDescription):
    name = "double"
    kind = TypeKind.DOUBLE


class String(TypeDescription):
    name = "string"
    kind = TypeKind.STRING


class Binary(TypeDescription):
    name = "binary"
    kind = TypeKind.BINARY


class Timestamp(TypeDescription):
    name = "timestamp"
    kind = TypeKind.TIMESTAMP


class TimestampInstant(TypeDescription):
    name = "timestamp with local time zone"
    kind = TypeKind.TIMESTAMP_INSTANT


class Date(TypeDescription):
    name = "date"
    kind = TypeKind.DATE


class Char(TypeDescription):
    name = "char"
    kind = TypeKind.CHAR

    def __init__(self, max_length: int) -> None:
        self.max_length = max_length
        super().__init__()

    def __str__(self) -> str:
        return "{name}({len})".format(name=Char.name, len=self.max_length)


class VarChar(TypeDescription):
    name = "varchar"
    kind = TypeKind.VARCHAR

    def __init__(self, max_length: int) -> None:
        super().__init__()
        self.max_length = max_length

    def __str__(self) -> str:
        return "{name}({len})".format(name=VarChar.name, len=self.max_length)


class Decimal(TypeDescription):
    name = "decimal"
    kind = TypeKind.DECIMAL

    def __init__(self, precision: int, scale: int) -> None:
        super().__init__()
        self.precision = precision
        self.scale = scale

    def __str__(self) -> str:
        return "{name}({prc},{scl})".format(
            name=Decimal.name, prc=self.precision, scl=self.scale
        )


class Union(TypeDescription):
    name = "uniontype"
    kind = TypeKind.UNION

    def __init__(self, *cont_types: TypeDescription) -> None:
        super().__init__()
        for c_types in cont_types:
            if not isinstance(c_types, TypeDescription):
                raise TypeError("Invalid container type for Union")
        self.__cont_types = cont_types

    def __str__(self):
        return "{name}<{types}>".format(
            name=Union.name, types=",".join(str(typ) for typ in self.__cont_types),
        )

    def __getitem__(self, idx: int) -> TypeDescription:
        return self.__cont_types[idx]

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        for c_type in self.__cont_types:
            val = c_type.set_column_id(val + 1)
        return val

    @property
    def cont_types(self) -> Tuple[TypeDescription, ...]:
        return self.__cont_types


class Array(TypeDescription):
    name = "array"
    kind = TypeKind.LIST

    def __init__(self, cont_type: TypeDescription) -> None:
        super().__init__()
        if not isinstance(cont_type, TypeDescription):
            raise TypeError("Array's container type must be a TypeDescription instance")
        self.__type = cont_type

    def __str__(self) -> str:
        return "{name}<{type}>".format(name=Array.name, type=str(self.__type))

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        val = self.__type.set_column_id(val + 1)
        return val

    @property
    def type(self) -> TypeDescription:
        return self.__type


class Map(TypeDescription):
    name = "map"
    kind = TypeKind.MAP

    def __init__(self, key: TypeDescription, value: TypeDescription) -> None:
        super().__init__()
        if not isinstance(key, TypeDescription):
            raise TypeError("Map's key type must be a TypeDescription instance")
        if not isinstance(value, TypeDescription):
            raise TypeError("Map's value type must be a TypeDescription instance")
        self.__key = key
        self.__value = value

    def __str__(self) -> str:
        return "{name}<{key},{val}>".format(
            name=Map.name, key=str(self.__key), val=str(self.__value)
        )

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        val = self.__key.set_column_id(val + 1)
        val = self.__value.set_column_id(val + 1)
        return val

    @property
    def key(self) -> TypeDescription:
        return self.__key

    @property
    def value(self) -> TypeDescription:
        return self.__value


class Struct(TypeDescription):
    name = "struct"
    kind = TypeKind.STRUCT

    def __init__(self, **fields: TypeDescription) -> None:
        super().__init__()
        for fld in fields.values():
            if not isinstance(fld, TypeDescription):
                raise TypeError(
                    "Struct's field type must be a TypeDescription instance"
                )
        self.__fields = fields
        self.set_column_id(0)

    def __str__(self) -> str:
        return "{name}<{fields}>".format(
            name=Struct.name,
            fields=",".join(
                "{field}:{type}".format(field=key, type=str(val))
                for key, val in self.__fields.items()
            ),
        )

    def __getitem__(self, key: str) -> TypeDescription:
        return self.__fields[key]

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        for fld in self.__fields.values():
            val = fld.set_column_id(val + 1)
        return val

    def find_column_id(self, dotted_key: str) -> int:
        this = self
        # Allow to use backtick for escaping column names with dot.
        for key in re.findall(r"[^\.`]+|`[^`]*`", dotted_key):
            this = this[key.replace("`", "")]
        return this.column_id

    @property
    def fields(self) -> Mapping[str, TypeDescription]:
        return MappingProxyType(self.__fields)
