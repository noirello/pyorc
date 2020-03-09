from .enums import TypeKind

class TypeDescription:
    name = ""
    kind = -1

    def __str__(self):
        return self.name

    def __init__(self):
        self._column_id = 0

    @property
    def column_id(self) -> int:
        return self._column_id

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        return self._column_id


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


class Date(TypeDescription):
    name = "date"
    kind = TypeKind.DATE


class Char(TypeDescription):
    name = "char"
    kind = TypeKind.CHAR

    def __init__(self, max_length: int):
        self.max_length = max_length
        super().__init__()

    def __str__(self) -> str:
        return "{name}({len})".format(name=self.__class__.name, len=self.max_length)


class VarChar(TypeDescription):
    name = "varchar"
    kind = TypeKind.VARCHAR

    def __init__(self, max_length: int):
        super().__init__()
        self.max_length = max_length

    def __str__(self) -> str:
        return "{name}({len})".format(name=self.__class__.name, len=self.max_length)


class Decimal(TypeDescription):
    name = "decimal"
    kind = TypeKind.DECIMAL

    def __init__(self, precision: int, scale: int):
        super().__init__()
        self.precision = precision
        self.scale = scale

    def __str__(self) -> str:
        return "{name}({prc},{scl})".format(
            name=self.__class__.name, prc=self.precision, scl=self.scale
        )


class Union(TypeDescription):
    name = "uniontype"
    kind = TypeKind.UNION

    def __init__(self, *cont_types):
        super().__init__()
        for c_types in cont_types:
            if not isinstance(c_types, TypeDescription):
                raise TypeError("Invalid container type for Union")
        self.cont_types = cont_types

    def __str__(self):
        return "{name}<{types}>".format(
            name=self.__class__.name,
            types=",".join(str(typ) for typ in self.cont_types),
        )

    def __getitem__(self, idx: int) -> TypeDescription:
        return self.cont_types[idx]

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        for c_type in self.cont_types:
            val = c_type.set_column_id(val + 1)
        return val


class Array(TypeDescription):
    name = "array"
    kind = TypeKind.LIST

    def __init__(self, cont_type: TypeDescription):
        super().__init__()
        if not isinstance(cont_type, TypeDescription):
            raise TypeError("Array's container type must be a TypeDescription instance")
        self.type = cont_type

    def __str__(self) -> str:
        return "{name}<{type}>".format(name=self.__class__.name, type=str(self.type))

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        val = self.type.set_column_id(val + 1)
        return val


class Map(TypeDescription):
    name = "map"
    kind = TypeKind.MAP

    def __init__(self, key: TypeDescription, value: TypeDescription):
        super().__init__()
        if not isinstance(key, TypeDescription):
            raise TypeError("Map's key type must be a TypeDescription instance")
        if not isinstance(value, TypeDescription):
            raise TypeError("Map's value type must be a TypeDescription instance")
        self.key = key
        self.value = value

    def __str__(self) -> str:
        return "{name}<{key},{val}>".format(
            name=self.__class__.name, key=str(self.key), val=str(self.value)
        )

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        val = self.key.set_column_id(val + 1)
        val = self.value.set_column_id(val + 1)
        return val


class Struct(TypeDescription):
    name = "struct"
    kind = TypeKind.STRUCT

    def __init__(self, **fields):
        super().__init__()
        for fld in fields.values():
            if not isinstance(fld, TypeDescription):
                raise TypeError(
                    "Struct's field type must be a TypeDescription instance"
                )
        self.fields = fields
        self.set_column_id(0)

    def __str__(self) -> str:
        return "{name}<{fields}>".format(
            name=self.__class__.name,
            fields=",".join(
                "{field}:{type}".format(field=key, type=str(val))
                for key, val in self.fields.items()
            ),
        )

    def __getitem__(self, key: str) -> TypeDescription:
        return self.fields[key]

    def set_column_id(self, val: int) -> int:
        self._column_id = val
        for fld in self.fields.values():
            val = fld.set_column_id(val + 1)
        return val

    def find_column_id(self, dotted_key: str) -> int:
        this = self
        for key in dotted_key.split("."):
            this = this.fields[key]
        return this.column_id
