from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone as tz
from decimal import Decimal, localcontext
import math
from typing import Dict, Tuple, Type, Any

from .enums import TypeKind

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo


class ORCConverter(ABC):
    @staticmethod
    @abstractmethod
    def from_orc(*args):
        pass

    @staticmethod
    @abstractmethod
    def to_orc(*args):
        pass


class TimestampConverter(ORCConverter):
    @staticmethod
    def from_orc(
        seconds: int, nanoseconds: int, timezone: zoneinfo.ZoneInfo,
    ) -> datetime:
        epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tz.utc)
        return (
            epoch + timedelta(seconds=seconds, microseconds=nanoseconds // 1000)
        ).astimezone(timezone)

    @staticmethod
    def to_orc(obj: datetime, timezone: zoneinfo.ZoneInfo) -> Tuple[int, int]:
        return math.floor(obj.timestamp()), obj.microsecond * 1000


class DateConverter(ORCConverter):
    @staticmethod
    def from_orc(days: int) -> date:
        return date(1970, 1, 1) + timedelta(days=days)

    @staticmethod
    def to_orc(obj: date) -> int:
        return (obj - date(1970, 1, 1)).days


class DecimalConverter(ORCConverter):
    @staticmethod
    def from_orc(decimal: str) -> Decimal:
        return Decimal(decimal)

    @staticmethod
    def to_orc(precision: int, scale: int, obj: Decimal) -> int:
        """
        Adjust the Decimal number to the given precision and scale.
        Return the integer value of the number.

        :param int precision: the precision of the decimal
        :param int scale: the scale of decimal
        :param decimal.Decimal obj: the number as Python decimal
        :return: an integer (interpreted with the given scale)
        :rtype: int
        """
        with localcontext() as ctx:
            try:
                ctx.prec = precision
                coefficient = Decimal("1.{0}".format("0" * scale))
                dec = obj.quantize(coefficient)
                dec_tup = dec.as_tuple()
                integer = sum(
                    dig * 10 ** exp for exp, dig in enumerate(reversed(dec_tup.digits))
                )
                if dec_tup.exponent > 0:
                    integer = integer * 10 ** dec_tup.exponent
                if dec_tup.sign == 1:
                    return integer * -1
                else:
                    return integer
            except AttributeError:
                raise TypeError(
                    "Item {0} cannot be cast as a decimal".format(type(obj))
                ) from None


DEFAULT_CONVERTERS: Dict[TypeKind, Type[ORCConverter]] = {
    TypeKind.DATE: DateConverter,
    TypeKind.DECIMAL: DecimalConverter,
    TypeKind.TIMESTAMP: TimestampConverter,
}
