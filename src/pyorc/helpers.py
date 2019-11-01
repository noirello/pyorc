from decimal import Decimal, localcontext

def adjust_decimal(prec: int, scale: int, dec: Decimal) -> int:
    """
    Adjust the Decimal number to the given precision and scale.
    Return the integer value of the number.

    :param int prec: the precision of the decimal
    :param int scale: the scale of decimal
    :param decimal.Decimal dec: the number as Python decimal
    :return: an integer (interpreted with the given scale)
    :rtype: int
    """
    with localcontext() as ctx:
        try:
            ctx.prec = prec
            coefficent = Decimal("1.{0}".format("0" * scale))
            dec = dec.quantize(coefficent)
            dec_tup = dec.as_tuple()
            integer = sum(dig * 10 ** exp for exp, dig in enumerate(reversed(dec_tup.digits)))
            if dec_tup.exponent > 0:
                integer = integer * 10 ** dec_tup.exponent
            if dec_tup.sign == 1:
                return integer * -1
            else:
                return integer
        except AttributeError:
            raise TypeError("Item {0} cannot be cast as a decimal".format(type(dec))) from None
