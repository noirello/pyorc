import os
import sys
from typing import NamedTuple

if sys.platform.startswith("win32") and "TZDIR" not in os.environ:
    # Windows does not come with a standard IANA time zone database,
    # but the ORC lib requires it. Set the TZDIR environment variable
    # to the tzdata module's data directory.
    import tzdata

    os.environ["TZDIR"] = os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo")

from pyorc._pyorc import _orc_version

from .enums import *
from .errors import *
from .predicates import PredicateColumn
from .reader import Column, Reader, Stripe
from .typedescription import *
from .writer import Writer

__version__ = "0.8.0"

orc_version = _orc_version()


def __extract_version_info():
    _orc_version_info = NamedTuple(
        "orc_version_info",
        [("major", int), ("minor", int), ("patch", int), ("releaselevel", str)],
    )
    splitted = _orc_version().split("-")
    ver = splitted[0]
    rel_level = splitted[1] if len(splitted) > 1 else ""
    major, minor, patch = map(int, ver.split("."))
    return _orc_version_info(major, minor, patch, rel_level)


orc_version_info = __extract_version_info()
