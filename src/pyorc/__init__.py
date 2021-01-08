import os
import sys

if sys.platform.startswith("win32") and "TZDIR" not in os.environ:
    # Windows does not come with a standard IANA time zone database,
    # but the ORC lib requires it. Set the TZDIR environment variable
    # to the tzdata module's data directory.
    import tzdata

    os.environ["TZDIR"] = os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo")

from .enums import *
from .errors import *
from .reader import Column, Reader, Stripe
from .writer import Writer
from .typedescription import *


__version__ = "0.4.0"
