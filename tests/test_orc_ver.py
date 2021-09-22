from collections import namedtuple
import pytest

from pyorc import orc_version, orc_version_info


def test_orc_version():
    assert isinstance(orc_version, str)
    assert len(orc_version.split(".")) == 3


def test_orc_version_info():
    assert isinstance(orc_version_info, tuple)
    assert isinstance(orc_version_info.major, int)
    assert isinstance(orc_version_info.minor, int)
    assert isinstance(orc_version_info.patch, int)
    assert isinstance(orc_version_info.releaselevel, str)
    inf = orc_version_info
    assert (
        orc_version
        == f"{inf.major}.{inf.minor}.{inf.patch}{'-' if inf.releaselevel else ''}{inf.releaselevel}"
    )

