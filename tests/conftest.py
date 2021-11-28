import os
import tempfile

import pytest


@pytest.fixture
def output_file():
    testfile = tempfile.NamedTemporaryFile(
        mode="wb", delete=False, prefix="pyorc_", suffix=".orc"
    )
    yield testfile
    if not testfile.closed:
        testfile.close()
    os.remove(testfile.name)


class NullValue:
    _instance = None

    def __new__(cls):
        if cls._instance is not None:
            return cls._instance
        cls._instance = super().__new__(cls)
        return cls._instance
