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