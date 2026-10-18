import io
import os
import subprocess
import sys
import sysconfig
import threading
import time

from typing import TypeAlias

import pytest

import pyorc

NTHREADS = 10
SCHEMA = "struct<id:int,name:string,tags:array<string>,attrs:map<string,int>>"

ROWTYPE: TypeAlias = tuple[int, str, list[str], dict[str, int]]


requires_free_threading = pytest.mark.skipif(
    not bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
    reason="Needs a free-threaded Python build",
)


def run_concurrently(worker_func, nthreads, args=(), kwargs={}):
    """Start `nthreads` threads together; re-raise the first error."""
    barrier = threading.Barrier(nthreads)
    errors = []

    def wrapper(*args, **kwargs):
        try:
            barrier.wait()
            worker_func(*args, **kwargs)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=wrapper, args=args, kwargs=kwargs)
        for _ in range(nthreads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]


def make_rows(seed: int, count: int = 2000) -> list[ROWTYPE]:
    return [
        (
            seed * count + i,
            f"name-{seed}-{i}",
            [f"t{j}" for j in range(i % 4)],
            {f"k{j}": j for j in range(i % 3)},
        )
        for i in range(count)
    ]


class SlowBytesIO(io.BytesIO):
    """For simulating slow(er) file read (for potential thread switch)."""

    def read(self, *args):
        data = super().read(*args)
        time.sleep(0.002)
        return data


@requires_free_threading
@pytest.mark.skipif(
    os.environ.get("PYTHON_GIL") == "1", reason="GIL forced on by environment"
)
def test_import_keeps_gil_disabled():
    env = {key: val for key, val in os.environ.items() if key != "PYTHON_GIL"}
    code = "import pyorc, sys; print(sys._is_gil_enabled())"
    out = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "False", out.stderr


@requires_free_threading
@pytest.mark.parametrize("file_class", [io.BytesIO, SlowBytesIO])
def test_shared_reader_is_rejected_not_corrupted(file_class):
    buf = io.BytesIO()
    input_rows = make_rows(0, 1000)
    with pyorc.Writer[ROWTYPE](buf, SCHEMA, batch_size=128) as writer:
        writer.writerows(input_rows)
    reader = pyorc.Reader[ROWTYPE](file_class(buf.getvalue()), batch_size=100)
    result: list[ROWTYPE] = []
    errors: list[Exception] = []

    def read_rows():
        try:
            for row in reader:
                result.append(row)
        except RuntimeError as err:
            errors.append(err)

    run_concurrently(read_rows, NTHREADS)
    # Whoever got in read everything exactly once; the rest were rejected.
    assert sorted(result) == sorted(input_rows)
    assert all("Concurrent" in str(exc) for exc in errors)


@requires_free_threading
def test_shared_writer_is_rejected_not_corrupted():
    buf = io.BytesIO()
    written: list[int] = []

    def write_rows():
        idx = threading.get_ident()
        for i in range(500):
            value = idx * 1000 + i
            try:
                writer.write((value, "n", [], {}))
            except RuntimeError:
                continue
            written.append(value)

    with pyorc.Writer[ROWTYPE](buf, SCHEMA, batch_size=64) as writer:
        run_concurrently(write_rows, NTHREADS)
    buf.seek(0)
    assert sorted(row[0] for row in pyorc.Reader[ROWTYPE](buf)) == sorted(written)


def test_parallel_independent_write_and_read():
    results = {}

    def work():
        idx = threading.get_ident()
        input_rows = make_rows(idx)
        buf = io.BytesIO()
        with pyorc.Writer[ROWTYPE](buf, SCHEMA, batch_size=128) as writer:
            writer.writerows(input_rows)
        buf.seek(0)
        results[idx] = (input_rows, list(pyorc.Reader[ROWTYPE](buf, batch_size=100)))

    run_concurrently(work, NTHREADS)
    for val in results.values():
        rows, read_back = val
        assert read_back == rows


def test_parallel_read_of_same_content():
    rows = make_rows(0)
    buf = io.BytesIO()
    with pyorc.Writer(buf, SCHEMA) as writer:
        writer.writerows(rows)
    content = buf.getvalue()
    results = {}

    def work():
        idx = threading.get_ident()
        # One file object and one Reader per thread.
        reader = pyorc.Reader(io.BytesIO(content), batch_size=64)
        results[idx] = [row for row in reader]

    run_concurrently(work, NTHREADS)
    assert all(val == rows for val in results.values())


@requires_free_threading
def test_mutating_containers_while_writing_does_not_crash():
    """
    Mutating a list/dict while another thread writes it is unsupported: the
    writer may raise an exception or write unspecified values. It must not
    crash the interpreter, and the library must keep working afterwards.
    """
    base_list = [f"item-{i}" for i in range(400)]
    shared_list = list(base_list)
    shared_map = {f"key-{i}": i for i in range(400)}
    stop = threading.Event()

    def mutator():
        n = 0
        while not stop.is_set():
            n += 1
            shared_list.clear()
            shared_list.extend(base_list)
            # fresh key objects, so the old ones are freed mid-write
            shared_map.clear()
            shared_map.update({f"key-{i}-{n}": i for i in range(400)})

    def writer_work():
        buf = io.BytesIO()
        try:
            with pyorc.Writer(buf, SCHEMA, batch_size=64) as writer:
                for i in range(300):
                    writer.write((i, "n", shared_list, shared_map))
        except (IndexError, RuntimeError, ValueError, TypeError):
            pass  # unsupported usage may fail cleanly

    thread = threading.Thread(target=mutator)
    thread.start()
    try:
        run_concurrently(writer_work, nthreads=4)
    finally:
        stop.set()
        thread.join()
