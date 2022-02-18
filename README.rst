PyORC
=====

.. image:: https://dev.azure.com/noirello/pyorc/_apis/build/status/noirello.pyorc?branchName=master
    :target: https://dev.azure.com/noirello/pyorc/_build?definitionId=1
    :alt: Azure Pipelines Status

.. image:: https://codecov.io/gh/noirello/pyorc/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/noirello/pyorc
    :alt: Codecov code coverage

.. image:: https://readthedocs.org/projects/pyorc/badge/?version=latest
    :target: https://pyorc.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Python module for reading and writing `Apache ORC`_ file format. It uses the Apache ORC's Core C++ API
under the hood, and provides a similar interface as the `csv module`_ in the Python standard library.

Supports only Python 3.7 or newer and ORC 1.7.

Features
--------

- Reading ORC files.
- Writing ORC files.
- While using Python's stream/file-like object IO interface.

That sums up quite well the purpose of this project.

Example
-------

Minimal example for reading an ORC file:

.. code:: python

        import pyorc

        with open("./data.orc", "rb") as data:
            reader = pyorc.Reader(data)
            for row in reader:
                print(row)

And another for writing one:

.. code:: python

        import pyorc

        with open("./new_data.orc", "wb") as data:
            with pyorc.Writer(data, "struct<col0:int,col1:string>") as writer:
                writer.write((1, "ORC from Python"))

Contribution
============

Any contributions are welcome. If you would like to help in development fork
or report issue here on Github. You can also help in improving the
documentation.

.. _Apache ORC: https://orc.apache.org/
.. _csv module: https://docs.python.org/3/library/csv.html
