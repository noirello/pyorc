PyORC
=====

.. image:: https://dev.azure.com/noirello/pyorc/_apis/build/status/noirello.pyorc?branchName=master
    :target: https://dev.azure.com/noirello/pyorc/_build?definitionId=1
    :alt: Azure Pipelines Status

.. image:: https://codecov.io/gh/noirello/pyorc/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/noirello/pyorc
    :alt: Codecov code coverage

Python module for reading and writing `Apache ORC`_ file format. It uses the Apache ORC's Core C++ API
under the hood, and provides a similar interface as the `csv module`_ in the Python standard library.

Supports only Python 3.6 or newer and ORC 1.6.0.

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


.. _Apache ORC: https://orc.apache.org/
.. _csv module: https://docs.python.org/3/library/csv.html
