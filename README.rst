PyORC
=====

Python module for reading and writing `Apache ORC`_ file format. It uses the Apache ORC's Core C++ API.

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
                writer.write({"col0": 1, "col1": "ORC from Python"})


.. _Apache ORC: https://orc.apache.org/