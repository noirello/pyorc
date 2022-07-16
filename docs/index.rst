.. PyORC documentation master file, created by
   sphinx-quickstart on Tue Nov 12 22:14:39 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PyORC's documentation
*********************

PyORC is a Python module for reading and writing `Apache ORC`_ file format.
It uses the Apache ORC's Core C++ API under the hood, and provides a similar
interface as the `csv module`_ in the Python standard library.

.. note::
    The module is compatible with Python 3.7 or newer releases.


Features
--------

- Reading ORC files.
- Writing ORC files.
- While using Python's stream/file-like object IO interface.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   tutorial
   api
   changelog


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Contribution
============

Any contributions are welcome. If you would like to help in development fork
or report issue on the project's `GitHub site`_. You can also help in
improving the documentation.

.. _github site: https://github.com/noirello/pyorc
.. _Apache ORC: https://orc.apache.org/
.. _csv module: https://docs.python.org/3/library/csv.html
