Installing
==========

Using pip
---------

For Linux and Mac, you can simply use pip that will install a wheel bundled
with the required libraries::

    $ pip3 install pyorc


Install from source
-------------------

To install the module from source you need to build the Apache ORC C++ Core
library with its dependencies first, which requires `cmake` -- in addition of
a suitable C++ complier.

To make that easier, the project's `setup.py` file also contains a
``build_orc`` command that:

    1. Downloads the Apache ORC release package.
    2. Extract the package to a directory named `deps` into the project's
       root directory.
    3. Runs cmake to configure the ORC C++ lbrary.
    4. Runs the ``make package`` command.
    5. Finally, moves the include headers, ORC example files and ORC tools
       to the top level of the `deps` directory for the `setup.py` and tests
       to find.

You also need the `pybind11` Python package to be installed before running
the installation::

    $ pip3 install pybind11
    $ python3 setup.py install

After the installation completes without errors, you have the module ready
to use.
