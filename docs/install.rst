Installing
==========

Using pip
---------

For Linux and Mac, you can simply use pip that will install a wheel bundled
with the required libraries::

    $ pip3 install pyorc

.. note::
    To install on Linux, you need *pip 19.0* or newer. Earlier versions are
    unable to handle the ``manylinux2010`` wheels, thus they try to install
    the package from source.

There could be some drawbacks of the bundled libraries in the package, when
using together with other Python modules. If another module is loaded into
the Python runtime besides PyORC that also pre-bundles one of the required
C/C++ libraries but a slightly different version, then the two libraries
will collide and the interpreter will crash with segmentation fault at some
point during the execution.

It's easy to run into this situation. For example, ``libprotobuf`` is
one of required library for ORC and it's quite popular for other projects
as well. To avoid this, you have to make sure that the very same version
of the common library is used by both of the modules and therefore 
you might need to build PyORC from source.


Install from source
-------------------

To install the module from source you need to build the Apache ORC C++ Core
library with its dependencies first, which requires `cmake` -- in addition of
a suitable C++ complier. Without it, the standard ``setup.py install``
command will fail (more likely with the error of missing the ``orc/OrcFile.hh``
header file).

To make that easier, the project's `setup.py` file also contains a
``build_orc`` command that:

    1. Downloads the Apache ORC release package.
    2. Extracts the package to a directory named `deps` into the project's
       root directory.
    3. Runs cmake to configure the ORC C++ library.
    4. Runs the ``make package`` command.
    5. Finally, moves the include headers, ORC example files and ORC tools
       to the top level of the `deps` directory for the `setup.py` and tests
       to find.

.. note::
    The ``build_orc`` command has a ``--orc-version`` and a ``--source-url``
    parameter for changing the default ORC library version or the URL of the
    source zip to download respectively.

You also need the `pybind11` Python package to be installed before running
the installation::

    $ python3 setup.py build_orc
    $ pip3 install pybind11
    $ python3 setup.py install

After the installation completes without errors, you have the module ready
to use.
