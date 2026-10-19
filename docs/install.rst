Installing
==========

Using pip
---------

For Linux, macOS and Windows, you can simply use pip that will install a wheel
bundled with the required libraries::

    $ pip3 install pyorc

.. note::
    To install on Linux, you need *pip 19.0* or newer. Earlier versions are
    unable to handle the appropriate ``manylinux`` wheels, thus they try to
    install the package from source.

There could be some drawbacks of the bundled libraries in the package, when
used together with other Python modules. If another module is loaded into
the Python runtime besides PyORC that also pre-bundles one of the required
C/C++ libraries but a slightly different version, then the two libraries
will collide, and the interpreter will crash with a segmentation fault at
some point during the execution.

It's easy to run into this situation. For example, ``libprotobuf`` is
one of the required libraries for ORC, and it's quite popular for other
projects as well. To avoid this, you have to make sure that the very same
version of the common library is used by both of the modules, and therefore
you might need to build PyORC from source.


Install from source
-------------------

To install from source, the module requires the Apache ORC C++ Core library.
During the extension build step, the module will build the ORC core library
before building the extension module itself. It requires ``cmake`` -- in
addition to a suitable C++ compiler. The following steps take place during
the ``build_ext`` command:

    1. Downloading the Apache ORC release package.
    2. Extracting the package to a directory named ``deps`` into the
       project's root directory.
    3. Running ``cmake`` to configure the ORC C++ library.
    4. Building the library with ``cmake --build`` and installing it into a
       temporary package directory with ``cmake --install``.
    5. Finally, moving the C++ headers, ORC example files and ORC tools
       to the top level of the ``deps`` directory for the ``setup.py`` and
       tests to find.
    6. Building the C++ extension part of PyORC.

.. note::
    The ``build_ext`` command has an ``--orc-version`` and a ``--source-url``
    parameter for changing the default ORC library version or the base URL
    that the ORC source archive is downloaded from, respectively. It also has
    a ``--skip-orc-build`` flag to skip the ORC library build steps.

    ``pip`` does not pass these parameters to ``build_ext``, so when
    installing with ``pip``, set the ``PYORC_LIB_VERSION`` and
    ``PYORC_SKIP_ORC_BUILD`` environment variables instead.

You also need the ``pybind11`` Python package to be installed before running
the installation, but it's part of the build requirements of the
`pyproject.toml`, so modern pip will install it automatically::

    $ pip3 install .

After the installation completes without errors, you have the module ready
to use.
