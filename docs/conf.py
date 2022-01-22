# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import sys
import os

sys.path[0:0] = [os.path.abspath("..")]

# For read-the-docs: mocking the _pyorc module.
from unittest.mock import MagicMock


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
        # For pyorc
        if name == "typedescription":
            return object
        elif name == "reader":
            return object
        elif name == "writer":
            return object
        elif name == "stripe":
            return object
        elif name == "_orc_version":
            return lambda: "0.0.0-DUMMY"
        # For zoneinfo
        elif name == "ZoneInfo":
            return lambda key: object


MOCK_MODULES = ["src.pyorc._pyorc", "pyorc", "pyorc._pyorc", "zoneinfo"]
sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)

import src.pyorc as pyorc

sys.modules["pyorc"] = pyorc


# -- Project information -----------------------------------------------------

project = "PyORC"
copyright = "2019-2022, noirello"
author = "noirello"

# The full version, including alpha/beta/rc tags
release = pyorc.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.doctest"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

master_doc = "index"
