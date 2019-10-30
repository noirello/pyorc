import io
import os
import sys
import shutil
import subprocess
import urllib.request
import tarfile
from distutils import log

import setuptools
from setuptools import setup, Extension, Command
from setuptools.command.build_ext import build_ext
from setuptools.command.build_clib import build_clib


SOURCES = [
    "_pyorc.cpp",
    "Converter.cpp",
    "PyORCStream.cpp",
    "Reader.cpp",
    "TypeDescription.cpp",
    "Writer.cpp",
]

HEADERS = ["Converter.h", "PyORCStream.h", "Reader.h", "TypeDescription.h", "Writer.h"]


class BuildORCLib(Command):
    description = "run cmake to build ORC C++ Core library"
    user_options = [
        ("orc-version=", None, "the version of the ORC lib"),
        ("output-dir=", None, "the output directory"),
        ("source-url=", None, "the HTTP url for downloading the ORC source"),
    ]

    def initialize_options(self):
        """Set default values for options."""
        self.orc_version = "1.6.0"
        self.output_dir = "deps/"
        self.source_url = "https://www-us.apache.org/dist/orc/"

    def finalize_options(self):
        """Post-process options."""
        pass

    def run(self):
        log.info("Build ORC C++ Core library")
        if not os.path.isdir(
            os.path.join(self.output_dir, "orc-{ver}".format(ver=self.orc_version))
        ):
            self._download_source()
        build_dir = self._build_with_cmake()
        pack_dir = os.path.join(
            build_dir,
            "_CPack_Packages",
            "Linux",  # XXX: Platform independency
            "TGZ",
            "ORC-{ver}-Linux".format(ver=self.orc_version),
        )
        log.info("Move artifacts to the %s folder" % self.output_dir)
        try:
            shutil.move(os.path.join(pack_dir, "include"), self.output_dir)
            shutil.move(os.path.join(pack_dir, "lib"), self.output_dir)
            shutil.move(os.path.join(pack_dir, "bin"), self.output_dir)
            shutil.move(
                os.path.join(
                    self.output_dir,
                    "orc-{ver}".format(ver=self.orc_version),
                    "examples",
                ),
                self.output_dir,
            )
        except:
            pass

    def _download_source(self) -> None:
        tmp_tar = io.BytesIO()
        url = "{url}orc-{ver}/orc-{ver}.tar.gz".format(
            url=self.source_url, ver=self.orc_version
        )
        with urllib.request.urlopen(url) as src:
            log.info("Download ORC release from: %s" % url)
            tmp_tar.write(src.read())
        tmp_tar.seek(0)
        tar_src = tarfile.open(fileobj=tmp_tar, mode="r:gz")
        log.info("Extract archives in: %s" % self.output_dir)
        tar_src.extractall(self.output_dir)
        tar_src.close()

    def _build_with_cmake(self) -> str:
        cmake_args = [
            "-DCMAKE_BUILD_TYPE=DEBUG",
            "-DBUILD_JAVA=OFF",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
        ]
        compiler_flags = ["CFLAGS=-fPIC", "CXXFLAGS=-fPIC"]
        env = os.environ.copy()
        build_dir = os.path.join(
            self.output_dir, "orc-{ver}".format(ver=self.orc_version), "build"
        )
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)
        log.info("Build libraries with cmake")
        env.update([flg.split("=") for flg in compiler_flags])
        cmake_cmd = ["cmake", ".."] + cmake_args
        log.info("Cmake command: %s" % cmake_cmd)
        subprocess.check_call(cmake_cmd, cwd=build_dir, env=env)
        subprocess.check_call(["make", "-j4", "package"], cwd=build_dir, env=env)
        return build_dir


class get_pybind_include:
    """Helper class to determine the pybind11 include path
    The purpose of this class is to postpone importing pybind11
    until it is actually installed, so that the ``get_include()``
    method can be invoked. """

    def __init__(self, user=False):
        self.user = user

    def __str__(self):
        import pybind11

        return pybind11.get_include(self.user)


SOURCES = [os.path.join("src/_pyorc", src) for src in SOURCES]
HEADERS = [os.path.join("src/_pyorc", hdr) for hdr in HEADERS]

LIBS = ["orc", "protobuf", "protoc", "lz4", "zstd", "z", "snappy", "pthread"]

EXT_MODULES = [
    Extension(
        "pyorc._pyorc",
        sources=SOURCES,
        depends=HEADERS,
        libraries=LIBS,
        include_dirs=[
            # Path to pybind11 headers
            get_pybind_include(),
            get_pybind_include(user=True),
            "deps/include/",
        ],
        library_dirs=["deps/lib/"],
        language="c++",
    )
]


def has_flag(compiler, flagname):
    """Return a boolean indicating whether a flag name is supported on
    the specified compiler.
    """
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".cpp") as f:
        f.write("int main (int argc, char **argv) { return 0; }")
        try:
            compiler.compile([f.name], extra_postargs=[flagname])
        except setuptools.distutils.errors.CompileError:
            return False
        finally:
            shutil.rmtree("tmp/", ignore_errors=True)
    return True


def cpp_flag(compiler):
    """Return the -std=c++[11/14/17] compiler flag.
    The newer version is prefered over c++11 (when it is available).
    """
    flags = ["-std=c++17", "-std=c++14", "-std=c++11"]

    for flag in flags:
        if has_flag(compiler, flag):
            return flag

    raise RuntimeError("Unsupported compiler -- at least C++11 support is needed!")


class BuildExt(build_ext):
    """A custom build extension for adding compiler-specific options."""

    c_opts = {"msvc": ["/EHsc"], "unix": []}
    l_opts = {"msvc": [], "unix": []}

    if sys.platform == "darwin":
        darwin_opts = ["-stdlib=libc++", "-mmacosx-version-min=10.7"]
        c_opts["unix"] += darwin_opts
        l_opts["unix"] += darwin_opts

    def build_extensions(self):
        ct = self.compiler.compiler_type
        opts = self.c_opts.get(ct, [])
        link_opts = self.l_opts.get(ct, [])
        if ct == "unix":
            opts.append('-DVERSION_INFO="%s"' % self.distribution.get_version())
            opts.append(cpp_flag(self.compiler))
            opts.append("-fvisibility=hidden")
        elif ct == "msvc":
            opts.append('/DVERSION_INFO=\\"%s\\"' % self.distribution.get_version())
        for ext in self.extensions:
            ext.extra_compile_args = opts
            ext.extra_link_args = link_opts
        build_ext.build_extensions(self)


setup(
    name="pyorc",
    version="0.1.0",
    description="Python module for reading and writing Apache ORC file format.",
    author="noirello",
    author_email="noirello@gmail.com",
    url="https://github.com/noirello/pyorc",
    long_description="",
    license="Apache License, Version 2.0",
    ext_modules=EXT_MODULES,
    package_dir={"pyorc": "src/pyorc"},
    packages=["pyorc"],
    include_package_data=True,
    install_requires=["pybind11>=2.4"],
    setup_requires=["pybind11>=2.4"],
    cmdclass={"build_ext": BuildExt, "build_orc": BuildORCLib},
    keywords=[],
    classifiers=[],
)
