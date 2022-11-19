import io
import os
import platform
import sys
import shutil
import subprocess
import urllib.request
import tarfile
import logging

from setuptools import setup

from pybind11.setup_helpers import Pybind11Extension, build_ext


logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


SOURCES = [
    "_pyorc.cpp",
    "Converter.cpp",
    "PyORCStream.cpp",
    "Reader.cpp",
    "SearchArgument.cpp",
    "Writer.cpp",
]

HEADERS = ["Converter.h", "PyORCStream.h", "Reader.h", "SearchArgument.h", "Writer.h"]

if sys.platform.startswith("win32"):
    LIBS = [
        "orc",
        "libprotobuf",
        "libprotoc",
        "lz4",
        "zstd_static",
        "zlibstatic",
        "snappy",
    ]
else:
    LIBS = ["orc", "protobuf", "protoc", "lz4", "zstd", "z", "snappy", "pthread"]

LIBS = os.getenv("PYORC_LIBRARIES", ",".join(LIBS)).split(",")

EXT_MODULES = [
    Pybind11Extension(
        "pyorc._pyorc",
        sources=[os.path.join("src", "_pyorc", src) for src in SOURCES],
        depends=[os.path.join("src", "_pyorc", hdr) for hdr in HEADERS],
        libraries=LIBS,
        include_dirs=[os.path.join("deps", "include")],
        library_dirs=[os.path.join("deps", "lib")],
    )
]


class BuildExt(build_ext):
    """
    A custom build extension for build ORC Core library and handling
    debug build on Windows.
    """

    user_options = build_ext.user_options + [
        ("orc-version=", None, "the version of the ORC C++ Core library"),
        ("output-dir=", None, "the output directory"),
        ("source-url=", None, "the HTTP url for downloading the ORC source"),
        ("download-only", None, "just download and extract the ORC source"),
        ("skip-orc-build", None, "skip building ORC C++ Core library"),
    ]

    boolean_options = build_ext.boolean_options + [
        "download-only",
        "skip-orc-build",
    ]

    def initialize_options(self) -> None:
        """Set default values for options."""
        super().initialize_options()
        self.orc_version = "1.7.7"
        self.output_dir = "deps"
        self.source_url = "https://archive.apache.org/dist/orc/"
        self.download_only = False
        self.skip_orc_build = False

    def finalize_options(self) -> None:
        # Workaround to set options with environment variables,
        # because pip fails to pass parameters to build_ext.
        if os.getenv("PYORC_DEBUG", 0):
            self.debug = True
        if os.getenv("PYORC_SKIP_ORC_BUILD", 0):
            self.skip_orc_build = True
        super().finalize_options()

    def _download_source(self) -> None:
        tmp_tar = io.BytesIO()
        url = "{url}orc-{ver}/orc-{ver}.tar.gz".format(
            url=self.source_url, ver=self.orc_version
        )
        with urllib.request.urlopen(url) as src:
            logging.info("Download ORC release from: %s" % url)
            tmp_tar.write(src.read())
        tmp_tar.seek(0)
        tar_src = tarfile.open(fileobj=tmp_tar, mode="r:gz")
        logging.info("Extract archives in: %s" % self.output_dir)
        tar_src.extractall(self.output_dir)
        tar_src.close()

    @staticmethod
    def _get_build_envs() -> dict:
        env = os.environ.copy()

        env["CFLAGS"] = "-fPIC"
        env["CXXFLAGS"] = "-fPIC"

        return env

    def _build_with_cmake(self) -> str:

        build_type = "DEBUG" if self.debug else "RELEASE"

        cmake_args = [
            f"-DCMAKE_BUILD_TYPE={build_type}",
            "-DBUILD_JAVA=OFF",
            "-DBUILD_LIBHDFSPP=OFF",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
        ]
        if sys.platform == "win32":
            cmake_args.append("-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded")
        if not self.debug or sys.platform == "win32":
            # Skip building tools and tests.
            cmake_args.append("-DBUILD_TOOLS=OFF")
            cmake_args.append("-DBUILD_CPP_TESTS=OFF")
        env = self._get_build_envs()
        build_dir = os.path.join(
            self.output_dir, "orc-{ver}".format(ver=self.orc_version), "build"
        )
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)
        logging.info("Build libraries with cmake")
        cmake_cmd = ["cmake", ".."] + cmake_args
        logging.info("Cmake command: %s" % cmake_cmd)
        subprocess.check_call(cmake_cmd, cwd=build_dir, env=env)
        if sys.platform == "win32":
            subprocess.check_call(
                [
                    "cmake",
                    "--build",
                    ".",
                    "--config",
                    build_type,
                    "--target",
                    "PACKAGE",
                ],
                cwd=build_dir,
                env=env,
            )
        else:
            j_flag = f"-j{os.cpu_count() or 1}"
            subprocess.check_call(["make", j_flag, "package"], cwd=build_dir, env=env)
        return build_dir

    def _build_orc_lib(self):
        logging.info("Build ORC C++ Core library")
        build_dir = self._build_with_cmake()
        plat = (
            sys.platform.title()
            if not sys.platform.startswith("win32")
            # Change platform title on Windows depending on arch (32/64bit)
            else sys.platform.title().replace("32", platform.architecture()[0][:2])
        )
        pack_dir = os.path.join(
            build_dir,
            "_CPack_Packages",
            plat,
            "TGZ",
            f"ORC-{self.orc_version}-{plat}",
        )
        logging.info(
            "Move artifacts from '%s' to the '%s' folder" % (pack_dir, self.output_dir)
        )
        try:
            shutil.move(os.path.join(pack_dir, "include"), self.output_dir)
            shutil.move(os.path.join(pack_dir, "lib"), self.output_dir)
            if not sys.platform.startswith("win32"):
                shutil.move(os.path.join(pack_dir, "bin"), self.output_dir)
            shutil.move(
                os.path.join(
                    self.output_dir,
                    f"orc-{self.orc_version}",
                    "examples",
                ),
                self.output_dir,
            )
        except Exception as exc:
            logging.warning(exc)

    def build_extensions(self):
        if not self.skip_orc_build:
            orc_lib = os.path.join(
                self.output_dir,
                "lib",
                "orc.lib" if sys.platform.startswith("win32") else "liborc.a",
            )
            if not os.path.isdir(
                os.path.join(self.output_dir, "orc-{ver}".format(ver=self.orc_version))
            ):
                self._download_source()

            if self.download_only:
                logging.info("Only downloaded the ORC library source. Skip build_ext")
                return

            if not os.path.exists(orc_lib):
                self._build_orc_lib()

        if sys.platform.startswith("win32") and self.debug:
            self.extensions[0].libraries = [
                lib if lib != "zlibstatic" else "zlibstaticd"
                for lib in self.extensions[0].libraries
            ]
        super().build_extensions()


with open("README.rst") as file:
    LONG_DESC = file.read()

# Get version number from the module's __init__.py file.
with open(os.path.join(".", "src", "pyorc", "__init__.py")) as src:
    VER = [
        line.split('"')[1] for line in src.readlines() if line.startswith("__version__")
    ][0]

setup(
    name="pyorc",
    version=VER,
    description="Python module for reading and writing Apache ORC file format.",
    author="noirello",
    author_email="noirello@gmail.com",
    url="https://github.com/noirello/pyorc",
    long_description=LONG_DESC,
    long_description_content_type="text/x-rst",
    license="Apache License, Version 2.0",
    ext_modules=EXT_MODULES,
    package_dir={"pyorc": "src/pyorc"},
    packages=["pyorc"],
    package_data={"pyorc": ["py.typed", "_pyorc.pyi"]},
    include_package_data=True,
    cmdclass={"build_ext": BuildExt},
    keywords=["python3", "orc", "apache-orc"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
    install_requires=[
        'tzdata >= 2020.5 ; sys_platform == "win32"',
        'backports.zoneinfo >= 0.2.1 ; python_version < "3.9"',
    ],
)
