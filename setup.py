import io
import os
import platform
import sys
import shutil
import subprocess
import urllib.request
import tarfile
import logging

from setuptools import setup, Command

from pybind11.setup_helpers import Pybind11Extension, build_ext


logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


class BuildORCLib(Command):
    description = "run cmake to build ORC C++ Core library"
    user_options = [
        ("orc-version=", None, "the version of the ORC lib"),
        ("output-dir=", None, "the output directory"),
        ("source-url=", None, "the HTTP url for downloading the ORC source"),
        ("build-type=", None, "set build type for ORC lib"),
        ("download-only=", None, "just download and extract the ORC source"),
    ]

    def initialize_options(self):
        """Set default values for options."""
        self.orc_version = "1.6.6"
        self.output_dir = "deps"
        self.source_url = "https://www-us.apache.org/dist/orc/"
        self.build_type = "debug"
        self.download_only = False

    def finalize_options(self):
        """Post-process options."""
        pass

    def run(self):
        if not os.path.isdir(
            os.path.join(self.output_dir, "orc-{ver}".format(ver=self.orc_version))
        ):
            self._download_source()
        if not self.download_only:
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
                "ORC-{ver}-{plat}".format(ver=self.orc_version, plat=plat),
            )
            logging.info(
                "Move artifacts from '%s' to the '%s' folder"
                % (pack_dir, self.output_dir)
            )
            try:
                shutil.move(os.path.join(pack_dir, "include"), self.output_dir)
                shutil.move(os.path.join(pack_dir, "lib"), self.output_dir)
                if not sys.platform.startswith("win32"):
                    shutil.move(os.path.join(pack_dir, "bin"), self.output_dir)
                shutil.move(
                    os.path.join(
                        self.output_dir,
                        "orc-{ver}".format(ver=self.orc_version),
                        "examples",
                    ),
                    self.output_dir,
                )
            except Exception as exc:
                logging.warning(exc)

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

    def _build_with_cmake(self) -> str:
        cmake_args = [
            "-DCMAKE_BUILD_TYPE={0}".format(self.build_type.upper()),
            "-DBUILD_JAVA=OFF",
            "-DBUILD_LIBHDFSPP=OFF",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
        ]
        if sys.platform == "win32":
            cmake_args.extend(
                [
                    "-DBUILD_CPP_TESTS=OFF",
                    "-DBUILD_TOOLS=OFF",
                    "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded",
                ]
            )
        compiler_flags = ["CFLAGS=-fPIC", "CXXFLAGS=-fPIC"]
        env = os.environ.copy()
        build_dir = os.path.join(
            self.output_dir, "orc-{ver}".format(ver=self.orc_version), "build"
        )
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)
        logging.info("Build libraries with cmake")
        env.update([flg.split("=") for flg in compiler_flags])
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
                    self.build_type,
                    "--target",
                    "PACKAGE",
                ],
                cwd=build_dir,
                env=env,
            )
        else:
            subprocess.check_call(["make", "-j4", "package"], cwd=build_dir, env=env)
        return build_dir


SOURCES = ["_pyorc.cpp", "Converter.cpp", "PyORCStream.cpp", "Reader.cpp", "Writer.cpp"]
HEADERS = ["Converter.h", "PyORCStream.h", "Reader.h", "Writer.h"]

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
    """A custom build extension for handling debug build on Windows"""

    def build_extensions(self):
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
    license="Apache License, Version 2.0",
    ext_modules=EXT_MODULES,
    package_dir={"pyorc": "src/pyorc"},
    packages=["pyorc"],
    include_package_data=True,
    cmdclass={"build_ext": BuildExt, "build_orc": BuildORCLib},
    keywords=["python3", "orc", "apache-orc"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
    install_requires=['tzdata >= 2020.5 ; sys_platform == "win32"'],
)
