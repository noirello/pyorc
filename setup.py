import io
import os
import pathlib
import platform
import sys
import shutil
import subprocess
import urllib.request
import tarfile
import logging
import fileinput

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

HEADERS = [
    "Converter.h",
    "PyORCStream.h",
    "Reader.h",
    "SearchArgument.h",
    "Writer.h",
    "verguard.h",
]

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
        self.orc_version = "2.1.4"
        self.output_dir = pathlib.Path("deps")
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
        self.orc_version = os.getenv("PYORC_LIB_VERSION", self.orc_version)
        super().finalize_options()

    def _download_source(self) -> None:
        tmp_tar = io.BytesIO()
        url = f"{self.source_url}orc-{self.orc_version}/orc-{self.orc_version}.tar.gz"
        with urllib.request.urlopen(url) as src:
            logging.info(f"Download ORC release from: {url}")
            tmp_tar.write(src.read())
        tmp_tar.seek(0)
        tar_src = tarfile.open(fileobj=tmp_tar, mode="r:gz")
        logging.info(f"Extract archives in: {self.output_dir}")
        tar_src.extractall(self.output_dir)
        tar_src.close()

    def _patch_protobuf_version(self, version) -> None:
        file_path = (
            self.output_dir
            / f"orc-{self.orc_version}"
            / "cmake_modules"
            / "ThirdpartyToolchain.cmake"
        )
        with fileinput.input(file_path, inplace=True, encoding="utf-8") as cmake_file:
            for line in cmake_file:
                if "set(PROTOBUF_VERSION " in line:
                    line = f'set(PROTOBUF_VERSION "{version}")\n'
                print(line, end="")
        logging.info(f"Overrode protobuf version to: {version}")

    @staticmethod
    def _get_build_envs() -> dict:
        env = os.environ.copy()

        if sys.platform != "win32":
            env["CFLAGS"] = "-fPIC"
            env["CXXFLAGS"] = "-fPIC"

        return env

    def _build_with_cmake(self) -> pathlib.Path:
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
        build_dir = self.output_dir / f"orc-{self.orc_version}" / "build"
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)
        logging.info("Build libraries with cmake")
        cmake_cmd = ["cmake", ".."] + cmake_args
        logging.info(f"Cmake command: {cmake_cmd}")
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

    def _build_orc_lib(self) -> None:
        logging.info("Build ORC C++ Core library")
        build_dir = self._build_with_cmake()
        plat = (
            sys.platform.title()
            if not sys.platform.startswith("win32")
            # Change platform title on Windows depending on arch (32/64bit)
            else sys.platform.title().replace("32", platform.architecture()[0][:2])
        )
        pack_dir = (
            build_dir
            / "_CPack_Packages"
            / plat
            / "TGZ"
            / f"ORC-{self.orc_version}-{plat}"
        )
        proto_src_dir = build_dir / "protobuf_ep-prefix" / "src" / "protobuf_ep" / "src"
        logging.info(
            f"Move artifacts from '{pack_dir}' to the '{self.output_dir}' folder"
        )
        try:
            shutil.move(pack_dir / "include", self.output_dir)
            shutil.move(proto_src_dir / "google", self.output_dir / "include")
            lib_dir = (
                "lib64" if os.path.exists(os.path.join(pack_dir, "lib64")) else "lib"
            )
            shutil.move(pack_dir / lib_dir, self.output_dir / "lib")
            if self.debug and not sys.platform.startswith("win32"):
                shutil.move(pack_dir / "bin", self.output_dir)
            shutil.move(
                self.output_dir / f"orc-{self.orc_version}" / "examples",
                self.output_dir,
            )
        except Exception as exc:
            logging.warning(exc)

    def get_version_macros(self):
        parts = self.orc_version.split(".")
        return (
            ("ORC_VERSION_MAJOR", int(parts[0])),
            ("ORC_VERSION_MINOR", int(parts[1])),
            ("ORC_VERSION_PATCH", int(parts[2])),
        )

    def build_extensions(self):
        if not self.skip_orc_build:
            orc_lib = (
                self.output_dir
                / "lib"
                / ("orc.lib" if sys.platform.startswith("win32") else "liborc.a")
            )
            if not os.path.isdir(self.output_dir / f"orc-{self.orc_version}"):
                self._download_source()

            if self.download_only:
                logging.info("Only downloaded the ORC library source. Skip build_ext")
                return

            protobuf_ver = os.getenv("PYORC_OVERRIDDEN_PROTOBUF_VERSION")
            if protobuf_ver:
                self._patch_protobuf_version(protobuf_ver)

            if not os.path.exists(orc_lib):
                self._build_orc_lib()

        if sys.platform.startswith("win32") and self.debug:
            self.extensions[0].libraries = [
                lib if lib != "zlibstatic" else "zlibstaticd"
                for lib in self.extensions[0].libraries
            ]
        self.extensions[0].define_macros.extend(self.get_version_macros())
        super().build_extensions()


CURRDIR = pathlib.Path(__file__).resolve().parent
with open(CURRDIR / "README.rst") as file:
    LONG_DESC = file.read()

# Get version number from the module's __init__.py file.
with open(CURRDIR / "src" / "pyorc" / "__init__.py") as src:
    VER = [
        line.split('"')[1] for line in src.readlines() if line.startswith("__version__")
    ][0]

setup(
    name="pyorc",
    version=VER,
    ext_modules=EXT_MODULES,
    package_dir={"pyorc": "src/pyorc"},
    packages=["pyorc"],
    package_data={"pyorc": ["py.typed", "_pyorc.pyi"]},
    include_package_data=True,
    cmdclass={"build_ext": BuildExt},
)
