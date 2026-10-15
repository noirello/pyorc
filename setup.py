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
import re

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
        "protobuf",
        "lz4",
        "zstd",
        "zlib",
        "snappy",
    ]
else:
    LIBS = ["orc", "protobuf", "lz4", "zstd", "z", "snappy", "pthread"]

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
        self.orc_version = "2.3.1"
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

    def _patch_protobuf_version(self, version, hash) -> None:
        file_path = (
            self.output_dir
            / f"orc-{self.orc_version}"
            / "cmake_modules"
            / "ThirdpartyToolchain.cmake"
        )
        content = file_path.read_text("utf-8")
        # Replace protobuf version.
        content = re.sub(
            r'set\(PROTOBUF_VERSION "[^"]*"\)',
            f'set(PROTOBUF_VERSION "{version}")',
            content,
        )
        # Protobuf 23 and later moved the CMake entry point from the "cmake"
        # subfolder to the repository root.
        source_subdir = (
            "cmake" if int(version.split(".")[0]) < 23 else "."
        )
        content = re.sub(
            r'(fetchcontent_declare\(Protobuf\b.*?)\n(\s*)SOURCE_SUBDIR "[^"]*"',
            rf'\1\n\g<2>SOURCE_SUBDIR "{source_subdir}"',
            content,
            count=1,
            flags=re.DOTALL,
        )
        if int(version.split(".")[0]) >= 23:
            # Protobuf 23 and later depends on Abseil, which has to be built
            # (and vendored) along with protobuf.
            content = content.replace(
                "    set(protobuf_INSTALL OFF)\n",
                "    set(protobuf_INSTALL OFF)\n"
                "    set(protobuf_FORCE_FETCH_DEPENDENCIES ON)\n",
            )
        content = re.sub(
            r"(fetchcontent_declare\(Protobuf\b.*?)\n\s*URL_HASH SHA256=\w+",
            r"\1",
            content,
            count=1,
            flags=re.DOTALL
        )
        # Replace protobuf hash when it's set, otherwise the entire url hash line.
        if hash:
            content = re.sub(
                r"(fetchcontent_declare\(Protobuf\b.*?\n\s*URL_HASH SHA256=)\w+",
                rf"\g<1>{hash}",
                content,
                count=1,
                flags=re.DOTALL
            )
        else:
            content = re.sub(
                r"(fetchcontent_declare\(Protobuf\b.*?)\n\s*URL_HASH SHA256=\w+",
                r"\1",
                content,
                count=1,
                flags=re.DOTALL
            )
        # Newer protobuf creates these aliases itself, avoid clash with already
        # defined ones.
        content = content.replace(
            "      add_library(protobuf::libprotobuf ALIAS libprotobuf)\n"
            "      add_executable(protobuf::protoc ALIAS protoc)\n",
            "      if(NOT TARGET protobuf::libprotobuf)\n"
            "        add_library(protobuf::libprotobuf ALIAS libprotobuf)\n"
            "      endif()\n"
            "      if(NOT TARGET protobuf::protoc)\n"
            "        add_executable(protobuf::protoc ALIAS protoc)\n"
            "      endif()\n",
        )
        content = content.replace(
            "        install(TARGETS libprotobuf\n"
            "                EXPORT orc_targets\n",
            "        install(TARGETS libprotobuf\n"
        )
        file_path.write_text(content, "utf-8")
        logging.info(f"Overrode protobuf version to: {version} (hash: {hash})")

    @staticmethod
    def _get_build_envs() -> dict[str, str]:
        env = os.environ.copy()

        if sys.platform != "win32":
            env["CFLAGS"] = "-fPIC"
            env["CXXFLAGS"] = "-fPIC"

        return env

    def _build_with_cmake(self) -> tuple[pathlib.Path, pathlib.Path]:
        build_type = "DEBUG" if self.debug else "RELEASE"

        cmake_args = [
            f"-DCMAKE_BUILD_TYPE={build_type}",
            "-DBUILD_JAVA=OFF",
            "-DBUILD_LIBHDFSPP=OFF",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
            "-DSTOP_BUILD_ON_WARNING=OFF",
        ]
        if sys.platform == "win32":
            cmake_args.append("-Dprotobuf_MSVC_STATIC_RUNTIME=OFF")
            if self.debug:
                cmake_args.append("-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDebugDLL")
            else:
                cmake_args.append("-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDLL")
        if os.getenv("PYORC_OVERRIDDEN_PROTOBUF_VERSION"):
            cmake_args.append("-DCMAKE_DISABLE_FIND_PACKAGE_Protobuf=ON")
        if not self.debug or sys.platform == "win32":
            # Skip building tools and tests.
            cmake_args.append("-DBUILD_TOOLS=OFF")
            cmake_args.append("-DBUILD_CPP_TESTS=OFF")
        if self.debug:
            cmake_args.append("-Dprotobuf_DEBUG_POSTFIX=")
        env = self._get_build_envs()
        build_dir = self.output_dir / f"orc-{self.orc_version}" / "build"
        pack_dir = (build_dir / "final-package").resolve()
        if not build_dir.exists():
            os.makedirs(build_dir)
        logging.info("Build libraries with cmake")
        cmake_cmd = ["cmake", ".."] + cmake_args
        logging.info(f"Cmake command: {cmake_cmd}")
        subprocess.check_call(cmake_cmd, cwd=build_dir, env=env)

        logging.info("Calling cmake build")
        subprocess.check_call(
            ["cmake", "--build", ".", "--config", build_type,
             "-j", str(os.cpu_count() or 1)],
            cwd=build_dir, env=env,
        )
        logging.info(f"Calling cmake install with prefix: {pack_dir}")
        subprocess.check_call(
            ["cmake", "--install", ".", "--config", build_type,
             "--prefix", str(pack_dir)],
            cwd=build_dir, env=env,
        )

        return build_dir, pack_dir

    def _build_orc_lib(self) -> None:
        logging.info("Build ORC C++ Core library")
        build_dir, pack_dir = self._build_with_cmake()
        proto_src_dir = build_dir / "_deps" / "protobuf-src" / "src"
        if not proto_src_dir.exists():
            proto_src_dir = build_dir / "protobuf_ep-prefix" / "src" / "protobuf_ep" / "src"
        logging.info(
            f"Move artifacts from '{pack_dir}' to the '{self.output_dir}' folder"
        )
        try:
            shutil.move(pack_dir / "include", self.output_dir)
            if (proto_src_dir / "google").exists():
                shutil.move(proto_src_dir / "google", self.output_dir / "include")
            lib_dir = (
                "lib64" if (pack_dir / "lib64").exists() else "lib"
            )
            shutil.move(pack_dir / lib_dir, self.output_dir / "lib")

            for ven_lib in (self.output_dir / "lib").glob("*orc_vendored_*"):
                if ven_lib.suffix in (".a", ".lib"):
                    dst_name = ven_lib.name.replace("orc_vendored_", "", 1)
                    ven_lib.replace(ven_lib.with_name(dst_name))
                    logging.info(f"Renamed {ven_lib} to {dst_name}")

            if self._uses_abseil():
                self._vendor_protobuf_dependencies(build_dir)

            if self.debug and not sys.platform.startswith("win32"):
                shutil.move(pack_dir / "bin", self.output_dir)
            shutil.move(
                self.output_dir / f"orc-{self.orc_version}" / "examples",
                self.output_dir,
            )
        except Exception as exc:
            logging.warning(exc)

    @staticmethod
    def _uses_abseil() -> bool:
        version = os.getenv("PYORC_OVERRIDDEN_PROTOBUF_VERSION", "")
        if not version:
            return False
        return int(version.split(".")[0]) >= 23

    def _vendor_protobuf_dependencies(self, build_dir: pathlib.Path) -> None:
        """
        Copy the Abseil and utf8_range headers and static libraries built by
        protobuf next to liborc, because protobuf 23 and later depend on them.
        """
        lib_dir = self.output_dir / "lib"
        patterns = ("libabsl_*.*", "libutf8_*.*", "absl_*.*", "utf8_*.*")
        copied = set[str]()
        for pattern in patterns:
            for lib in build_dir.rglob(pattern):
                if lib.suffix not in (".a", ".lib") or lib.name in copied:
                    continue
                copied.add(lib.name)
                shutil.copy2(lib, lib_dir / lib.name)
        logging.info(f"Copied {len(copied)} protobuf dependency libraries")

        include_dir = self.output_dir / "include"
        # Abseil headers, because protobuf 23+ headers include "absl/...".
        absl_src = build_dir / "_deps" / "absl-src" / "absl"
        if absl_src.is_dir():
            dst = include_dir / "absl"
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(absl_src, dst)
            logging.info("Copied Abseil headers")
        # utf8_range headers, because protobuf 23+ headers include
        # "utf8_validity.h" and "utf8_range.h" unqualified.
        utf8_src = (
            build_dir / "_deps" / "protobuf-src" / "third_party" / "utf8_range"
        )
        if utf8_src.is_dir():
            for hdr in list(utf8_src.glob("*.h")) + list(utf8_src.glob("*.inc")):
                shutil.copy2(hdr, include_dir / hdr.name)
            logging.info("Copied utf8_range headers")

    def _link_protobuf_dependencies(self) -> None:
        """Add the vendored Abseil and utf8_range libraries to the link line."""
        lib_dir = self.output_dir / "lib"
        is_win = sys.platform.startswith("win32")
        names = []
        for lib in sorted(lib_dir.iterdir()):
            stem = lib.name
            if not (stem.startswith("libabsl_") or stem.startswith("libutf8_")
                    or stem.startswith("absl_") or stem.startswith("utf8_")):
                continue
            if not is_win and stem.endswith(".a"):
                names.append(stem[len("lib"):-len(".a")])
            elif is_win and stem.endswith(".lib"):
                names.append(stem[:-len(".lib")])
        if not names:
            return
        logging.info(f"Linking protobuf dependencies: {names}")
        if is_win or sys.platform == "darwin":
            # On Windows the linker does not care about the order and on macOS
            # the linker does not support grouping.
            self.extensions[0].libraries.extend(names)
        else:
            link_args = list(self.extensions[0].extra_link_args or [])
            link_args += ["-Wl,--start-group"]
            link_args += [f"-l{name}" for name in names]
            link_args += ["-Wl,--end-group"]
            self.extensions[0].extra_link_args = link_args

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
            protobuf_hash = os.getenv("PYORC_OVERRIDDEN_PROTOBUF_HASH")
            if protobuf_ver:
                self._patch_protobuf_version(protobuf_ver, protobuf_hash)

            if not orc_lib.exists():
                self._build_orc_lib()

        if self._uses_abseil():
            self._link_protobuf_dependencies()

        if sys.platform.startswith("win32") and self.debug:
            self.extensions[0].libraries = [
                lib if lib != "zlib" else "zlibd"
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
