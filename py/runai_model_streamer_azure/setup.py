# Name and dependencies are in pyproject.toml; this file keeps what needs code (the version
# from PACKAGE_VERSION, the platform wheel tag, the bundled .so and its check) and the license
# files.
import os
from setuptools import setup, find_packages
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel


VERSION = os.getenv("PACKAGE_VERSION", "0.0.0")
LIB = "libstreamerazure.so"


class bdist_wheel(_bdist_wheel):
    """Force platform-specific wheel without Python ABI"""

    def finalize_options(self):
        super().finalize_options()
        # create platform-specific wheel (platlib)
        self.root_is_pure = False

    def get_tag(self):
        _, _, plat = super().get_tag()
        # wheel tag, e.g. py3-none-linux_x86_64
        return "py3", "none", plat


def assert_lib_exists():
    lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), LIB)
    if os.path.islink(lib_path):
        target_path = os.path.realpath(lib_path)
        if not os.path.exists(target_path):
            raise FileNotFoundError(
                f"{target_path} (target of the symlink) not found. Aborting build."
            )
    else:
        if not os.path.exists(lib_path):
            raise FileNotFoundError(f"{lib_path} not found. Aborting build.")


assert_lib_exists()
setup(
    version=VERSION,
    # The wheel ships this directory's NOTICE: the third-party attribution of what its .so links,
    # kept current by the Renovate pipeline. Here rather than PEP 639's [project].license-files,
    # which needs setuptools >= 77 and so Python >= 3.9; the devcontainer builds with Python 3.8.
    license_files=("LICENSE", "NOTICE"),  # attribution: ships NOTICE
    packages=find_packages(),
    data_files=[("/runai_model_streamer/libstreamer/lib/", [LIB])],
    cmdclass={"bdist_wheel": bdist_wheel},
)
