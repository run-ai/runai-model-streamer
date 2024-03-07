import os
from setuptools import setup, find_packages

version = os.getenv("PACKAGE_VERSION", "0.0.0-devel")

setup(
    name="runai-streamer",
    version=version,
    packages=find_packages(),
    package_data={"streamer": ["libstreamer/lib/libstreamer.so"]},
)
