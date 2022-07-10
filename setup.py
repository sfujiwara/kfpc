from setuptools import find_packages
from setuptools import setup

setup(
    name="kfpc",
    version="0.0.1",
    packages=find_packages(),
    package_data={"kfpc": ["specifications/*.yaml"]},
)
