"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the default_python project.
"""
from setuptools import setup
import deus_lib

setup(
    name="deus_lib",
    version=deus_lib.__version__,
    author_email="dataavengers@deus.ai",
    description="Package for deus",
    packages=['deus_lib','deus_lib.bronze_factory','deus_lib.utils', 'deus_lib.great_expectations', 'deus_lib.abstract','deus_lib.landing_factory', 'deus_lib.silver_factory'],
)