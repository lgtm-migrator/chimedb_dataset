"""Tables and tools for CHIME datasets and states."""
from setuptools import setup

import codecs
import os
import re

# Get the version from __init__.py without having to import it.
def _get_version():
    with codecs.open(
        os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "chimedb",
            "dataset",
            "__init__.py",
        ),
        "r",
    ) as init_py:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", init_py.read(), re.M
        )

        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


setup(
    name="chimedb.dataset",
    version=_get_version(),
    packages=["chimedb.dataset"],
    zip_safe=False,
    install_requires=[
        "chimedb @ git+https://github.com/chime-experiment/chimedb.git",
        "peewee >= 3.10",
        "future",
    ],
    author="CHIME collaboration",
    author_email="rick@phas.ubc.ca",
    description="CHIME dataset (comet) ORM",
    license="GPL v3.0",
    url="https://github.com/chime-experiment/chimedb_dataset",
)
