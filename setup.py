"""Tables and tools for CHIME datasets and states."""
from setuptools import setup

import codecs
import os
import re
import versioneer


setup(
    name="chimedb.dataset",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
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
