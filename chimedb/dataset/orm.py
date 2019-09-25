"""Table definitions for the comet datasets and states."""
# === Start Python 2/3 compatibility
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *  # noqa  pylint: disable=W0401, W0614
from future.builtins.disabled import *  # noqa  pylint: disable=W0401, W0614

# === End Python 2/3 compatibility

from chimedb.core.orm import base_model, JSONDictField

import orjson as json
import peewee

# Logging
# =======

import logging

_logger = logging.getLogger("chimedb")
_logger.addHandler(logging.NullHandler())


# Tables.
# =======


class DatasetStateType(base_model):
    """Model for datasetstatetype table."""

    name = peewee.CharField()


class DatasetState(base_model):
    """Model for datasetstate table."""

    id = peewee.DecimalField(21, 0, primary_key=True)
    type = peewee.ForeignKeyField(DatasetStateType, null=True)
    data = JSONDictField()


class Dataset(base_model):
    """Model for dataset table."""

    id = peewee.DecimalField(21, 0, primary_key=True)
    root = peewee.BooleanField()
    state = peewee.ForeignKeyField(DatasetState)
    time = peewee.DateTimeField()
    base_dset = peewee.ForeignKeyField("self", null=True)


class DatasetAttachedType(base_model):
    """
    Model for datasettypes table.

    Lists which state types are attached to which datasets
    """

    id = peewee.AutoField(primary_key=True)
    dataset_id = peewee.ForeignKeyField(Dataset)
    type = peewee.ForeignKeyField(DatasetStateType)


class DatasetCurrentState(base_model):
    """Model for datasetcurrentstate table."""

    state = peewee.ForeignKeyField(DatasetState)
    time = peewee.DateTimeField()
