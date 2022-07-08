"""Table definitions for the comet datasets and states."""
from chimedb.core.orm import base_model, JSONDictField

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

    def __repr__(self):
        return f"<DatasetStateType: {self.name}>"


class DatasetState(base_model):
    """Model for datasetstate table."""

    id = peewee.FixedCharField(max_length=32, primary_key=True)
    type = peewee.ForeignKeyField(DatasetStateType, null=True)
    data = JSONDictField()
    time = peewee.DateTimeField()


class Dataset(base_model):
    """Model for dataset table."""

    id = peewee.FixedCharField(max_length=32, primary_key=True)
    root = peewee.BooleanField()
    state = peewee.ForeignKeyField(DatasetState)
    time = peewee.DateTimeField()
    base_dset = peewee.ForeignKeyField("self", null=True)
