"""Functions to write comet datasets and states to the database."""

# === Start Python 2/3 compatibility
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *  # noqa  pylint: disable=W0401, W0614
from future.builtins.disabled import *  # noqa  pylint: disable=W0401, W0614

# === End Python 2/3 compatibility

# Imports
# =======

import datetime

from comet.manager import TIMESTAMP_FORMAT
from .orm import (
    DatasetStateType,
    DatasetCurrentState,
    DatasetState,
    Dataset,
    DatasetAttachedType,
)

# Logging
# =======

import logging

_logger = logging.getLogger("chimedb")
_logger.addHandler(logging.NullHandler())


def insert_state(entry):
    """
    Insert a dataset state.

    Parameters
    ----------
    entry : dict
        A dict containing the fields `state`, `hash` and `time`. If `state` is `None` this will
        add an entry to the datasetcurrentstate entry with a reference to the state using it's
        ID or return `False` in case the state is not found.

    Returns
    -------
        `True` if successful, `False` if "entry[`state`]" is `False` but the state references
        by `entry["hash"]` is not in the database.
    """
    if entry["state"] is None:
        try:
            state = DatasetState.get(DatasetState.id == entry["hash"])
        except DatasetState.DoesNotExist:
            return False
        DatasetCurrentState.get_or_create(
            state=state,
            time=datetime.datetime.strptime(entry["time"], TIMESTAMP_FORMAT),
        )
    else:
        # Make sure state type known to DB
        state_type, _ = DatasetStateType.get_or_create(name=entry["state"]["type"])

        # Add this state to the DB
        DatasetState.get_or_create(
            id=entry["hash"], type=state_type, data=entry["state"]
        )
    return True


def insert_dataset(entry):
    """
    Insert a dataset.

    Parameters
    ----------
    entry : dict
        A dict containing the fields `ds/state`, `ds/is_root`, `ds/types`, `time`, `hash`.
    """
    state = DatasetState.get(DatasetState.id == entry["ds"]["state"])
    try:
        dataset = Dataset.get(Dataset.id == entry["hash"])
    except Dataset.DoesNotExist:
        dataset, _ = Dataset.get_or_create(
            id=entry["hash"],
            state=state,
            root=entry["ds"]["is_root"],
            time=datetime.datetime.strptime(entry["time"], TIMESTAMP_FORMAT),
        )

    for state_type in entry["ds"]["types"]:
        state_type_id, _ = DatasetStateType.get_or_create(name=state_type)
        DatasetAttachedType.get_or_create(dataset_id=dataset, type=state_type_id)
