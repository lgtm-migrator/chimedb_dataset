"""Functions to read comet datasets and states from the database."""

# === Start Python 2/3 compatibility
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *  # noqa  pylint: disable=W0401, W0614
from future.builtins.disabled import *  # noqa  pylint: disable=W0401, W0614

# === End Python 2/3 compatibility

# Imports
# =======

from .orm import Dataset, DatasetState, DatasetAttachedType

# Logging
# =======

import logging

_logger = logging.getLogger("chimedb")
_logger.addHandler(logging.NullHandler())


def get_dataset(id):
    """
    Get dataset.

    Parameters
    ----------
    id : int
        The ID of the dataset to get.

    Returns
    -------
    The dataset or `None` if not found.
    """
    try:
        return Dataset.get(Dataset.id == id)
    except Dataset.DoesNotExist:
        _logger.warning("Could not find dataset {}.".format(id))
        return None


def get_state(id):
    """
    Get dataset state.

    Parameters
    ----------
    id : int
        ID of state to get.

    Returns
    -------
    :class:'State'
        The dataset state, or `None` if not found.
    """
    try:
        return DatasetState.get(DatasetState.id == id)
    except DatasetState.DoesNotExist:
        _logger.warning("Could not find state {}.".format(id))
        return None


def get_types(dataset_id):
    """
    Get state types of the given dataset.

    Parameters
    ----------
    dataset_id : int
        ID of the dataset to find attached state types for.

    Returns
    -------
    list of str
        The state types attached to the dataset. Returns `None` if dataset or types not found.
    """
    try:
        result = DatasetAttachedType.select().where(
            DatasetAttachedType.dataset_id == dataset_id
        )
    except DatasetAttachedType.DoesNotExist:
        _logger.warning(
            "Could not find any types attached to dataset {}.".format(dataset_id)
        )
        return None
    types = list()
    for t in result:
        types.append(t.type.name)
    return types
