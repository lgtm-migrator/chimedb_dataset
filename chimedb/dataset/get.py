"""Functions to read comet datasets and states from the database."""

# === Start Python 2/3 compatibility
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *  # noqa  pylint: disable=W0401, W0614
from future.builtins.disabled import *  # noqa  pylint: disable=W0401, W0614

# === End Python 2/3 compatibility

# Imports
# =======

import warnings

from . import orm
from chimedb.core.exceptions import NotFoundError, ValidationError

# Logging
# =======

import logging

_logger = logging.getLogger("chimedb")
_logger.addHandler(logging.NullHandler())


# local cache for states etc so they don't get transferred from the database repeatedly
_state_cache = dict()
_dataset_cache = dict()
_type_cache = dict()  # by name
_type_cache_by_id = dict()


class DatasetStateType(orm.DatasetStateType):
    """
    Model for datasetstatetype table.

    This wraps the model in chimedb.database.orm and adds a local cache and additional
    functionality.
    """

    @classmethod
    def from_name(cls, name):
        """Create a new DatasetStateType object.

        Get the table row from cache or database.
        This does not insert a new row into the database.

        Parameter
        ----------
        name : str
            Name of the type.

        Returns
        -------
        DatasetStateType
            The requested type.

        Raises
        ------
        chimedb.dataset.orm.DatasetStateType.DoesNotExist
            If the type with the requested name doesn't exist in the database. Use
            chimedb.dataset.insert to create rows.
        """
        try:
            # look in cache first
            return _type_cache[name]
        except KeyError:
            # ask database
            new_type = orm.DatasetStateType.get(orm.DatasetStateType.name == name)
            if new_type is None:
                return None

            # fill cache
            _type_cache[name] = new_type
            _type_cache_by_id[new_type.id] = new_type
            return _type_cache[name]

    def __repr__(self):
        return "<get.DatasetStateType: {}>".format(self.name)


class DatasetState(orm.DatasetState):
    """
    Model for datasetstate table.

    This wraps the model in chimedb.database.orm and adds a local cache and additional
    functionality.
    """

    @classmethod
    def from_id(cls, state_id, load_data=True):
        """Create a new DatasetState object.

        Get the table row from cache or database.
        This does not insert a new row into the database.

        Parameter
        ----------
        state_id
            State ID.
        load_data : bool
            .data is only loaded and cached if this is True.

        Returns
        -------
        DatasetState or None
            The requested state. Or None if it wasn't found.
        """
        if state_id not in _state_cache.keys():
            try:
                _logger.debug("Loading state {} from database...".format(state_id))
                if load_data:
                    _state_cache[state_id] = DatasetState.get(
                        DatasetState.id == state_id
                    )
                else:
                    _state_cache[state_id] = DatasetState.select(
                        DatasetState.type, DatasetState.time
                    ).get()
            except orm.DatasetState.DoesNotExist:
                _logger.warning("Could not find state {}.".format(state_id))
                return None
        return _state_cache[state_id]

    def __repr__(self):
        type_ = self.state_type
        if type_ is not None:
            return "<get.DatasetState[{}]: {}>".format(type_.name, self.id)
        else:
            return "<get.DatasetState: {}>".format(self.id)

    @property
    def state_type(self):
        """Get state type."""
        try:
            return _type_cache_by_id[self.type_id]
        except KeyError:
            type_ = self.type
            _type_cache_by_id[self.type_id] = type_
            _type_cache[type_.name] = type_
            return type_

    @staticmethod
    def exists(state_id):
        """
        Find out if a state exists.

        Parameters
        ----------
        state_id : str
            State ID

        Returns
        -------
        bool
            True if the state exists
        """
        if state_id in _state_cache.keys():
            return True
        return orm.DatasetState.select().where(orm.DatasetState.id == state_id).exists()


class Dataset(orm.Dataset):
    """
    Model for dataset table.

    This wraps the model in chimedb.database.orm and adds a local cache and additional
    functionality.
    """

    _type = None
    _base_dataset = None

    @classmethod
    def from_id(cls, ds_id):
        """Create a new Dataset object.

        Get the table row from cache or database.
        This does not insert a new row into the database.

        Parameter
        ----------
        ds_id
            Dataset ID.

        Returns
        -------
        Dataset
            The requested dataset.
        """
        if not isinstance(ds_id, str):
            raise ValidationError(
                "ds_id is of type {} (expected str)".format(type(ds_id).__name__)
            )
        if ds_id not in _dataset_cache:
            try:
                _dataset_cache[ds_id] = Dataset.get(Dataset.id == ds_id)
            except orm.Dataset.DoesNotExist:
                _logger.warning("Could not find dataset {}.".format(ds_id))
                return None
        return _dataset_cache[ds_id]

    @property
    def type(self):
        """Get the type of the attached dataset state."""
        if self._type is None:
            return self.dataset_state.type
        return self._type

    @property
    def base_dataset(self):
        """Get the base dataset."""
        if not self._base_dataset:
            self._base_dataset = self.base_dset
        return self._base_dataset

    @property
    def dataset_state(self):
        """Get the dataset state."""
        # get the raw ID of the state (foreign key)
        orm_base_class = super()
        state_id = orm_base_class.state_id

        if state_id not in _state_cache:
            _state_cache[state_id] = orm_base_class.state
        return _state_cache[state_id]

    def __repr__(self):
        if self._type:
            return "<get.Dataset[{}]: {}>".format(self.type, self.id)
        else:
            return "<get.Dataset: {}>".format(self.id)

    def closest_ancestor_of_type(self, type_):
        """
        Get the closest ancestorial dataset of the given type.

        Parameters
        ----------
        type_ : str or :class:`chimedb.dataset.orm.DatasetStateType`

        Raises
        ------
        :class:`chimedb.dataset.orm.DatasetStateType.DoesNotExist`
            If `type_` is not an existing dataset state type.

        Returns
        -------
        :class:`chimedb.dataset.orm.Dataset`
            The closest ancestor of the given type. Returns `None` if no ancestor of the
            given type found.
        """
        if isinstance(type_, str):
            type_ = DatasetStateType.from_name(name=type_)
            if type_ is None:
                raise NotFoundError("{} is not a known DatasetStateType".format(type_))
            type_ = type_.name
        d = self

        while d:
            if d.type == type_:
                return d
            d = d.base_dataset

        raise NotFoundError(
            "No ancestor of type {} found for Dataset {}".format(type_, self.__repr__())
        )


def index():
    """Pre-fetch and cache all Dataset(State(Type))s."""
    query = (
        orm.Dataset.select(orm.Dataset, orm.DatasetStateType.name.alias("_type"))
        .join(orm.DatasetState)
        .join(orm.DatasetStateType)
    )

    # Create a mapping from dataset ID to
    dsdict = {d.id: d for d in query.objects(Dataset)}

    # De-reference the base datasets
    for d in dsdict.values():
        base_dset = None if d.root or d.base_dset_id is None else dsdict[d.base_dset_id]
        d._base_dataset = base_dset
    _dataset_cache.update(dsdict)

    # Pre-fetch all states without their .data and fill cache
    for s in DatasetState.select(DatasetState.id, DatasetState.type_id):
        _state_cache[s.id] = s

    # Pre-fetch and cache types
    for t in DatasetStateType.select():
        _type_cache[t.name] = t
        _type_cache_by_id[t.id] = t


def get_dataset(ds_id):
    """
    Get dataset.

    Deprecated.

    Parameters
    ----------
    ds_id : int
        The ID of the dataset to get.

    Returns
    -------
    The dataset or `None` if not found.
    """
    warnings.warn(
        "chimedb.dataset.get.get_dataset is deprecated. Use chimedb.dataset.Dataset.from_id "
        "instead",
        DeprecationWarning,
    )
    return Dataset.from_id(ds_id)


def get_state(state_id):
    """
    Get dataset state.

    Deprecated.

    Parameters
    ----------
    state_id : int
        ID of state to get.

    Returns
    -------
    :class:'State'
        The dataset state, or `None` if not found.
    """
    warnings.warn(
        "chimedb.dataset.get.get_state is deprecated. Use chimedb.dataset.DatasetState.from_id "
        "instead",
        DeprecationWarning,
    )
    return DatasetState.from_id(state_id)
