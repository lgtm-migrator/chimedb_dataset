"""comet (datasets and states) table definitions."""

from .get import DatasetState, Dataset, DatasetStateType
from .insert import insert_dataset, insert_state

# deprecated
from .get import get_dataset, get_state


from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
