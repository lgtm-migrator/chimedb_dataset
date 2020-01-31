"""comet (datasets and states) table definitions."""

from .get import DatasetState, Dataset, DatasetStateType
from .insert import insert_dataset, insert_state

# deprecated
from .get import get_dataset, get_state

__version__ = "0.0.1"
