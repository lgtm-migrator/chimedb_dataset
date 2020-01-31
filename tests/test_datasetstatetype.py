"""Test chimedb.dataset.get.DatasetStateType."""

import chimedb.dataset.get as dget
import chimedb.dataset.orm as orm
from chimedb.dataset.testing import TestChimeDB


class TestDatasetStateType(TestChimeDB):
    """Test using test_enable() for testing"""

    def test_get_dataset_state_type(self):
        # insert state type in DB
        orm.DatasetStateType.get_or_create(name="new_type")

        # get it from DB
        t = dget.DatasetStateType.from_name("new_type")
        assert t.__repr__() == "<DatasetStateType: new_type>"

        # get it from cache
        t = dget.DatasetStateType.from_name("new_type")
        assert t.__repr__() == "<DatasetStateType: new_type>"
