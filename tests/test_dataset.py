"""Test chimedb.dataset.get.Dataset(State)."""

import chimedb.dataset.get as dget
import chimedb.dataset.orm as orm
from chimedb.dataset.testing import TestChimeDB


import datetime


class TestDataset(TestChimeDB):
    """Test using test_enable() for testing"""

    def test_get_dataset(self):
        # insert test data type in DB
        state_type, _ = orm.DatasetStateType.get_or_create(name="twentythree")
        state_type = state_type

        orm.DatasetState.get_or_create(
            id="23",
            type=state_type.id,
            data={"twenty": 3},
            time=datetime.datetime.now(),
        )
        orm.Dataset.get_or_create(
            id="1337",
            root=True,
            state="23",
            time=datetime.datetime.now(),
            base_dset=None,
        )

        # pre-fetch
        dget.index()
        assert "1337" in dget._dataset_cache
        assert "23" in dget._state_cache

        ds = dget.Dataset.from_id("1337")
        assert ds.__repr__() == "<get.Dataset[twentythree]: 1337>"
        assert ds.base_dataset is None
        assert ds.root is True

        state = dget.DatasetState.from_id("23")
        assert state.__repr__() == "<get.DatasetState[twentythree]: 23>"

        ss = ds.closest_ancestor_of_type(type_="twentythree").state
        assert ss.__repr__() == "<DatasetState: 23>"
        assert ss.type.name == "twentythree"
