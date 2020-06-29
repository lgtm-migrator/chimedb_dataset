"""Test chimedb.dataset.get.Dataset(State)."""

import chimedb.dataset.get as dget
import chimedb.dataset.orm as orm
from chimedb.dataset.testing import TestChimeDB

import datetime
from peewee import DoesNotExist


class TestDataset(TestChimeDB):
    """Test using test_enable() for testing"""

    def setUp(self):
        super().setUp()
        # insert test data type in DB
        state_type, _ = orm.DatasetStateType.get_or_create(name="twentythree")

        try:
            orm.DatasetState.get(id="23")
        except DoesNotExist:
            orm.DatasetState.get_or_create(
                id="23",
                type=state_type.id,
                data={"twenty": 3},
                time=datetime.datetime.now(),
            )
        try:
            ds_ = orm.Dataset.get(id="1337")
        except DoesNotExist:
            ds_, _ = orm.Dataset.get_or_create(
                id="1337",
                root=True,
                state="23",
                time=datetime.datetime.now(),
                base_dset=None,
            )

        # insert test data type 2 in DB
        state_type2, _ = orm.DatasetStateType.get_or_create(name="twentyfour")

        try:
            orm.DatasetState.get(id="24")
        except DoesNotExist:
            orm.DatasetState.get_or_create(
                id="24",
                type=state_type2.id,
                data={"twenty": 4},
                time=datetime.datetime.now(),
            )
        try:
            ds_ = orm.Dataset.get(id="1338")
        except DoesNotExist:
            orm.Dataset.get_or_create(
                id="1338",
                root=False,
                state="24",
                time=datetime.datetime.now(),
                base_dset=ds_,
            )

    def test_get_dataset_no_prefetch(self):
        def tests():
            ds = dget.Dataset.from_id("1337")
            assert (
                ds.__repr__() == "<get.Dataset: 1337>"
                or ds.__repr__() == "<get.Dataset[twentythree]: 1337>"
            )
            assert ds.base_dataset is None
            assert ds.root is True

            state = dget.DatasetState.from_id("23")
            assert state.__repr__() == "<get.DatasetState[twentythree]: 23>"

            ss = ds.closest_ancestor_of_type(type_="twentythree").state
            assert ss.__repr__() == "<DatasetState: 23>"
            assert ss.type.name == "twentythree"

            assert ss.data == {"twenty": 3}

        tests()

        # pre-fetch
        dget.index()
        tests()

    def test_get_root_dataset_no_prefetch(self):
        def tests():
            assert "1338" in dget._dataset_cache
            assert "24" in dget._state_cache

            ds = dget.Dataset.from_id("1338")
            assert ds.__repr__() == "<get.Dataset[twentyfour]: 1338>"
            assert ds.base_dataset is not None
            assert ds.root is False

            state = dget.DatasetState.from_id("24")
            assert state.__repr__() == "<get.DatasetState[twentyfour]: 24>"

            ss = ds.closest_ancestor_of_type(type_="twentyfour").state
            assert ss.__repr__() == "<DatasetState: 24>"
            assert ss.type.name == "twentyfour"

        tests()

        # pre-fetch
        dget.index()
        tests()
