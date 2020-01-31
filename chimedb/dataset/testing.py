"""Utility to set up for testing with a local dummy chimedb."""

import unittest
import tempfile
import os

import chimedb.core as db
import chimedb.dataset.get as dget


class TestChimeDB(unittest.TestCase):
    """Test case using dummy chimedb."""

    def setUp(self):
        """Set up chimedb.core for testing with a local dummy DB."""
        (fd, rcfile) = tempfile.mkstemp(text=True)
        with os.fdopen(fd, "a") as rc:
            rc.write(
                """\
            chimedb:
                db_type:         MySQL
                db:              test
                user_ro:         travis
                passwd_ro:       ""
                user_rw:         travis
                passwd_rw:       ""
                host:            127.0.0.1
                port:            3306
            """
            )

        # Tell chimedb where the database connection config is
        assert os.path.isfile(rcfile), "Could not find {}.".format(rcfile)
        os.environ["CHIMEDB_TEST_RC"] = rcfile

        # Make sure we don't write to the actual chime database
        os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

        db.connect()

        db.orm.create_tables("chimedb.dataset")
        dget.index()
