#!/usr/bin/env python

__doc__ = """
Contains sqlite3 database lock test using with_retry decorator.
"""

import os
import pytest
import sqlite3
import sys
import tempfile
import threading
import time
import uuid


@pytest.fixture(autouse=True)
def reset_hashio_env(tmp_path):
    """Sets up the hashio environment for testing."""
    db_path = tmp_path / f"hashio_test_{uuid.uuid4().hex}.db"
    os.environ["HASHIO_DB"] = str(db_path)
    os.environ["LOG_LEVEL"] = "DEBUG"

    # clear cached hashio modules (force re-import)
    for name in list(sys.modules):
        if name.startswith("hashio"):
            sys.modules.pop(name)

    yield

    os.environ.pop("HASHIO_DB", None)
    os.environ.pop("LOG_LEVEL", None)


def test_retry_on_real_db_lock():
    """Test that the with_retry decorator retries on a real database lock."""
    from hashio.cache import with_retry

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        table = "foo"

        # pre-create the table
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        conn.close()

        def hold_lock():
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("BEGIN IMMEDIATE")  # acquires RESERVED lock
            conn.execute(f"INSERT INTO {table} VALUES (1)")
            time.sleep(1.5)
            conn.commit()
            conn.close()

        t = threading.Thread(target=hold_lock)
        t.start()
        time.sleep(0.1)

        class FakeWriter:
            def __init__(self):
                self.conn = sqlite3.connect(db_path, timeout=0.1)  # no waiting
                self.conn.execute("PRAGMA journal_mode=WAL")

            @with_retry()
            def write(self):
                self.conn.execute(f"INSERT INTO {table} VALUES (2)")
                self.conn.commit()

        writer = FakeWriter()
        writer.write()
        t.join()

        # check results
        conn = sqlite3.connect(db_path)
        rows = list(conn.execute(f"SELECT * FROM {table}"))
        conn.close()
        assert len(rows) == 2

