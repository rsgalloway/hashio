#!/usr/bin/env python

__doc__ = """
Contains sqlite3 database lock test using with_retry decorator.
"""

import os
import pytest
import sqlite3
import sys
import threading
import time
import uuid


@pytest.fixture(autouse=True)
def reset_hashio_env(tmp_path):
    """Sets up the hashio environment for testing."""
    db_path = tmp_path / f"hashio_test_{uuid.uuid4().hex}.db"
    os.environ["HOME"] = str(tmp_path)
    os.environ["HASHIO_DB"] = str(db_path)
    os.environ["LOG_LEVEL"] = "DEBUG"

    # clear cached hashio modules (force re-import)
    for name in list(sys.modules):
        if name.startswith("hashio"):
            sys.modules.pop(name)

    yield

    os.environ.pop("HOME", None)
    os.environ.pop("HASHIO_DB", None)
    os.environ.pop("LOG_LEVEL", None)


def test_cache_put_succeeds_after_retry(tmp_path):
    from hashio.cache import Cache

    file_path = tmp_path / "test.txt"
    file_path.write_text("hello again")

    cache = Cache()
    lock_acquired = threading.Event()

    # Hold a write lock long enough for Cache.put() to exercise its retry path.
    def hold_lock():
        conn = sqlite3.connect(cache.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            lock_acquired.set()
            time.sleep(0.25)
            conn.commit()
        finally:
            conn.close()

    locker = threading.Thread(target=hold_lock)
    locker.start()
    assert lock_acquired.wait(timeout=1)

    stat = file_path.stat()
    row_id = cache.put(
        str(file_path), stat.st_mtime, "sha256", "hash", stat.st_size, stat.st_ino
    )
    locker.join(timeout=1)
    assert not locker.is_alive()

    assert row_id is not None
    assert cache.get(str(file_path), stat.st_mtime, "sha256") == "hash"
