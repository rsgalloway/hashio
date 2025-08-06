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
    os.environ["HASHIO_DB"] = str(db_path)
    os.environ["LOG_LEVEL"] = "DEBUG"

    # clear cached hashio modules (force re-import)
    for name in list(sys.modules):
        if name.startswith("hashio"):
            sys.modules.pop(name)

    yield

    os.environ.pop("HASHIO_DB", None)
    os.environ.pop("LOG_LEVEL", None)


def test_hashworker_retries_on_locked_cache(monkeypatch, tmp_path):
    """Test that HashWorker retries when the cache is locked by another process."""

    from hashio.cache import Cache
    from hashio.worker import HashWorker

    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")

    cache = Cache()

    def hold_lock():
        conn = sqlite3.connect(cache.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT OR IGNORE INTO files (id, path, mtime, algo, hash, size, inode) "
            "VALUES (0, 'dummy', 0, 'sha256', '', 0, 'inode')"
        )
        time.sleep(0.5)
        conn.commit()

    # start lock-holder thread
    locker = threading.Thread(target=hold_lock)
    locker.start()
    time.sleep(0.1)

    # run the worker
    worker = HashWorker(str(file_path), force=True, verbose=True)

    # this should raise RuntimeError due to the lock
    with pytest.raises(RuntimeError) as exc_info:
        worker.run()

    locker.join()

    # verify the insert failed
    conn = sqlite3.connect(cache.db_path)
    row = conn.execute(
        "SELECT path, hash FROM files WHERE path = ?", [str(file_path)]
    ).fetchone()
    conn.close()

    assert row is None
