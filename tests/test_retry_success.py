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


def test_hashworker_succeeds_after_retry(monkeypatch, tmp_path):

    from hashio.cache import Cache
    from hashio.worker import HashWorker

    file_path = tmp_path / "test.txt"
    file_path.write_text("hello again")

    cache = Cache()

    # Lock DB briefly
    def hold_lock():
        conn = sqlite3.connect(cache.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO files VALUES (0, 'dummy', 0, 'sha256', '', 0, 'inode', strftime('%s','now'))"
        )
        print("[LOCKER] Holding DB lock...")
        time.sleep(1.0)  # increased
        print("[LOCKER] Releasing DB lock")
        conn.commit()
        conn.close()

    locker = threading.Thread(target=hold_lock)
    locker.start()

    # try a quick dummy insert from main thread
    test_conn = sqlite3.connect(cache.db_path)
    test_conn.execute("PRAGMA journal_mode=WAL")
    test_conn.execute("INSERT INTO files VALUES (999, 'main', 0, 'sha256', '', 0, 'inode', strftime('%s','now'))")
    test_conn.commit()
    test_conn.close()

    # run the worker
    worker = HashWorker(str(file_path), force=True, verbose=True)
    worker.run()
    locker.join()
    time.sleep(0.1)

    # verify hash got written
    conn = sqlite3.connect(cache.db_path)
    row = conn.execute(
        "SELECT path, hash FROM files WHERE path = ?", [str(file_path)]
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == str(file_path)
    assert row[1] is not None
    print("[TEST] Hash successfully written to database:", row)