#!/usr/bin/env python
#
# Copyright (c) 2024-2025, Ryan Galloway (ryan@rsgalloway.com)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#  - Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
#  - Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
#  - Neither the name of the software nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

__doc__ = """
Contains global cache classes and functions.
"""

import fnmatch
import functools
import os
import random
import sqlite3
import time

from datetime import datetime
from collections import OrderedDict
from typing import Optional

from hashio.config import DEFAULT_DB_PATH
from hashio.logger import logger

# sqlite3 OperationalError messages that indicate a locked database
LOCK_MESSAGES = [
    "database is locked",
    "database schema is locked",
    "database table is locked",
    "database is busy",
    "already in use",
]


def with_retry(retries: int = 5, delay: float = 0.2, backoff: float = 1.25):
    """Retry a function on SQLite 'database is locked' errors using exponential backoff."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            _delay = delay
            last_err = None
            for i in range(retries):
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    logger.debug(
                        f"{fn.__name__} attempt {i + 1} with delay {_delay:.2f}s"
                    )
                    err_msg = str(e).lower()
                    if any(msg in err_msg for msg in LOCK_MESSAGES):
                        last_err = e
                        # rollback if in transaction
                        conn = getattr(args[0], "conn", None)
                        if conn and getattr(conn, "in_transaction", False):
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                        # jitter to reduce contention
                        time.sleep(_delay + random.uniform(0, 0.1))
                        _delay *= backoff
                    else:
                        logger.warning("sqlite3.OperationalError: %s", str(e))
                        raise
            raise RuntimeError(
                f"{fn.__name__} failed after {retries} retries with error: {last_err} ({args[0].db_path})"
            )

        return wrapper

    return decorator


class LRU:
    """A simple LRU cache implementation using an OrderedDict.

    Usage:

    >>> lru = LRU(maxsize=100)
    >>> lru.put('key1', 'value1')
    >>> value = lru.get('key1')  # returns 'value1'
    >>> lru.put('key2', 'value2')  # adds another item
    >>> # if maxsize is exceeded, the oldest item will be removed.
    """

    def __init__(self, maxsize: int = 100_000):
        """Initialize the LRU cache.

        :param maxsize: Maximum number of items to store in the cache.
        """
        self.maxsize = maxsize
        self.cache = OrderedDict()

    def get(self, key: str):
        """Retrieve a value from the cache by key."""
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key: str, value: bool = True):
        """Store a key-value pair in the cache."""
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)


class Cache:
    """A class to manage a SQLite database cache for file hashes."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """Initialize the cache with a database path.

        :param db_path: The path to the SQLite database file.
        """
        self.db_path = db_path
        self.conn = None
        self.lru = LRU()
        self._ensure_db()

    def _ensure_db(self):
        """Ensure the database and its tables exist, creating them if necessary."""
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        # create files table if it does not exist
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                mtime REAL NOT NULL,
                algo TEXT NOT NULL,
                hash TEXT,
                size INTEGER,
                inode TEXT,
                updated_at REAL DEFAULT (strftime('%s','now')),
                UNIQUE(path, mtime, algo)
            )
        """
        )
        # create snapshots and snapshot_files tables if they do not exist
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                path TEXT,
                created_at REAL DEFAULT (strftime('%s','now'))
            )
        """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_files (
                snapshot_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                PRIMARY KEY (snapshot_id, file_id),
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            );
        """
        )
        # enable foreign keys and set performance optimizations
        self.conn.execute("PRAGMA foreign_keys = ON")
        # use Write-Ahead Logging for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        # set synchronous mode to NORMAL for better performance
        self.conn.execute("PRAGMA synchronous=NORMAL")
        # set busy timeout to 0ms to rely on retry decorator for lock handling
        self.conn.execute("PRAGMA busy_timeout=0")
        # create indexes for faster lookups
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_algo ON files(algo)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_inode ON files(inode)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_hash_inode ON files(hash, inode)"
        )
        self.conn.commit()

    def flush(self):
        """Flush any pending changes to the database."""
        try:
            self.conn.commit()
        except sqlite3.Error as e:
            logger.warning(f"SQLite flush error: {e}")

    def get(self, path: str, mtime: float, algo: str):
        """Retrieve the hash for a given path, mtime, and algorithm.

        :param path: The file path to query.
        :param mtime: The last modified time of the file.
        :param algo: The hashing algorithm used.
        :return: The hash value if found, otherwise None.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT hash FROM files
            WHERE path = ? AND mtime = ? AND algo = ?
        """,
            (path, mtime, algo),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def has(self, path, mtime, algo, hashval):
        """Check if a hash entry exists in the cache.

        :param path: The file path to check.
        :param mtime: The last modified time of the file.
        :param algo: The hashing algorithm used.
        :param hashval: The hash value to check.
        :return: True if the entry exists, otherwise False.
        """
        key = (path, mtime, algo, hashval)
        if self.lru.get(key):
            return True

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM files
            WHERE path=? AND mtime=? AND algo=? AND hash=?
        """,
            key,
        )
        exists = cur.fetchone() is not None
        if exists:
            self.lru.put(key)
        return exists

    @with_retry()
    def put(
        self, path: str, mtime: float, algo: str, hashval: str, size: int, inode: int
    ):
        """Store a hash entry in the cache. If the entry already exists, it will
        return the existing ID instead of inserting a new row.

        :param path: The file path to store.
        :param mtime: The last modified time of the file.
        :param algo: The hashing algorithm used.
        :param hashval: The hash value to store.
        :param size: The size of the file.
        :param inode: The inode number of the file.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO files (path, mtime, algo, hash, size, inode, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
        """,
            (path, mtime, algo, hashval, size, str(inode)),
        )

        # fetch the ID of the existing or newly inserted row
        cur.execute(
            """
            SELECT id FROM files WHERE path=? AND mtime=? AND algo=?
        """,
            (path, mtime, algo),
        )
        row = cur.fetchone()
        return row[0] if row else None

    @with_retry()
    def merge(self, path: str):
        """Merge another database into this cache. This will insert all unique
        entries from the other database into the current cache. This will lock
        the database for the duration of the merge.

        :param path: The path to the other SQLite database file.
        """
        self.conn.execute("ATTACH DATABASE ? AS tempdb", (path,))
        # do not insert IDs
        self.conn.executescript(
            """
            INSERT OR IGNORE INTO files (path, mtime, algo, hash, size, inode)
            SELECT path, mtime, algo, hash, size, inode FROM tempdb.files;

            INSERT OR IGNORE INTO snapshots (name, created_at, path)
            SELECT name, created_at, path FROM tempdb.snapshots;

            INSERT OR IGNORE INTO snapshot_files (snapshot_id, file_id)
            SELECT snapshot_id, file_id FROM tempdb.snapshot_files;
        """
        )
        self.conn.commit()
        self.conn.execute("DETACH DATABASE tempdb")

    def query(
        self, pattern: str, algo: Optional[str] = None, since: Optional[str] = None
    ):
        """Query the cache for files matching a pattern.

        :param pattern: The pattern to match against file paths.
        :param algo: Optional hashing algorithm to filter results.
        :param since: Optional timestamp to filter results by last updated time.
        :return: A list of tuples containing matching files.
        """
        sql = "SELECT * FROM files"
        params = []
        filters = []

        # algo filter
        if algo:
            filters.append("algo = ?")
            params.append(algo)

        # time filter
        if since:
            try:
                ts = datetime.fromisoformat(since).timestamp()
                filters.append("updated_at >= ?")
                params.append(ts)
            except ValueError:
                raise ValueError(f"Invalid --since datetime: {since}")

        # base wildcard logic
        if "*" in pattern or "?" in pattern or "[" in pattern:
            self.conn.row_factory = sqlite3.Row
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return [
                row
                for row in rows
                if fnmatch.fnmatch(row["path"], pattern)
                and all(row[k] == v for k, v in zip(["algo"], [algo]) if algo)
                and (not since or row["updated_at"] >= ts)
            ]
        else:
            filters.append("path = ?")
            params.append(pattern)
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            cur = self.conn.cursor()
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def get_file_id(self, path: str, mtime: float, algo: str):
        """Return the ID of a file entry matching path, mtime, algo.

        :param path: The file path to query.
        :param mtime: The last modified time of the file.
        :param algo: The hashing algorithm used.
        :return: The ID of the file entry if found, otherwise None.
        """
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id FROM files WHERE path=? AND mtime=? AND algo=?",
            (path, mtime, algo),
        )
        row = cur.fetchone()
        return row[0] if row else None

    @with_retry()
    def put_file_and_get_id(
        self, path: str, mtime: float, algo: str, hashval: str, size: int, inode: int
    ):
        """Insert or reuse a file entry, and return its ID."""
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO files (path, mtime, algo, hash, size, inode, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
        """,
            (path, mtime, algo, hashval, size, str(inode)),
        )
        return self.get_file_id(path, mtime, algo)

    @with_retry()
    def get_or_create_snapshot(self, name: str, path: str = None):
        """Get the ID of an existing snapshot by name, or create it if it
        doesn't exist.

        :param name: The name of the snapshot to retrieve or create.
        :param path: Optional root path for the snapshot.
        :return: The ID of the snapshot.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM snapshots WHERE name = ?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO snapshots (name, path) VALUES (?, ?)", (name, path))
        self.conn.commit()
        return cur.lastrowid

    @with_retry()
    def replace_snapshot(self, name: str, path: str = None):
        """Deletes any existing snapshot with the same name and creates a fresh one.

        :param name: The name of the snapshot to replace.
        :param path: Optional root path for the snapshot.
        :return: The ID of the newly created snapshot.
        """
        self.delete_snapshot(name)
        return self.get_or_create_snapshot(name, path)

    def get_snapshot_id(self, name: str):
        """Retrieve the ID of a snapshot by name.

        :param name: The name of the snapshot to retrieve.
        :return: The ID of the snapshot, or None if it does not exist.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM snapshots WHERE name = ?", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    def get_all_file_ids(self):
        """Retrieve all file IDs in the database.

        :return: A list of file IDs.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM files")
        return [row[0] for row in cur.fetchall()]

    @with_retry()
    def delete_snapshot(self, name: str):
        """Delete a snapshot by name.

        :param name: The name of the snapshot to delete.
        :raises ValueError: If the snapshot does not exist.
        """
        self.conn.execute("DELETE FROM snapshots WHERE name = ?", (name,))
        self.conn.commit()

    @with_retry()
    def add_to_snapshot(self, snapshot_id: int, file_id: int):
        """Link an entry to a snapshot.

        :param snapshot_id: The ID of the snapshot to link to.
        :param file_id: The file id to link.
        """
        self.conn.execute(
            """
            INSERT OR IGNORE INTO snapshot_files (snapshot_id, file_id)
            VALUES (?, ?)
            """,
            (snapshot_id, file_id),
        )

    @with_retry()
    def batch_add_snapshot_links(self, snapshot_id: int, file_ids: list):
        """Batch-insert links between a snapshot and file IDs.

        :param snapshot_id: The ID of the snapshot to link to.
        :param file_ids: A list of file IDs to link to the snapshot.
        :raises ValueError: If file_ids is empty.
        """
        if not file_ids:
            return
        records = [(snapshot_id, fid) for fid in file_ids]
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO snapshot_files (snapshot_id, file_id)
            VALUES (?, ?)
        """,
            records,
        )

    def list_snapshots(self):
        """Return all snapshots.

        :return: A list of dictionaries containing snapshot IDs, names, and
            creation times.
        """
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, name, path, created_at FROM snapshots ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
        return [
            {"id": row[0], "name": row[1], "path": row[2], "created_at": row[3]}
            for row in rows
        ]

    def diff_snapshots(self, name1: str, name2: str, force: bool = False):
        """Compare two snapshots and return a diff summary.

        :param name1: The name of the first snapshot.
        :param name2: The name of the second snapshot.
        :param force: If True, force the diff even if root paths don't match.
        :return: A dictionary with lists of added, removed, changed, and moved files.
        """
        cur = self.conn.cursor()

        # get snapshot IDs
        cur.execute("SELECT id, path FROM snapshots WHERE name = ?", (name1,))
        row1 = cur.fetchone()
        cur.execute("SELECT id, path FROM snapshots WHERE name = ?", (name2,))
        row2 = cur.fetchone()

        if not row1 or not row2:
            raise ValueError(f"Snapshot not found: {name1 if not row1 else name2}")

        id1, root1 = row1
        id2, root2 = row2

        if root1 != root2 and not force:
            raise ValueError(f"Snapshots have different paths: {root1} != {root2}")

        diff = {"added": [], "removed": [], "changed": []}

        # changed files: same path in both, but different hash
        changed_paths = set()
        cur.execute(
            """
            SELECT f1.path
            FROM snapshot_files sf1
            JOIN files f1 ON f1.id = sf1.file_id
            JOIN snapshot_files sf2 ON sf2.snapshot_id = ?
            JOIN files f2 ON f2.id = sf2.file_id
            WHERE sf1.snapshot_id = ?
            AND f1.path = f2.path
            AND f1.hash IS NOT NULL AND f2.hash IS NOT NULL
            AND f1.hash != f2.hash
        """,
            (id2, id1),
        )
        for row in cur.fetchall():
            changed_paths.add(row[0])
            diff["changed"].append(row[0])

        # added files: in v2, not in v1 (ecluding changed paths)
        cur.execute(
            """
            SELECT f.path FROM snapshot_files sf2
            JOIN files f ON f.id = sf2.file_id
            WHERE sf2.snapshot_id = ?
            AND sf2.file_id NOT IN (
                SELECT file_id FROM snapshot_files WHERE snapshot_id = ?
            )
        """,
            (id2, id1),
        )
        for row in cur.fetchall():
            if row[0] not in changed_paths:
                diff["added"].append(row[0])

        # removed files: in v1, not in v2 (ecluding changed paths)
        cur.execute(
            """
            SELECT f.path FROM snapshot_files sf1
            JOIN files f ON f.id = sf1.file_id
            WHERE sf1.snapshot_id = ?
            AND sf1.file_id NOT IN (
                SELECT file_id FROM snapshot_files WHERE snapshot_id = ?
            )
        """,
            (id1, id2),
        )
        for row in cur.fetchall():
            if row[0] not in changed_paths:
                diff["removed"].append(row[0])

        # normalize everything into sets for deduplication
        added_set = set(diff["added"])
        removed_set = set(diff["removed"])
        changed_set = set(diff["changed"])

        # if a file appears in both added and removed, but not changed, it's a false positive
        both_added_and_removed = added_set & removed_set

        # remove those from both lists — likely due to mtime/hash/id issues
        diff["added"] = sorted(list(added_set - both_added_and_removed))
        diff["removed"] = sorted(list(removed_set - both_added_and_removed))

        # if it was truly changed, let it appear only in 'changed'
        diff["changed"] = sorted(list(changed_set))

        return diff

    def commit(self):
        """Commit any pending changes to the database."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.commit()
        self.conn.close()
