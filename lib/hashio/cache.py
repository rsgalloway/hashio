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
import os
import sqlite3

from datetime import datetime
from collections import OrderedDict
from typing import Optional

from hashio.config import DEFAULT_DB_PATH
from hashio.logger import logger


class LRU:
    """A simple LRU cache implementation using an OrderedDict."""

    def __init__(self, maxsize=100_000):
        self.maxsize = maxsize
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key, value=True):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)


class Cache:
    """A class to manage a SQLite database cache for file hashes."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.lru = LRU()
        self._ensure_db()

    def _ensure_db(self):
        """Ensure the database and its tables exist, creating them if necessary."""
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                path TEXT NOT NULL,
                mtime REAL NOT NULL,
                algo TEXT NOT NULL,
                hash TEXT,
                size INTEGER,
                inode TEXT,
                updated_at REAL DEFAULT (strftime('%s','now')),
                PRIMARY KEY (path, mtime, algo)
            )
        """
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entries_algo ON entries(algo)"
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
            SELECT hash FROM entries
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
            SELECT 1 FROM entries
            WHERE path=? AND mtime=? AND algo=? AND hash=?
        """,
            key,
        )
        exists = cur.fetchone() is not None
        if exists:
            self.lru.put(key)
        return exists

    def put(
        self, path: str, mtime: float, algo: str, hashval: str, size: int, inode: int
    ):
        """Store a hash entry in the cache.

        :param path: The file path to store.
        :param mtime: The last modified time of the file.
        :param algo: The hashing algorithm used.
        :param hashval: The hash value to store.
        :param size: The size of the file.
        :param inode: The inode number of the file.
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO entries
            (path, mtime, algo, hash, size, inode, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
        """,
            (path, mtime, algo, hashval, size, str(inode)),
        )

    def query(
        self, pattern: str, algo: Optional[str] = None, since: Optional[str] = None
    ):
        """Query the cache for entries matching a pattern.

        :param pattern: The pattern to match against file paths.
        :param algo: Optional hashing algorithm to filter results.
        :param since: Optional timestamp to filter results by last updated time.
        :return: A list of tuples containing matching entries.
        """
        sql = "SELECT * FROM entries"
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

    def commit(self):
        """Commit any pending changes to the database."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.commit()
        self.conn.close()
