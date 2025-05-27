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

from collections import OrderedDict
from typing import Optional

from hashio.config import DEFAULT_DB_PATH


class LRU:
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
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.lru = LRU()
        self._ensure_db()

    def _ensure_db(self):
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
                inode INTEGER,
                updated_at REAL DEFAULT (strftime('%s','now')),
                PRIMARY KEY (path, mtime, algo)
            )
        """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entries_algo ON entries(algo)"
        )
        self.conn.commit()

    def get(self, path: str, mtime: float, algo: str):
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
        self.conn.execute(
            """
            INSERT OR REPLACE INTO entries
            (path, mtime, algo, hash, size, inode, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
        """,
            (path, mtime, algo, hashval, size, inode),
        )

    def query(self, pattern: str, algo: Optional[str] = None):
        cur = self.conn.cursor()
        if "*" in pattern or "?" in pattern or "[" in pattern:
            cur.execute("SELECT * FROM entries")
            rows = cur.fetchall()
            return [
                row
                for row in rows
                if fnmatch.fnmatch(row[0], pattern) and (algo is None or row[2] == algo)
            ]
        else:
            if algo:
                cur.execute(
                    "SELECT * FROM entries WHERE path=? AND algo=?", (pattern, algo)
                )
            else:
                cur.execute("SELECT * FROM entries WHERE path=?", (pattern,))
            return cur.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()
