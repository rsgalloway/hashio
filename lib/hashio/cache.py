#!/usr/bin/env python
#
# Copyright (c) 2024-2025, Ryan Galloway (ryan@rsgalloway.com)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the software nor the names of its contributors may be
#     used to endorse or promote products derived from this software without
#     specific prior written permission.
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
"""
Contains sharded SQLite cache with natural keys.
"""

from __future__ import annotations

import fnmatch
import functools
import hashlib
import os
import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from hashio.config import DEFAULT_DB_PATH
from hashio.logger import logger

LOCK_MESSAGES = [
    "database is locked",
    "database schema is locked",
    "database table is locked",
    "database is busy",
    "already in use",
]


def with_retry(retries: int = 5, delay: float = 0.2, backoff: float = 1.25):
    """Retry a function on SQLite 'database is locked' errors using exponential backoff.

    If the wrapped function receives a `conn=` keyword arg, that connection will be
    rolled back on lock errors; otherwise the wrapper will try to rollback `self.meta_conn`.
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            _delay = delay
            last_err = None
            for i in range(retries):
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    err_msg = str(e).lower()
                    if any(msg in err_msg for msg in LOCK_MESSAGES):
                        last_err = e
                        # rollback if in transaction (prefer explicit conn param)
                        conn = kwargs.get("conn")
                        if conn is None:
                            # Try instance-level meta conn fallback
                            if args:
                                self = args[0]
                                conn = getattr(self, "meta_conn", None)
                        try:
                            if conn is not None and getattr(
                                conn, "in_transaction", False
                            ):
                                conn.rollback()
                        except Exception:
                            pass
                        logger.debug(
                            "%s locked (attempt %d/%d). Sleeping %.2fs",
                            fn.__name__,
                            i + 1,
                            retries,
                            _delay,
                        )
                        time.sleep(_delay + random.uniform(0, 0.1))
                        _delay *= backoff
                    else:
                        raise
            raise RuntimeError(
                f"{fn.__name__} failed after {retries} retries: {last_err}"
            )

        return wrapper

    return decorator


class LRU:
    """Minimal in-memory LRU cache for recently-seen (device, inode, mtime_ns,
    algo, hash) tuples."""

    def __init__(self, maxsize: int = 100_000):
        from collections import OrderedDict

        self.maxsize = maxsize
        self._d = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            v = self._d.get(key)
            if v is not None:
                self._d.move_to_end(key)
            return v

    def put(self, key, value=True):
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            if len(self._d) > self.maxsize:
                self._d.popitem(last=False)


@dataclass(frozen=True)
class NaturalKey:
    device: int
    inode: int

    def shard_key(self) -> str:
        h = hashlib.sha1(f"{self.device}:{self.inode}".encode("utf-8")).hexdigest()
        return h[:2]  # 256 shards by default


DEFAULT_SHARDS = 256
SHARDS_DIRNAME = "shards"


class Cache:
    """Sharded SQLite cache using **natural keys** (device, inode).

    The path you pass to `Cache(db_path)` is the **meta DB** (snapshots only).
    All file data lives in per-shard DBs under `<db_dir>/shards/xx.db`.
    """

    def __init__(
        self, db_path: str = DEFAULT_DB_PATH, num_shards: int = DEFAULT_SHARDS
    ):
        self.db_path = db_path
        self.db_dir = os.path.dirname(os.path.abspath(db_path)) or "."
        self.num_shards = int(num_shards)
        self.shards_dir = os.path.join(self.db_dir, SHARDS_DIRNAME)
        os.makedirs(self.db_dir, exist_ok=True)
        os.makedirs(self.shards_dir, exist_ok=True)

        # Meta DB for snapshots
        self.meta_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._prep_conn(self.meta_conn, is_meta=True)
        self._ensure_meta_schema()

        # Lazy-opened shard conns keyed by two-hex prefix ("00".."ff")
        self._shard_conns: Dict[str, sqlite3.Connection] = {}
        self._shard_lock = threading.Lock()

        self.lru = LRU()

    # ------------------- connection & schema helpers -------------------

    def _prep_conn(self, conn: sqlite3.Connection, *, is_meta: bool = False) -> None:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA page_size=4096")
        conn.execute("PRAGMA cache_size=-1048576")  # ~1GB page cache (tune)
        # busy_timeout=0 so our retry wrapper controls contention
        conn.execute("PRAGMA busy_timeout=0")
        if not is_meta:
            # A few perf-leaning pragmas for shard DBs
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB

    def _ensure_meta_schema(self) -> None:
        self.meta_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE,
                path        TEXT,
                created_at  REAL DEFAULT (strftime('%s','now'))
            );
            """
        )
        self.meta_conn.commit()

    def _ensure_shard_schema(self, conn: sqlite3.Connection) -> None:
        # files keyed by natural key (device, inode)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                device      INTEGER NOT NULL,
                inode       INTEGER NOT NULL,
                algo        TEXT    NOT NULL,
                hash        TEXT,
                size        INTEGER,
                mtime_ns    INTEGER NOT NULL,
                path        TEXT    NOT NULL,
                updated_at  REAL    DEFAULT (strftime('%s','now')),
                PRIMARY KEY (device, inode)
            );
            """
        )
        # snapshot membership is also by natural key
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_files (
                snapshot_id INTEGER NOT NULL,
                device      INTEGER NOT NULL,
                inode       INTEGER NOT NULL,
                path        TEXT    NOT NULL,
                size        INTEGER NOT NULL,
                mtime_ns    INTEGER NOT NULL,
                PRIMARY KEY (snapshot_id, device, inode)
            );
            """
        )
        # indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime_ns)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
        conn.commit()

    def _open_shard(self, shard: str) -> sqlite3.Connection:
        with self._shard_lock:
            conn = self._shard_conns.get(shard)
            if conn is not None:
                return conn
            dbfile = os.path.join(self.shards_dir, f"{shard}.db")
            conn = sqlite3.connect(dbfile, check_same_thread=False)
            self._prep_conn(conn, is_meta=False)
            self._ensure_shard_schema(conn)
            self._shard_conns[shard] = conn
            return conn

    def _conn_for_key(self, nk: NaturalKey) -> Tuple[str, sqlite3.Connection]:
        shard = nk.shard_key()
        return shard, self._open_shard(shard)

    # ------------------- public API (natural-key oriented) -------------------

    @with_retry()
    def upsert_file(
        self,
        device: int,
        inode: int,
        path: str,
        size: int,
        mtime_ns: int,
        algo: str,
        hashval: Optional[str],
    ) -> None:
        """Insert or update a file record keyed by (device, inode).

        This is the canonical write path. Keeps the latest path/mtime/hash.
        """
        nk = NaturalKey(device, inode)
        shard, conn = self._conn_for_key(nk)
        conn.execute(
            """
            INSERT INTO files (device, inode, algo, hash, size, mtime_ns, path, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
            ON CONFLICT(device, inode) DO UPDATE SET
                algo=excluded.algo,
                hash=excluded.hash,
                size=excluded.size,
                mtime_ns=excluded.mtime_ns,
                path=excluded.path,
                updated_at=strftime('%s','now')
            ;
            """,
            (device, inode, algo, hashval, size, mtime_ns, path),
        )
        # small LRU for has_entry()
        self.lru.put((device, inode, mtime_ns, algo, hashval))

    def get_hash(
        self, device: int, inode: int, mtime_ns: int, algo: str
    ) -> Optional[str]:
        nk = NaturalKey(device, inode)
        shard, conn = self._conn_for_key(nk)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hash FROM files
             WHERE device=? AND inode=? AND mtime_ns=? AND algo=?
            """,
            (device, inode, mtime_ns, algo),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def has_entry(
        self, device: int, inode: int, mtime_ns: int, algo: str, hashval: str
    ) -> bool:
        key = (device, inode, mtime_ns, algo, hashval)
        if self.lru.get(key):
            return True
        nk = NaturalKey(device, inode)
        shard, conn = self._conn_for_key(nk)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM files
             WHERE device=? AND inode=? AND mtime_ns=? AND algo=? AND hash=?
            """,
            (device, inode, mtime_ns, algo, hashval),
        )
        ok = cur.fetchone() is not None
        if ok:
            self.lru.put(key)
        return ok

    # ------------------- snapshots -------------------

    @with_retry()
    def get_or_create_snapshot(self, name: str, path: Optional[str] = None) -> int:
        cur = self.meta_conn.cursor()
        cur.execute("SELECT id FROM snapshots WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO snapshots(name, path) VALUES (?, ?)", (name, path))
        self.meta_conn.commit()
        return cur.lastrowid

    @with_retry()
    def replace_snapshot(self, name: str, path: Optional[str] = None) -> int:
        self.delete_snapshot(name)
        return self.get_or_create_snapshot(name, path)

    def get_snapshot_id(self, name: str) -> Optional[int]:
        cur = self.meta_conn.cursor()
        cur.execute("SELECT id FROM snapshots WHERE name=?", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    @with_retry()
    def delete_snapshot(self, name: str) -> None:
        cur = self.meta_conn.cursor()
        cur.execute("DELETE FROM snapshots WHERE name=?", (name,))
        self.meta_conn.commit()
        # no need to delete per-shard rows; they are keyed by snapshot_id

    @with_retry()
    def add_to_snapshot(
        self,
        snapshot_id: int,
        device: int,
        inode: int,
        path: str,
        size: int,
        mtime_ns: int,
    ) -> None:
        nk = NaturalKey(device, inode)
        shard, conn = self._conn_for_key(nk)
        conn.execute(
            """
            INSERT OR IGNORE INTO snapshot_files
                (snapshot_id, device, inode, path, size, mtime_ns)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, device, inode, path, size, mtime_ns),
        )

    @with_retry()
    def batch_add_snapshot_links(
        self, snapshot_id: int, entries: Iterable[Tuple[int, int, str, int, int]]
    ) -> None:
        """Batch insert snapshot membership.
        entries: iterable of (device, inode, path, size, mtime_ns)
        """
        # group by shard to avoid cross-DB executemany
        buckets: Dict[str, List[Tuple[int, int, int, str, int, int]]] = {}
        for device, inode, path, size, mtime_ns in entries:
            nk = NaturalKey(device, inode)
            shard = nk.shard_key()
            buckets.setdefault(shard, []).append(
                (snapshot_id, device, inode, path, size, mtime_ns)
            )
        for shard, rows in buckets.items():
            conn = self._open_shard(shard)
            conn.executemany(
                """
                INSERT OR IGNORE INTO snapshot_files
                    (snapshot_id, device, inode, path, size, mtime_ns)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def list_snapshots(self) -> List[dict]:
        cur = self.meta_conn.cursor()
        cur.execute(
            "SELECT id, name, path, created_at FROM snapshots ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
        return [
            {"id": r[0], "name": r[1], "path": r[2], "created_at": r[3]} for r in rows
        ]

    # ------------------- snapshot diff (added / removed / moved / changed) ---

    def _all_shards(self) -> List[str]:
        # Open-on-demand; but for diff we may need to iterate the existing shard files
        files = []
        for name in os.listdir(self.shards_dir):
            if name.endswith(".db") and len(name) >= 3:
                files.append(name[:-3])
        # include any already-open shards too
        files.extend([k for k in self._shard_conns.keys() if k not in files])
        return sorted(set(files))

    def diff_snapshots(self, name1: str, name2: str, force: bool = False) -> dict:
        cur = self.meta_conn.cursor()
        cur.execute("SELECT id, path FROM snapshots WHERE name=?", (name1,))
        row1 = cur.fetchone()
        cur.execute("SELECT id, path FROM snapshots WHERE name=?", (name2,))
        row2 = cur.fetchone()
        if not row1 or not row2:
            raise ValueError(f"Snapshot not found: {name1 if not row1 else name2}")
        id1, root1 = row1
        id2, root2 = row2
        if root1 != root2 and not force:
            raise ValueError(f"Snapshots have different paths: {root1} != {root2}")

        added: List[str] = []
        removed: List[str] = []
        moved: List[Tuple[str, str]] = []  # (old_path, new_path)
        changed: List[str] = []

        # Iterate all shard DBs and aggregate diffs
        for shard in self._all_shards():
            conn = self._open_shard(shard)
            c = conn.cursor()

            # changed: same NK in both snapshots, but hash differs
            c.execute(
                """
                SELECT f1.path
                  FROM snapshot_files s1
                  JOIN snapshot_files s2
                    ON s2.snapshot_id=? AND s2.device=s1.device AND s2.inode=s1.inode
                  JOIN files f1 ON f1.device=s1.device AND f1.inode=s1.inode
                  JOIN files f2 ON f2.device=s2.device AND f2.inode=s2.inode
                 WHERE s1.snapshot_id=?
                   AND f1.hash IS NOT NULL AND f2.hash IS NOT NULL
                   AND f1.hash != f2.hash
                """,
                (id2, id1),
            )
            changed.extend([r[0] for r in c.fetchall()])

            # moved: same NK in both, but path differs
            c.execute(
                """
                SELECT s1.path, s2.path
                  FROM snapshot_files s1
                  JOIN snapshot_files s2
                    ON s2.snapshot_id=? AND s2.device=s1.device AND s2.inode=s1.inode
                 WHERE s1.snapshot_id=? AND s1.path <> s2.path
                """,
                (id2, id1),
            )
            moved.extend([(r[0], r[1]) for r in c.fetchall()])

            # added: in id2 not in id1
            c.execute(
                """
                SELECT s2.path
                  FROM snapshot_files s2
                  LEFT JOIN snapshot_files s1
                    ON s1.snapshot_id=? AND s1.device=s2.device AND s1.inode=s2.inode
                 WHERE s2.snapshot_id=? AND s1.device IS NULL
                """,
                (id1, id2),
            )
            added.extend([r[0] for r in c.fetchall()])

            # removed: in id1 not in id2
            c.execute(
                """
                SELECT s1.path
                  FROM snapshot_files s1
                  LEFT JOIN snapshot_files s2
                    ON s2.snapshot_id=? AND s2.device=s1.device AND s2.inode=s1.inode
                 WHERE s1.snapshot_id=? AND s2.device IS NULL
                """,
                (id2, id1),
            )
            removed.extend([r[0] for r in c.fetchall()])

        # de-dup results
        added_set = set(added)
        removed_set = set(removed)
        changed_set = set(changed)
        moved_set = set(moved)

        # If a path appears in both added and removed but not changed, treat as moved (already handled above)
        both = added_set & removed_set
        added_set -= both
        removed_set -= both

        return {
            "added": sorted(added_set),
            "removed": sorted(removed_set),
            "moved": sorted(list(moved_set)),
            "changed": sorted(changed_set),
        }

    # ------------------- admin / legacy-compatible helpers -------------------

    def flush(self) -> None:
        try:
            self.meta_conn.commit()
            for conn in list(self._shard_conns.values()):
                conn.commit()
        except sqlite3.Error as e:
            logger.warning(f"SQLite flush error: {e}")

    def commit(self) -> None:
        self.flush()

    def close(self) -> None:
        self.flush()
        try:
            self.meta_conn.close()
        finally:
            for conn in list(self._shard_conns.values()):
                try:
                    conn.close()
                except Exception:
                    pass

    # ---- Legacy path-based APIs (discouraged in sharded mode) ----

    def get(self, path: str, mtime: float, algo: str) -> Optional[str]:
        """Path-based lookup is ambiguous in sharded mode (we shard by device/inode).
        We fall back to scanning shards by path and mtime, which is O(#shards) and
        intended only for admin tooling. Prefer `get_hash(device, inode, mtime_ns, algo)`.
        """
        # Convert seconds->ns if float seconds provided
        mtime_ns = int(
            mtime if isinstance(mtime, int) else round(mtime * 1_000_000_000)
        )
        for shard in self._all_shards():
            conn = self._open_shard(shard)
            cur = conn.cursor()
            cur.execute(
                "SELECT hash FROM files WHERE path=? AND mtime_ns=? AND algo=?",
                (path, mtime_ns, algo),
            )
            row = cur.fetchone()
            if row:
                return row[0]
        return None

    def has(self, path: str, mtime: float, algo: str, hashval: str) -> bool:
        mtime_ns = int(
            mtime if isinstance(mtime, int) else round(mtime * 1_000_000_000)
        )
        for shard in self._all_shards():
            conn = self._open_shard(shard)
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM files WHERE path=? AND mtime_ns=? AND algo=? AND hash=?",
                (path, mtime_ns, algo, hashval),
            )
            if cur.fetchone() is not None:
                return True
        return False

    # Legacy merge is not applicable in sharded mode; provide a helper to import a legacy DB
    @with_retry(retries=10)
    def import_legacy_sqlite(self, path: str, default_device: int = 0) -> int:
        """Import rows from a *legacy single-file* cache (path,mtime,algo,hash,size,inode)
        into the sharded structure. Returns number of imported rows.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        conn = self.meta_conn
        conn.execute("ATTACH DATABASE ? AS tempdb", (path,))
        cur = conn.cursor()
        cur.execute("SELECT path, mtime, algo, hash, size, inode FROM tempdb.files")
        rows = cur.fetchmany(100_000)
        imported = 0
        while rows:
            batch = []
            for p, mtime, algo, hv, sz, ino in rows:
                device = default_device
                mtime_ns = int(
                    mtime
                    if isinstance(mtime, int)
                    else round(float(mtime) * 1_000_000_000)
                )
                if ino is None:
                    ino = 0
                batch.append(
                    (
                        device,
                        int(ino),
                        p,
                        int(sz) if sz is not None else 0,
                        mtime_ns,
                        str(algo),
                        hv,
                    )
                )
            # group per shard
            buckets: Dict[str, List[Tuple[int, int, str, int, int, Optional[str]]]] = {}
            for device, inode, p, sz, mt, algo, hv in batch:
                shard = NaturalKey(device, inode).shard_key()
                buckets.setdefault(shard, []).append(
                    (device, inode, p, sz, mt, algo, hv)
                )
            for shard, rows2 in buckets.items():
                sconn = self._open_shard(shard)
                sconn.executemany(
                    """
                    INSERT INTO files (device, inode, algo, hash, size, mtime_ns, path, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
                    ON CONFLICT(device, inode) DO UPDATE SET
                        algo=excluded.algo,
                        hash=excluded.hash,
                        size=excluded.size,
                        mtime_ns=excluded.mtime_ns,
                        path=excluded.path,
                        updated_at=strftime('%s','now')
                    """,
                    [(d, i, a, h, s, mt, p) for (d, i, p, s, mt, a, h) in rows2],
                )
                imported += len(rows2)
            rows = cur.fetchmany(100_000)
        conn.execute("DETACH DATABASE tempdb")
        return imported

    # ------------------- search/query helpers -------------------

    def query(
        self, pattern: str, algo: Optional[str] = None, since: Optional[str] = None
    ) -> List[sqlite3.Row]:
        """Scan shards for files matching a glob pattern. Intended for admin tooling."""
        if since:
            try:
                ts = datetime.fromisoformat(since).timestamp()
                since_ns = int(round(ts * 1_000_000_000))
            except ValueError:
                raise ValueError(f"Invalid --since datetime: {since}")
        else:
            since_ns = None

        rows_out: List[sqlite3.Row] = []
        for shard in self._all_shards():
            conn = self._open_shard(shard)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            sql = "SELECT * FROM files"
            filters = []
            params: List[object] = []
            if algo:
                filters.append("algo = ?")
                params.append(algo)
            if since_ns is not None:
                filters.append("updated_at >= ?")
                params.append(int(round(since_ns / 1_000_000_000)))
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            cur.execute(sql, tuple(params))
            for row in cur.fetchall():
                if fnmatch.fnmatch(row["path"], pattern):
                    rows_out.append(row)
        return rows_out

    # Convenience to enumerate all natural keys (rarely used)
    def get_all_file_keys(self) -> List[NaturalKey]:
        out: List[NaturalKey] = []
        for shard in self._all_shards():
            conn = self._open_shard(shard)
            cur = conn.cursor()
            cur.execute("SELECT device, inode FROM files")
            out.extend(NaturalKey(d, i) for d, i in cur.fetchall())
        return out
