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
"""
HashWorker updated for **sharded SQLite cache with natural keys**.

Key changes:
- Uses `(st_dev, st_ino)` as stable natural keys across renames.
- Writes **directly** to the sharded cache (no more per-process temp DB merges).
- Creates the snapshot **once** before starting worker processes; children add
  membership rows using the shared `snapshot_id`.
- Skip-hash check uses `get_hash(device, inode, mtime_ns, algo)`.
"""

import ctypes
import multiprocessing
import os
import queue
import time
from typing import Tuple
from multiprocessing import Event, Lock, Pool, Manager, Queue, Value

from hashio import config, utils
from hashio.cache import Cache
from hashio.encoder import checksum_file, ENCODER_MAP
from hashio.logger import logger
from hashio.utils import get_metadata, normalize_path

# wait time in seconds to check for queue emptiness
WAIT_TIME = 0.25

# size of the shared memory buffer for current file path
CURRENT_FILE_BUFFER_SIZE = 512

# cache the current working directory
CWD = os.getcwd()


def get_encoder(algo: str):
    """Returns an encoder instance based on the algorithm."""
    from hashio.encoder import get_encoder_class

    encoder_class = get_encoder_class(algo)
    if not encoder_class:
        raise ValueError(f"Unsupported algorithm: {algo}")
    return encoder_class()


def get_exporter(outfile: str):
    """Returns an exporter instance based on the file extension."""
    from hashio.exporter import get_exporter_class

    if not outfile:
        return None
    ext = os.path.splitext(outfile)[1].lower()
    exporter_class = get_exporter_class(ext)
    if not exporter_class:
        raise ValueError(f"Unsupported file extension: {ext}")
    return exporter_class(outfile)


class HashWorker:
    """A multiprocessing hash worker class.

    >>> w = HashWorker(path, outfile="hash.json")
    >>> w.run()
    """

    def __init__(
        self,
        path: str = os.getcwd(),
        outfile: str = None,
        procs: int = config.MAX_PROCS,
        start: str = None,
        algo: str = config.DEFAULT_ALGO,
        snapshot: str = None,
        merge_interval: int = config.MERGE_INTERVAL,
        force: bool = False,
        verbose: int = 0,
    ):
        """Initializes a HashWorker instance.

        :param path: path to search for files to hash
        :param outfile: output file to write results to
        :param procs: maximum number of processes to use
        :param start: starting path for relative paths in output
        :param algo: hashing algorithm to use
        :param snapshot: snapshot name to use
        :param merge_interval: interval in seconds to COMMIT shard DBs
        :param force: hash all files including ignorable patterns
        :param verbose: verbosity level
        """
        self.path = path
        self.algo = algo
        self.buffer_size = utils.get_buffer_size(path=path)
        self.outfile = outfile
        self.snapshot = snapshot
        self.snapshot_id: int | None = None
        self.procs = procs
        self.force = force
        self.lock = Lock()
        self.start_time = 0.0
        self.total_time = 0.0
        self.pending = 0
        self.verbose = verbose
        self.progress = Value("i", 0)  # shared files counter
        self.bytes_hashed = Value(ctypes.c_ulonglong, 0)  # shared bytes counter
        self.merge_interval = merge_interval
        self.last_commit_time = 0.0
        self.current_file = multiprocessing.Array(
            ctypes.c_char, CURRENT_FILE_BUFFER_SIZE
        )
        self.start = start or os.path.relpath(path)
        self.exporter = get_exporter(outfile)
        self.queue: Queue = Queue()  # task queue shared by workers
        self.pool = None
        self.done = Event()

        # per-process cache handle (created lazily inside each child proc)
        self._cache = None  # type: ignore

    def __str__(self):
        p_name = multiprocessing.current_process().name
        return f"<HashWorker {p_name}>"

    def _get_cache(self) -> Cache:
        """Lazily initializes and returns the per-process Cache instance."""
        if self._cache is None:
            with self.lock:
                self._cache = Cache()
        return self._cache

    def add_to_queue(self, data: dict):
        """Adds data to the task queue."""
        with self.lock:
            self.queue.put(data)

    def add_path_to_queue(self, path: str):
        """Adds a path exploration task to the queue."""
        self.add_to_queue({"task": "search", "path": path})

    def add_hash_to_queue(self, path: str):
        """Adds a hash task to the queue."""
        self.pending += 1
        self.queue.put({"task": "hash", "path": path})

    def explore_path(self, path: str):
        """Explores a path and adds hash tasks for each file found."""
        for filename in utils.walk(path, filetype="f", force=self.force):
            self.add_hash_to_queue(filename)

    def _stat_keys(self, path: str, meta: dict) -> Tuple[int, int, int]:
        """Return (device, inode, mtime_ns). Uses metadata if present, otherwise os.stat."""
        try:
            device = int(meta.get("dev")) if meta.get("dev") is not None else None
            inode = int(meta.get("ino")) if meta.get("ino") is not None else None
            mtime_ns = meta.get("mtime_ns")
            if mtime_ns is None and meta.get("mtime") is not None:
                mtime_ns = int(round(float(meta["mtime"]) * 1_000_000_000))
            if device is None or inode is None or mtime_ns is None:
                st = os.stat(path, follow_symlinks=False)
                device = st.st_dev if device is None else device
                inode = st.st_ino if inode is None else inode
                # Prefer ns if available (Py3.8+)
                mtime_ns = getattr(
                    st, "st_mtime_ns", int(round(st.st_mtime * 1_000_000_000))
                )
            return int(device), int(inode), int(mtime_ns)
        except FileNotFoundError:
            # File disappeared between list and stat
            raise

    def do_hash(self, path: str):
        """Hashes a single file and enqueues a write task."""
        meta = get_metadata(path)
        size = int(meta["size"]) if "size" in meta else os.path.getsize(path)

        # normalize for output
        normalized_path = normalize_path(path, start=self.start)
        if normalized_path == self.outfile:
            return

        # update shared "currently hashing" string
        self.update_current_file(path)

        # natural keys
        try:
            device, inode, mtime_ns = self._stat_keys(path, meta)
        except FileNotFoundError:
            return

        # skip-hash via cache lookup
        cached_hash = None
        if not self.force:
            cache = self._get_cache()
            try:
                cached_hash = cache.get_hash(device, inode, mtime_ns, self.algo)
            except Exception as e:
                logger.debug("cache get_hash error: %s", e)
                cached_hash = None

        if cached_hash and not self.force:
            meta[self.algo] = cached_hash
            value = cached_hash
            extra = "(cached)"
            do_write = False  # still record snapshot membership even if cached? We'll enqueue write for snapshot.
        else:
            extra = ""
            do_write = True
            try:
                encoder = get_encoder(self.algo)
                value = checksum_file(path, encoder, buffer_size=self.buffer_size)
                meta[self.algo] = value
            except OSError as e:
                logger.debug(str(e))
                return

        if self.verbose >= 2 or (self.verbose == 1 and not cached_hash):
            print(f"{value}  {normalized_path} {extra}")

        # always enqueue a write so we upsert state and snapshot membership
        # (hash may be cached)
        self.queue.put(
            {
                "task": "write",
                "path": path,
                "normalized_path": normalized_path,
                "metadata": meta,
                "device": device,
                "inode": inode,
                "mtime_ns": mtime_ns,
                "size": size,
            }
        )
        self.pending -= 1

        with self.progress.get_lock():
            self.progress.value += 1
        with self.bytes_hashed.get_lock():
            self.bytes_hashed.value += size

        if self.exporter:
            self.exporter.write(normalized_path, meta)

    def write(self, data: dict):
        """Upsert file into sharded cache and record snapshot membership."""
        cache = self._get_cache()

        npath = data.get("normalized_path")
        meta = data.get("metadata") or {}
        device = int(data["device"])  # required
        inode = int(data["inode"])  # required
        mtime_ns = int(data["mtime_ns"])  # required
        size = int(data.get("size", 0))

        # Upsert one or more algorithms present in metadata (usually just self.algo)
        wrote = False
        for algo, hashval in meta.items():
            if algo not in ENCODER_MAP:
                continue
            cache.upsert_file(
                device=device,
                inode=inode,
                path=npath,  # store normalized for consistency
                size=size,
                mtime_ns=mtime_ns,
                algo=algo,
                hashval=hashval,
            )
            wrote = True

        # snapshot membership (only if snapshot was created)
        if self.snapshot_id is not None:
            cache.add_to_snapshot(
                self.snapshot_id, device, inode, npath, size, mtime_ns
            )

        # Periodic commit of shard DBs to keep WAL segments bounded
        now = time.time()
        if now - self.last_commit_time >= self.merge_interval:
            try:
                cache.commit()
            finally:
                self.last_commit_time = now

    def update_current_file(self, path: str):
        """Updates the shared current file path buffer."""
        encoded = path.encode("utf-8")[: CURRENT_FILE_BUFFER_SIZE - 1]
        with self.current_file.get_lock():
            self.current_file[: len(encoded)] = encoded
            self.current_file[len(encoded) :] = b"\x00" * (
                CURRENT_FILE_BUFFER_SIZE - len(encoded)
            )

    def merge(self):
        """Legacy. Commits Cache."""
        try:
            if self._cache is not None:
                self._cache.commit()
        except Exception:
            pass

    def run(self):
        """Runs the worker."""
        self.start_time = time.time()

        # prepare snapshot once (so children can reference its id)
        if self.snapshot:
            cache = Cache()
            # Replace to ensure a clean membership set for this run
            self.snapshot_id = cache.replace_snapshot(self.snapshot, self.path)
            cache.commit()

        # start worker pool **after** snapshot id exists
        self.pool = Pool(self.procs, HashWorker.main, (self,))

        # seed initial path
        self.add_path_to_queue(self.path)

        try:
            self.pool.close()
            self.pool.join()
        finally:
            self.merge()
            self.queue.close()
            self.queue.join_thread()
            self.total_time = time.time() - self.start_time
            self.done.set()
            if self.exporter:
                self.exporter.close()

    def stop(self):
        """Stops the worker."""
        if self.pool:
            self.pool.terminate()
            self.pool.join()
        self.total_time = time.time() - self.start_time
        self.done.set()
        if not self.verbose:
            logger.info("Committing results to cache...")
        self.merge()
        logger.debug("Stopping %s", multiprocessing.current_process())

    def progress_count(self):
        """Returns the number of files hashed so far."""
        return self.progress.value

    def progress_bytes(self):
        """Returns the number of bytes hashed so far."""
        return self.bytes_hashed.value

    def progress_filename(self):
        """Returns the current filename being hashed."""
        with self.current_file.get_lock():
            raw = bytes(self.current_file[:])
            return raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")

    def is_done(self):
        """Returns True if the worker is done."""
        return self.done.is_set()

    @staticmethod
    def main(worker):
        """Worker function that walks folders and adds data to the queue."""
        while True:
            try:
                data = worker.queue.get(True, timeout=WAIT_TIME)
                if data == -1:
                    break
                task = data["task"]
                if task == "search":
                    worker.explore_path(data["path"])
                elif task == "hash":
                    worker.do_hash(data["path"])
                elif task == "write":
                    worker.write(data)
                elif task == "merge":
                    worker.merge()
            except queue.Empty:
                break
            except (KeyboardInterrupt, EOFError):
                break
            except Exception as err:
                logger.error(err)


def run_profiled(path: str):
    """Runs the HashWorker with profiling enabled."""
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    profiler.enable()

    worker = HashWorker(path=path, outfile=os.path.join(os.getcwd(), "hash.json"))
    worker.run()

    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)


if __name__ == "__main__":
    import sys

    run_profiled(sys.argv[1])
