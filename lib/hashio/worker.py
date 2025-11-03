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
Contains hash worker class and functions.
"""

import ctypes
import multiprocessing
import os
import queue
import time
import uuid
from multiprocessing import Event, Lock, Pool, Manager, Queue, Value

from hashio import config, utils
from hashio.cache import Cache
from hashio.encoder import checksum_file
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
        :param merge_interval: interval in seconds to merge temporary caches
        :param force: hash all files including ignorable patterns
        :param verbose: if True, print verbose output
        """
        self.path = path
        self.algo = algo
        self.buffer_size = utils.get_buffer_size(path=path)
        self.outfile = outfile
        self.snapshot = snapshot
        self.procs = procs
        self.force = force
        self.lock = Lock()
        self.start_time = 0.0
        self.total_time = 0.0
        self.pending = 0
        self.verbose = verbose
        self.progress = Value("i", 0)  # shared files counter
        self.bytes_hashed = Value(ctypes.c_ulonglong, 0)  # shared bytes counter
        self.temp_cache = None
        self.temp_db_path = None
        self.temp_db_queue = Manager().Queue()
        self.merge_interval = merge_interval
        self.last_execution_time = 0
        self.current_file = multiprocessing.Array(
            ctypes.c_char, CURRENT_FILE_BUFFER_SIZE
        )
        self.start = start or os.path.relpath(path)
        self.exporter = get_exporter(outfile)
        self.queue = Queue()  # task queue
        self.pool = Pool(self.procs, HashWorker.main, (self,))
        self.done = Event()

    def __str__(self):
        """Returns a string representation of the worker."""
        p_name = multiprocessing.current_process().name
        return f"<HashWorker {p_name}>"

    def add_to_queue(self, data: dict):
        """Adds task data to the queue."""
        with self.lock:
            self.queue.put(data)

    def add_path_to_queue(self, path: str):
        """Add a directory to the search queue.

        :param path: search path
        """
        self.add_to_queue({"task": "search", "path": path})

    def add_hash_to_queue(self, path: str):
        """Add a filename to the hash queue.

        :param path: file path
        """
        self.pending += 1
        self.queue.put({"task": "hash", "path": path})

    def explore_path(self, path: str):
        """Walks a path and adds files to hash queue.

        :param path: search path
        """
        logger.debug("Exploring path %s", path)
        logger.debug("Using read buffer size: %s", self.buffer_size)
        logger.debug("Using database: %s", config.DEFAULT_DB_PATH)
        for filename in utils.walk(path, filetype="f", force=self.force):
            self.add_hash_to_queue(filename)

    def do_hash(self, path: str):
        """Hashes a file and puts the result in the result queue.

        :param path: file path to hash
        """
        metadata = get_metadata(path)
        mtime = metadata["mtime"]
        size = metadata["size"]

        # get the global cache instance
        with self.lock:
            cache = Cache()

        # normalize the path for consistent output
        normalized_path = normalize_path(path, start=self.start)

        # skip if the normalized path is the same as the outfile
        if normalized_path == self.outfile:
            return

        # update the current file in shared memory
        self.update_current_file(path)

        # check if the hash is cached
        cached_hash = None
        if cache and not self.force:
            abs_path = os.path.abspath(path)
            cached_hash = cache.get(abs_path, mtime, self.algo)

        # if the hash is cached, use it; otherwise compute it
        if cached_hash and not self.force:
            metadata[self.algo] = cached_hash
            value = cached_hash
            extra = "(cached)"
            do_write = False
        else:
            extra = ""
            do_write = True
            try:
                encoder = get_encoder(self.algo)
                value = checksum_file(path, encoder, buffer_size=self.buffer_size)
                metadata[self.algo] = value
            except OSError as e:
                logger.debug(str(e))
                return

        # print the result if verbose mode is enabled
        if self.verbose >= 2 or (self.verbose == 1 and not cached_hash):
            print(f"{value}  {normalized_path} {extra}")

        # write the result to the output file if required
        if do_write:
            with self.lock:
                abs_path = os.path.abspath(path)
                self.queue.put(
                    {
                        "task": "write",
                        "path": path,
                        "normalized_path": normalized_path,
                        "abs_path": abs_path,
                        "metadata": metadata,
                    }
                )
                self.pending -= 1

        # periodically merge into the main cache
        elif time.time() - self.last_execution_time >= self.merge_interval:
            self.merge()
            self.last_execution_time = time.time()

        with self.progress.get_lock():
            self.progress.value += 1

        with self.bytes_hashed.get_lock():
            self.bytes_hashed.value += size

        if self.exporter:
            self.exporter.write(normalized_path, metadata)

    def write(self, data: dict):
        """Write hash result to this worker's temp cache."""
        from hashio.encoder import ENCODER_MAP

        if self.temp_cache is None:
            pid = os.getpid()
            with self.lock:
                dbname = f"worker_{pid}_{uuid.uuid4().hex}.sql"
                self.temp_db_path = os.path.join(config.TEMP_CACHE_DIR, dbname)
                self.temp_cache = Cache(self.temp_db_path)
                logger.debug("Adding worker cache to queue: %s", self.temp_db_path)
                self.temp_db_queue.put(self.temp_db_path)

        npath = data.get("normalized_path")
        abspath = data.get("abs_path")
        meta = data.get("metadata")

        if not npath or not abspath or not meta:
            return

        mtime = meta.get("mtime")
        size = meta.get("size")
        inode = meta.get("ino")

        snapshot_id = None
        if self.snapshot and self.temp_cache:
            snapshot_id = self.temp_cache.replace_snapshot(self.snapshot, self.path)

        snapshot_file_ids = []

        for algo, hashval in meta.items():
            if algo not in ENCODER_MAP:
                continue

            if not self.temp_cache.has(abspath, mtime, algo, hashval):
                file_id = self.temp_cache.put_file_and_get_id(
                    abspath, mtime, algo, hashval, size, inode
                )
            else:
                file_id = self.temp_cache.get_file_id(abspath, mtime, algo)

            if snapshot_id and file_id:
                snapshot_file_ids.append(file_id)

        if snapshot_id and snapshot_file_ids:
            self.temp_cache.batch_add_snapshot_links(snapshot_id, snapshot_file_ids)

        with self.lock:
            self.temp_cache.commit()

    def update_current_file(self, path: str):
        """Updates the current file in shared memory."""
        encoded = path.encode("utf-8")[: CURRENT_FILE_BUFFER_SIZE - 1]
        with self.current_file.get_lock():
            self.current_file[: len(encoded)] = encoded
            self.current_file[len(encoded) :] = b"\x00" * (
                CURRENT_FILE_BUFFER_SIZE - len(encoded)
            )  # null-pad

    def merge(self):
        """Merges temporary caches into the main cache."""
        # final commit and close on temp dbs
        if self.temp_cache:
            with self.lock:
                self.temp_cache.commit()

        # merge the temporary caches into the main cache
        with self.lock:
            main_cache = Cache()

        merged_paths = set()

        while not self.temp_db_queue.empty():
            try:
                db_path = self.temp_db_queue.get_nowait()
                if db_path in merged_paths:
                    continue  # avoid merging same file twice
                with self.lock:
                    logger.debug("Merging %s into main cache", db_path)
                    main_cache.merge(db_path)
                    merged_paths.add(db_path)
                    os.remove(db_path)
            except queue.Empty:
                break

        if self.temp_cache:
            self.temp_cache = None

    def run(self):
        """Runs the worker."""
        self.start_time = time.time()
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
        """Stops the worker and cleans up resources."""
        if self.pool:
            self.pool.terminate()
            self.pool.join()
        self.total_time = time.time() - self.start_time
        self.done.set()
        if not self.verbose:
            logger.info("Merging results to central cache...")
        self.merge()
        logger.debug("Stopping %s", multiprocessing.current_process())

    def progress_count(self):
        """Returns the current progress count."""
        return self.progress.value

    def progress_bytes(self):
        """Returns the total bytes hashed so far."""
        return self.bytes_hashed.value

    def progress_filename(self) -> str:
        with self.current_file.get_lock():
            raw = bytes(self.current_file[:])
            return raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")

    def is_done(self):
        """Checks if the worker has completed its tasks."""
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
                break  # clean exit
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
    stats.print_stats(20)  # top 20 cumulative time consumers


if __name__ == "__main__":
    import sys

    run_profiled(sys.argv[1])
