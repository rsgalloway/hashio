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

import multiprocessing
import os
import queue
import sqlite3
import time
from multiprocessing import Event, Lock, Pool, Process, Queue, Value

from hashio import config, utils
from hashio.encoder import ENCODER_MAP, NullEncoder, checksum_file, get_encoder_class
from hashio.exporter import BaseExporter, get_exporter_class
from hashio.logger import logger
from hashio.utils import get_metadata, normalize_path

# wait time in seconds to check for queue emptiness
WAIT_TIME = 0.25

# cache the current working directory
CWD = os.getcwd()

# per-process global singletons for cache connections
_worker_cache = None


def get_worker_cache():
    """Returns the worker cache instance."""
    global _worker_cache
    if _worker_cache is None:
        from hashio.cache import Cache

        _worker_cache = Cache()
    return _worker_cache


def write_to_cache(cache, abspath, data):
    """Writes hash data to the cache.

    :param cache: Cache instance to write to
    :param abspath: absolute path of the file
    :param data: dictionary containing hash data
    """
    try:
        mtime = data.get("mtime")
        size = data.get("size")
        inode = data.get("ino")
        for algo, hashval in data.items():
            if algo in ENCODER_MAP:
                if not cache.has(abspath, mtime, algo, hashval):
                    cache.put(abspath, mtime, algo, hashval, size, inode)
                    cache.flush()
    except sqlite3.Error as e:
        logger.warning(f"Cache write error for {abspath}: {e}")


def writer_process(
    queue: Queue,
    exporter: BaseExporter,
    flush_interval: float = 1.0,
    batch_size: int = 100,
    use_cache: bool = True,
):
    """A process that writes data from a queue to an exporter.

    :param queue: a multiprocessing Queue containing data to write
    :param exporter: an instance of an exporter to write data to
    :param flush_interval: time interval in seconds to flush data
    :param batch_size: number of items to collect before flushing
    :param use_cache: whether to use cache for writing data
    """
    buffer = []
    last_flush = time.time()

    from queue import Empty

    from hashio.cache import Cache

    # instantiate the cache db
    cache = Cache() if use_cache else None

    # ensure the exporter is open
    while True:
        try:
            item = queue.get(timeout=flush_interval)
            if item == "__DONE__":
                break
            buffer.append(item)
        except Empty:
            pass
        except (KeyboardInterrupt, EOFError):
            break  # clean exit
        except Exception as e:
            logger.error("write error: %s", e)

        if len(buffer) >= batch_size or (time.time() - last_flush) >= flush_interval:
            for npath, abspath, data in buffer:
                exporter.write(npath, data)
                if cache:
                    write_to_cache(cache, abspath, data)

            buffer.clear()
            last_flush = time.time()

    # final flush
    for npath, abspath, data in buffer:
        exporter.write(npath, data)
        if cache:
            write_to_cache(cache, abspath, data)

    # close the cache db
    if cache:
        try:
            cache.commit()
            cache.close()
        except sqlite3.Error as e:
            logger.warning(f"Cache finalization error: {e}")


class HashWorker:
    """A multiprocessing hash worker class.

    >>> w = HashWorker(path, outfile="hash.json")
    >>> w.run()
    """

    def __init__(
        self,
        path: str = os.getcwd(),
        outfile: str = config.CACHE_FILENAME,
        procs: int = config.MAX_PROCS,
        start: str = None,
        algo: str = config.DEFAULT_ALGO,
        force: bool = False,
        verbose: bool = False,
    ):
        """Initializes a HashWorker instance.

        :param path: path to search for files to hash
        :param outfile: output file to write results to
        :param procs: maximum number of processes to use
        :param start: starting path for relative paths in output
        :param algo: hashing algorithm to use
        :param force: hash all files including ignorable patterns
        :param verbose: if True, print verbose output
        """
        self.path = path
        self.algo = algo
        self.outfile = outfile
        self.procs = procs
        self.force = force
        self.lock = Lock()
        self.start_time = 0.0
        self.total_time = 0.0
        self.pending = 0
        self.verbose = verbose
        self.progress = Value("i", 0)  # shared int for progress
        self.start = start or os.path.relpath(path)
        self.exporter = get_exporter_class(os.path.splitext(outfile)[1])(outfile)
        self.queue = Queue()  # task queue
        self.result_queue = Queue()  # write queue
        self.pool = Pool(self.procs, HashWorker.main, (self,))
        self.done = Event()
        self.writer = Process(
            target=writer_process, args=(self.result_queue, self.exporter)
        )

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
        for filename in utils.walk(path, filetype="f", force=self.force):
            self.add_hash_to_queue(filename)

    def do_hash(self, path: str):
        """Hashes a file and puts the result in the result queue.

        :param path: file path to hash
        """
        metadata = get_metadata(path)
        mtime = metadata["mtime"]

        # get the worker cache instance
        cache = get_worker_cache()

        # nrmalize the path for consistent output
        normalized_path = normalize_path(path, start=self.start)

        cached_hash = None
        if cache:
            cached_hash = cache.get(path, mtime, self.algo)

        # if the hash is cached, use it; otherwise compute it
        if cached_hash:
            metadata[self.algo] = cached_hash
            value = cached_hash
            extra = "(cached)"
        else:
            encoder = get_encoder_class(self.algo)()
            value = checksum_file(path, encoder)
            metadata[self.algo] = value
            extra = ""

        if self.verbose:
            print(f"{value}  {normalized_path} {extra}")

        with self.lock:
            abs_path = os.path.abspath(path)
            self.result_queue.put((normalized_path, abs_path, metadata))
            self.pending -= 1

        with self.progress.get_lock():
            self.progress.value += 1

    def run(self):
        """Runs the worker."""
        self.start_time = time.time()
        self.writer.start()
        self.add_path_to_queue(self.path)
        self.pool.close()
        self.pool.join()
        self.result_queue.put("__DONE__")
        self.writer.join()
        self.queue.close()
        self.queue.join_thread()
        self.exporter.close()
        self.total_time = time.time() - self.start_time
        self.done.set()

    def stop(self):
        """Stops the worker and cleans up resources."""
        if self.pool:
            self.pool.terminate()
            self.pool.join()
        if self.writer:
            self.writer.terminate()
            self.writer.join()
        self.exporter.close()
        self.total_time = time.time() - self.start_time
        self.done.set()
        logger.debug("stopping %s", multiprocessing.current_process())

    @staticmethod
    def main(worker):
        """Worker function that walks folders and adds data to the queue."""
        while True:
            try:
                data = worker.queue.get(True, timeout=WAIT_TIME)
                if data == -1:
                    break
                task = data["task"]
                path = data["path"]
                if task == "search":
                    worker.explore_path(path)
                elif task == "hash":
                    worker.do_hash(path)
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
