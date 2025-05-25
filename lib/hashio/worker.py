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
import time
from multiprocessing import Event, Lock, Pool, Process, Queue, Value

from hashio import config, utils
from hashio.encoder import checksum_file, get_encoder_class
from hashio.exporter import BaseExporter, get_exporter_class
from hashio.logger import logger
from hashio.utils import get_metadata, normalize_path

# wait time in seconds to check for queue emptiness
WAIT_TIME = 0.25

# cache the current working directory
CWD = os.getcwd()


def writer_process(
    queue: Queue,
    exporter: BaseExporter,
    flush_interval: float = 1.0,
    batch_size: int = 100,
):
    """A process that writes data from a queue to an exporter.

    :param queue: a multiprocessing Queue containing data to write
    :param exporter: an instance of an exporter to write data to
    :param flush_interval: time interval in seconds to flush data
    :param batch_size: number of items to collect before flushing
    """
    buffer = []
    last_flush = time.time()

    from queue import Empty

    # ensure the exporter is open
    while True:
        try:
            item = queue.get(timeout=flush_interval)
            if item == "__DONE__":
                break
            buffer.append(item)
        except Empty:
            pass
        except Exception as e:
            logger.error("write error: %s", e)

        if len(buffer) >= batch_size or (time.time() - last_flush) >= flush_interval:
            for path, data in buffer:
                exporter.write(path, data)
            buffer.clear()
            last_flush = time.time()

    # final flush
    for path, data in buffer:
        exporter.write(path, data)


class HashWorker:
    """A multiprocessing hash worker class.

    >>> w = HashWorker(path, outfile="hash.json")
    >>> w.run()
    >>> pprint(w.results)
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
        self.encoder = get_encoder_class(algo)()
        self.exporter = get_exporter_class(os.path.splitext(outfile)[1])(outfile)
        self.queue = Queue()  # queue for tasks
        self.result_queue = Queue()  # queue for results
        self.pool = Pool(self.procs, HashWorker.main, (self,))
        self.done = Event()
        self.writer = Process(
            target=writer_process, args=(self.result_queue, self.exporter)
        )  # process to write results

    def __str__(self):
        """Returns a string representation of the worker."""
        p_name = multiprocessing.current_process().name
        return f"<HashWorker {p_name}>"

    def add_to_queue(self, data: dict):
        """Adds task data to the queue."""
        with self.lock:
            self.queue.put(data)

    def add_path_to_queue(self, path: str):
        """
        Add a directory to the search queue.

        :param path: search path
        """
        self.add_to_queue({"task": "search", "path": path})

    def add_hash_to_queue(self, path: str):
        """
        Add a filename to the hash queue.

        :param path: file path
        """
        self.pending += 1
        self.queue.put({"task": "hash", "path": path})

    def explore_path(self, path: str):
        """
        Walks a dir and adds files to hash queue, and returns a list of found
        subdirs to add to the search queue.

        :param path: search path
        """
        directories = []
        nondirectories = []
        if self.force:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    fullname = os.path.join(path, filename)
                    if os.path.isdir(fullname):
                        directories.append(fullname)
                    else:
                        nondirectories.append(fullname)
            else:
                nondirectories.append(path)
            for filename in nondirectories:
                self.add_hash_to_queue(filename)
        else:
            for filename in utils.walk(path, filetype="f"):
                self.add_hash_to_queue(filename)
        return directories

    def do_hash(self, path: str):
        """
        Checksums a given path. Writes the checksum and file metadata to the
        exporter.

        :param path: file path
        """
        value = checksum_file(path, self.encoder)

        # normalize path to be relative to the start directory
        npath = normalize_path(path, start=self.start)

        # get metadata for the file
        metadata = get_metadata(path)
        metadata.update({self.encoder.name: value})

        # print progress to stdout
        if self.verbose:
            print(f"{value}  {npath}")

        with self.lock:
            # if the start directory is not the current working directory,
            # write the normalized path, otherwise write the original path
            if self.start != CWD:
                self.result_queue.put((npath, metadata))
            else:
                self.result_queue.put((path, metadata))

            # decrement pending count
            self.pending -= 1

        # update progress
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
        """Stops worker."""
        logger.info("stopping %s", str(self))
        self.pool.terminate()
        self.exporter.close()
        self.total_time = time.time() - self.start_time

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
                    dirs = worker.explore_path(path)
                    for newdir in dirs:
                        worker.add_path_to_queue(newdir)
                elif task == "hash":
                    worker.do_hash(path)
            except queue.Empty:
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
    stats.print_stats(20)  # top 20 cumulative time consumers


if __name__ == "__main__":
    import sys

    run_profiled(sys.argv[1])
