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
from multiprocessing import Lock, Pool, Queue

from hashio import config, utils
from hashio.encoder import checksum_file, get_encoder_class
from hashio.exporter import get_exporter_class
from hashio.logger import logger
from hashio.utils import get_metadata, normalize_path

# wait time in seconds to check for queue emptiness
WAIT_TIME = 0.25

# cache the current working directory
CWD = os.getcwd()


class HashWorker:
    """A multiprocessing hash worker class.

    >>> w = HashWorker(path)
    >>> w.run()
    >>> pprint(w.results)
    """

    def __init__(
        self,
        path=os.getcwd(),
        outfile=config.CACHE_FILENAME,
        procs=config.MAX_PROCS,
        start=None,
        algo=config.DEFAULT_ALGO,
        force=False,
    ):
        self.path = path
        self.outfile = outfile
        self.procs = procs
        self.force = force
        self.start = start or os.path.relpath(path)
        self.encoder = get_encoder_class(algo)()
        self.exporter = get_exporter_class(os.path.splitext(outfile)[1])(outfile)
        self.queue = Queue()
        self.lock = Lock()
        self.start_time = 0.0
        self.total_time = 0.0
        self.results = None

    def __str__(self):
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
        self.add_to_queue({"task": "hash", "path": path})

    def explore_path(self, path: str):
        """Walks a dir and adds files to hash queue, and returns a list of
        found subdirs to add to the search queue.

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
        """Checksums a given path. Writes the checksum and file metadata to the
        exporter.

        :param path: file path
        """
        value = checksum_file(path, self.encoder)

        # normalize path to be relative to the start directory
        npath = normalize_path(path, start=self.start)

        # get metadata for the file
        metadata = get_metadata(path)
        metadata.update({self.encoder.name: value})

        print(f"{value}  {npath}")
        with self.lock:
            # if the start directory is not the current working directory,
            # write the normalized path, otherwise write the original path
            if self.start != CWD:
                self.exporter.write(npath, metadata)
            else:
                self.exporter.write(path, metadata)

    def reset(self):
        """Resets worker state."""
        self.results = None
        self.total_time = 0.0

    def run(self):
        """Starts worker processes in a blocking way and returns results."""
        if self.results and self.total_time:
            raise Exception(f"{self} completed")
        self.start_time = time.time()
        self.pool = Pool(self.procs, HashWorker.main, (self,))
        self.add_path_to_queue(self.path)
        self.wait_until_done()
        self.pool.close()
        self.pool.join()
        self.queue.close()
        self.queue.join_thread()
        self.exporter.close()
        self.total_time = time.time() - self.start_time
        self.results = self.exporter.read(self.outfile)

    def stop(self):
        """Stops worker."""
        logger.info("stopping %s", str(self))
        self.pool.terminate()
        self.exporter.close()
        self.total_time = time.time() - self.start_time
        self.results = self.exporter.read(self.outfile)

    def wait_until_done(self):
        """Blocks main process until queue is empty."""
        while True:
            time.sleep(WAIT_TIME)
            if self.queue.qsize() <= 0:
                break

    @staticmethod
    def main(worker):
        """Worker function that walks folders and adds data to the queue."""
        while True:
            try:
                data = worker.queue.get(True, timeout=WAIT_TIME)
                logger.debug("data %s", data)
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
