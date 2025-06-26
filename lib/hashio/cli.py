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
Contains command line interface for hashio.
"""

import argparse
import os
import sys
import threading
import time

from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from hashio import __version__, config
from hashio.cache import Cache
from hashio.encoder import verify_caches, verify_checksums
from hashio.worker import HashWorker


def format_result(row):
    """Format a row from the cache query into a human-readable string."""
    # unpack row data
    fileid, path, mtime, algo, hashval, size, inode, updated_at = row
    ts = datetime.fromtimestamp(updated_at).isoformat()
    hval = f"{algo}:{hashval}"
    return f"{ts:<20} {inode:<10} {hval:<35} {path}"


def normalize_args(argv):
    """Rewrite `-or value` to `-o value -r value`."""
    new_argv = []
    skip = False
    for i, arg in enumerate(argv):
        if skip:
            skip = False
            continue
        if arg == "-or" and i + 1 < len(argv):
            val = argv[i + 1]
            new_argv.extend(["-o", val, "-r", os.path.dirname(val) or "."])
            skip = True
        else:
            new_argv.append(arg)
    return new_argv


def parse_args():
    """Parse command line arguments."""

    # normalize arguments to handle -or value as -o value -r value
    argv = normalize_args(sys.argv[1:])

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "path",
        type=str,
        metavar="PATH",
        default=[os.getcwd()],
        nargs="*",
        help="path(s) to directory or files to hash",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        type=str,
        metavar="OUTFILE",
        help="write results to OUTFILE",
        default=None,
    )
    parser.add_argument(
        "-p",
        "--procs",
        type=int,
        metavar="PROCS",
        help="max number of processes to use",
        default=config.MAX_PROCS,
    )
    parser.add_argument(
        "-r",
        "--start",
        type=str,
        metavar="START",
        help="starting directory for relative paths",
        default=os.getcwd(),
    )
    parser.add_argument(
        "-a",
        "--algo",
        type=str,
        metavar="ALGO",
        help="hashing algorithm to use (default %s)" % config.DEFAULT_ALGO,
        default=config.DEFAULT_ALGO,
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="skip ignorables",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="output verbosity: -v shows new hashes, -vv shows cached too",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )

    # argument group for verification
    group = parser.add_argument_group("verification")
    group.add_argument(
        "--verify",
        type=str,
        metavar="HASHFILE",
        nargs="*",
        help="verify checksums from a previously created hash file",
    )

    # mutually exclusive group for cache operations
    cache_group = parser.add_argument_group("cache")
    cache_group.add_argument(
        "-q",
        "--query",
        help="Query cache for path",
    )
    parser.add_argument(
        "--snapshot",
        metavar="NAME",
        help="Create a snapshot with given name",
    )
    parser.add_argument(
        "--list-snapshots",
        action="store_true",
        help="List all snapshots in the cache",
    )
    parser.add_argument(
        "--diff",
        nargs=2,
        metavar="SNAPSHOT",
        help="Diff snapshot(s) or current cache.",
    )
    cache_group.add_argument(
        "--since",
        help="Filter results since an ISO datetime (e.g. YYYY-MM-DDTHH:MM:SS)",
    )

    args = parser.parse_args(argv)
    return args


def start_progress_thread(
    worker: HashWorker, position: int = 0, update_interval: float = 0.2
):
    """Start a thread to monitor worker progress and update tqdm with MB/sec.

    :param worker: The HashWorker instance to monitor.
    :param update_interval: How often to update the progress bar (in seconds).
    :return: The thread that updates the progress bar.
    """

    pbar = tqdm(
        desc=f"hashing {worker.path}",
        unit="B",  # base unit
        unit_scale=True,  # auto-scale (KB, MB, GB...)
        unit_divisor=1024,  # use 1024-based units (KiB, MiB)
        smoothing=0.1,
        position=position,
        dynamic_ncols=True,
    )

    last_time = time.time()
    last_files = worker.progress_count()

    def watch():
        nonlocal last_time, last_files
        while not worker.is_done():
            now = time.time()
            files_now = worker.progress_count()
            bytes_now = worker.progress_bytes()

            elapsed = now - last_time
            file_delta = files_now - last_files
            files_per_sec = file_delta / elapsed if elapsed > 0 else 0

            # tqdm expects raw byte count for n
            pbar.n = bytes_now
            filename = os.path.basename(worker.progress_filename())
            pbar.set_postfix(
                {
                    "files/s": f"{files_per_sec:.2f}",
                    "file": filename,
                }
            )

            pbar.refresh()
            last_time = now
            last_files = files_now
            time.sleep(update_interval)

        # final update
        pbar.n = worker.progress_bytes()
        pbar.set_postfix(
            {
                "files/s": f"0.00",
            }
        )
        pbar.refresh()
        pbar.close()

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


def run_worker_for_path(path: str, args_dict: dict):
    """Run a HashWorker for a given path with the provided arguments.

    This function checks if the path exists, if it is ignorable, and if the
    specified hashing algorithm is supported. It then initializes a HashWorker
    and runs it, optionally starting a progress thread if the path is a directory
    and verbose output is not enabled.

    :param path: The path to process (file or directory).
    :param args_dict: Dictionary of arguments to pass to the worker.
    :return: Result message indicating success or failure.
    """
    import os
    from hashio.utils import is_ignorable
    from hashio.encoder import get_encoder_class
    from hashio.worker import HashWorker

    if not os.path.exists(path):
        return f"[{path}] path does not exist"

    if is_ignorable(path) and not args_dict["force"]:
        return f"[{path}] path is ignorable"

    if not get_encoder_class(args_dict["algo"]):
        return f"[{path}] unsupported hash algorithm: {args_dict['algo']}"

    # set verbosity based on whether path is a file or directory
    verbose = 2 if os.path.isfile(path) else args_dict["verbose"]

    # reduce procs if it's a file
    procs = 1 if os.path.isfile(path) else args_dict["procs"]

    # if verbose, disable watcher and use tqdm directly
    do_watcher = not verbose and os.path.isdir(path) and sys.stdout.isatty()

    worker = HashWorker(
        path,
        args_dict["outfile"],
        procs=procs,
        start=args_dict["start"],
        algo=args_dict["algo"],
        snapshot=args_dict["snapshot"],
        force=args_dict["force"],
        verbose=verbose,
    )

    try:
        if do_watcher:
            progress_thread = start_progress_thread(worker)
        worker.run()
        if do_watcher:
            progress_thread.join()
        return f"[{path}] done in {worker.total_time:.2f}s"
    except KeyboardInterrupt:
        worker.stop()
        return f"\n[{path}] interrupted"
    except Exception as e:
        return f"\n[{path}] error: {e}"


def main():
    """Main thread."""

    args = parse_args()

    # query cache or list all entries
    if args.query:
        cache = Cache()
        results = cache.query(args.query, args.algo, args.since)
        if results:
            print(f"{'timestamp':<20} {'inode':<10} {'hash':<35} path")
            for row in results:
                print(format_result(row))
        else:
            print("No matching entries")
        return 0

    # list all snapshots
    elif args.list_snapshots:
        cache = Cache()
        snapshots = cache.list_snapshots()
        if snapshots:
            for snap in snapshots:
                ts = datetime.fromtimestamp(snap["created_at"]).isoformat()
                print(f"{snap['name']}  {snap['path']} - created at {ts}")
        else:
            print("No snapshots found.")
        return 0

    # diff two snapshots
    elif args.diff:
        cache = Cache()

        # common root path for comparing diffs
        start_root = os.path.abspath(args.start or os.getcwd())
        diff = cache.diff_snapshots(args.diff[0], args.diff[1], force=args.force)

        def is_under_root(path, root):
            try:
                return os.path.commonpath([os.path.abspath(path), root]) == root
            except ValueError:
                # handles cases like windows drive mismatch
                return False

        # filter paths by start_root if provided
        for k in diff:
            diff[k] = [p for p in diff[k] if is_under_root(p, start_root)]

        for path in diff["added"]:
            print(f"+ {path}")
        for path in diff["removed"]:
            print(f"- {path}")
        for path in diff["changed"]:
            print(f"~ {path}")

        cache.close()
        sys.exit(0)

    # hash verification
    if args.verify is not None:
        if len(args.verify) == 0:
            if not os.path.exists(config.CACHE_FILENAME):
                print(f"file not found: {config.CACHE_FILENAME}")
                return 0
            for algo, value, miss in verify_checksums(
                config.CACHE_FILENAME, start=args.start
            ):
                print("{0} {1}".format(algo, miss))
        elif len(args.verify) == 1:
            for algo, value, miss in verify_checksums(args.verify[0], start=args.start):
                print("{0} {1}".format(algo, miss))
        elif len(args.verify) == 2:
            source = args.verify[0]
            other = args.verify[1]
            for algo, value, miss in verify_caches(source, other):
                print("{0} {1}".format(value, miss))
        else:
            print("incorrect number of arguments for --verify")
            return 2
        return 0

    args_dict = vars(args).copy()
    paths = args_dict.pop("path")

    progress_threads = []
    worker_threads = []
    workers = []

    try:
        for i, path in enumerate(paths):
            worker = HashWorker(
                path=path,
                outfile=args_dict["outfile"],
                procs=args_dict["procs"],
                start=args_dict["start"],
                algo=args_dict["algo"],
                snapshot=args_dict["snapshot"],
                force=args_dict["force"],
                verbose=args_dict["verbose"],
            )
            workers.append(worker)

            # if verbose, disable watcher and use tqdm directly
            do_watcher = (
                not args.verbose and os.path.isdir(path) and sys.stdout.isatty()
            )

            if do_watcher:
                t = start_progress_thread(worker, position=i)
                progress_threads.append(t)

            thread = threading.Thread(target=worker.run)
            thread.start()
            worker_threads.append(thread)

        # wait for workers to finish
        for thread in worker_threads:
            thread.join()

    except KeyboardInterrupt:
        print("\nstopping...")
        for worker in workers:
            worker.stop()
        sys.exit(0)

    finally:
        print("")

    return 0


if __name__ == "__main__":
    sys.exit(main())
