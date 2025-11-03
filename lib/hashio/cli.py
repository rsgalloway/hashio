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

from hashio import __version__, config, utils
from hashio.cache import Cache
from hashio.encoder import get_encoder_class, verify_caches, verify_checksums
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
        "-s",
        "--summarize",
        action="store_true",
        help="show summary of results",
    )
    parser.add_argument(
        "--update-cache",
        action="store_true",
        help="apply the latest schema updates and create any missing indexes",
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
        "--merge",
        metavar="CACHE",
        help="Merge another cache database into the current one",
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
        desc=worker.path,
        unit="B",  # base unit
        unit_scale=True,  # auto-scale (KB, MB, GB...)
        unit_divisor=1024,  # use 1024-based units (KiB, MiB)
        smoothing=0.1,
        position=position,
        dynamic_ncols=True,
        leave=True,
    )

    last_time = time.time()
    last_files = worker.progress_count()

    def watch():
        nonlocal last_time, last_files

        # wait for worker done event with a timeout
        while not worker.done.wait(timeout=update_interval):
            now = time.time()
            files_now = worker.progress_count()
            bytes_now = worker.progress_bytes()

            elapsed = now - last_time
            file_delta = files_now - last_files
            files_per_sec = file_delta / elapsed if elapsed > 0 else 0

            # tqdm expects raw byte count for n
            pbar.n = bytes_now
            filename = os.path.basename(worker.progress_filename())[:50]
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
        # avoid closing the progress bar immediately
        # pbar.close()

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


def main():
    """Main thread."""

    args = parse_args()

    # validate and update the cache
    if args.update_cache:
        cache = Cache()
        cache._ensure_db()
        cache.close()
        print("Cache DB updated.")
        return 0

    # merge another cache into the current one
    elif args.merge:
        if not os.path.exists(args.merge):
            print(f"Cache file not found: {args.merge}")
            return 2
        cache = Cache()
        cache.merge(args.merge)
        print(f"Merged into {cache.db_path}")
        return 0

    # query cache or list all entries
    elif args.query:
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
    paths = list(dict.fromkeys(args_dict.pop("path")))

    # validate algorithm
    if not get_encoder_class(args_dict["algo"]):
        print(f"Unsupported algorithm: {args_dict['algo']}")
        return 2

    progress_threads = []
    worker_threads = []
    workers = []

    verbose = 2 if all(os.path.isfile(p) for p in paths) else args_dict["verbose"]

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
                verbose=verbose,
            )
            workers.append(worker)

            # if verbose, disable watcher and use tqdm directly
            do_watcher = not verbose and os.path.isdir(path) and sys.stdout.isatty()

            if do_watcher:
                t = start_progress_thread(worker, position=i)
                progress_threads.append(t)

            thread = threading.Thread(target=worker.run)
            thread.start()
            worker_threads.append(thread)

        # wait for workers to finish
        while any(t.is_alive() for t in worker_threads):
            # wait for all workers to finish
            for t in worker_threads:
                t.join(timeout=0.2)

            # now wait for progress threads to exit
            for t in progress_threads:
                t.join()

    except KeyboardInterrupt:
        for worker in workers:
            worker.stop()
        for t in progress_threads:
            t.join(timeout=0.2)
        print("stopping...")

    finally:
        if len(paths) > 1 and not (args.verbose or args.summarize):
            print("")

        if args.summarize:
            total_files = sum(w.progress_count() for w in workers)
            total_bytes = sum(w.progress_bytes() for w in workers)
            total_time = sum(w.total_time for w in workers if hasattr(w, "total_time"))

            print(f"total files: {total_files}")
            print(f"total bytes: {utils.format_bytes(total_bytes)}")
            print(f"total time:  {total_time:.2f} seconds")
            print(
                f"avg files/s: {total_files / total_time:.2f}"
                if total_time
                else "  Avg files/s: N/A"
            )
            print(
                f"avg MB/s:    {(total_bytes / 1024 / 1024) / total_time:.2f}"
                if total_time
                else "  Avg MB/s:    N/A"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
