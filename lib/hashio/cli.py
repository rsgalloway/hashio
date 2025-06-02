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
from hashio.logger import logger
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
        nargs="?",
        help="path to directory or files to checksum",
        default=os.getcwd(),
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
        action="store_true",
        help="verbose output",
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
        nargs="+",
        metavar="SNAPSHOT",
        help="Diff snapshot(s) or current cache.",
    )
    cache_group.add_argument(
        "--since",
        help="Filter results since an ISO datetime (e.g. YYYY-MM-DDTHH:MM:SS)",
    )

    args = parser.parse_args(argv)
    return args


def start_progress_thread(worker, update_interval=0.2):
    """Start a thread to monitor worker.progress.value and update tqdm.

    :param worker: The HashWorker instance to monitor.
    :param update_interval: How often to update the progress bar (in seconds).
    :return: The thread that updates the progress bar.
    """
    pbar = tqdm(desc="hashing files", unit="file", dynamic_ncols=True)

    def watch():
        while not worker.is_done():
            pbar.n = worker.progress_count()
            pbar.refresh()
            time.sleep(update_interval)

        # final update to catch any remaining progress
        pbar.n = worker.progress_count()
        pbar.refresh()
        pbar.close()

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


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
                print(f"name: {snap['name']} (id: {snap['id']}) - created at {ts}")
        else:
            print("No snapshots found.")
        return 0

    # diff two snapshots
    elif args.diff:
        cache = Cache()

        # diff snapshot vs current state of cache
        if len(args.diff) == 1 or args.diff[1] == ".":
            snapname = args.diff[0]
            snapshot_id = cache.get_snapshot_id(snapname)
            if snapshot_id is None:
                print(f"Snapshot not found: {snapname}")
                sys.exit(1)

            # build a temporary snapshot from current files
            head_name = "__head__"
            head_id = cache.get_or_create_snapshot(head_name)
            file_ids = cache.get_all_file_ids()
            cache.batch_add_snapshot_links(head_id, file_ids)

            diff = cache.diff_snapshots(snapname, head_name)

            # cleanup head snapshot
            cache.delete_snapshot(head_name)

        # diff two snapshots
        elif len(args.diff) == 2:
            diff = cache.diff_snapshots(args.diff[0], args.diff[1])

            for path in diff["added"]:
                print(f"+ {path}")
            for path in diff["removed"]:
                print(f"- {path}")
            for path in diff["changed"]:
                print(f"~ {path}")

        else:
            print("Use --diff with 1 or 2 snapshot names.")
            sys.exit(1)

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

    if not os.path.exists(args.path):
        print(f"path does not exist: {args.path}")
        return 2

    if os.path.isfile(args.path):
        args.procs = 1
        args.verbose = True

    if utils.is_ignorable(args.path) and not args.force:
        print(f"path is ignorable: {args.path}")
        return 2

    if args.outfile and os.path.isdir(args.outfile):
        print(f"output file cannot be a directory: {args.outfile}")
        return 2

    if not get_encoder_class(args.algo):
        print(f"unsupported hash algorithm: {args.algo}")
        return 2

    # create hash worker and generate checksums
    # TODO: support progress callback: HashWorker(progress_callback=update_progress)
    worker = HashWorker(
        args.path,
        args.outfile,
        procs=args.procs,
        start=args.start,
        algo=args.algo,
        snapshot=args.snapshot,
        force=args.force,
        verbose=args.verbose,
    )

    # if verbose, disable watcher and use tqdm directly
    do_watcher = not args.verbose and os.path.isdir(args.path) and sys.stdout.isatty()

    try:
        if do_watcher:
            progress_thread = start_progress_thread(worker)
        worker.run()
        if do_watcher:
            progress_thread.join()

    except KeyboardInterrupt:
        worker.stop()
        print("\nstopping...")
        sys.exit(0)

    finally:
        logger.debug("done in %s seconds", worker.total_time)

    return 0


if __name__ == "__main__":
    sys.exit(main())
