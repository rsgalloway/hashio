#!/usr/bin/env python
#
# Copyright (c) 2024-2026, Ryan Galloway (ryan@rsgalloway.com)
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
import time
from dataclasses import fields

from tqdm import tqdm
from datetime import datetime

from hashio import __version__, config, utils
from hashio.api import Hashio, common_root
from hashio.cache import SQLiteUnavailableError, require_sqlite_support
from hashio.encoder import get_encoder_class
from hashio.inputs import resolve_selection


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


def parse_args(argv=None):
    """Parse command line arguments."""

    # normalize arguments to handle -or value as -o value -r value
    argv = normalize_args(sys.argv[1:] if argv is None else argv)

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "path",
        type=str,
        metavar="PATH",
        default=[os.getcwd()],
        nargs="*",
        help="files, directories, wildcards, or quoted pyseq sequence references",
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
    parser.add_argument(
        "--uncompress",
        action="store_true",
        help="hash and verify the decompressed contents of .gz files",
    )

    # mutually exclusive group for cache operations
    cache_group = parser.add_argument_group("cache")
    cache_group.add_argument(
        "-q",
        "--query",
        help="Query cache for path",
    )
    parser.add_argument(
        "--db",
        metavar="DB",
        help="cache database path (default %s)" % config.DEFAULT_DB_PATH,
        default=config.DEFAULT_DB_PATH,
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
    cache_toggle_group = cache_group.add_mutually_exclusive_group()
    cache_toggle_group.add_argument(
        "--cache",
        dest="use_cache",
        action="store_true",
        default=config.DEFAULT_USE_CACHE,
        help=(
            "enable cache lookups and writes while hashing "
            f"(default: {'enabled' if config.DEFAULT_USE_CACHE else 'disabled'})"
        ),
    )
    cache_toggle_group.add_argument(
        "--no-cache",
        dest="use_cache",
        action="store_false",
        help="disable cache lookups and writes while hashing",
    )

    args = parser.parse_args(argv)
    return args


def main(argv=None):
    """Parse arguments, run the matching Hashio method, and print its results."""

    args = parse_args(argv)
    if args.snapshot:
        args.use_cache = True

    cache_requested = any(
        [
            args.use_cache,
            args.update_cache,
            bool(args.merge),
            bool(args.query),
            args.list_snapshots,
            bool(args.diff),
            bool(args.snapshot),
        ]
    )

    try:
        if cache_requested:
            require_sqlite_support()
    except SQLiteUnavailableError as e:
        print(str(e), file=sys.stderr)
        return 2

    # CLI argument names match the Hashio setting names
    hio = Hashio(**{f.name: getattr(args, f.name) for f in fields(Hashio)})

    # validate and update the cache
    if args.update_cache:
        hio.update_cache()
        print("Cache DB updated.")
        return 0

    # merge another cache into the current one
    elif args.merge:
        try:
            hio.merge(args.merge)
        except FileNotFoundError as e:
            print(e)
            return 2
        print(f"Merged into {os.path.abspath(hio.db)}")
        return 0

    # query cache or list all entries
    elif args.query:
        results = list(hio.query(args.query, args.since))
        if results:
            print(f"{'timestamp':<20} {'inode':<10} {'hash':<35} path")
            for row in results:
                print(format_result(row))
        else:
            print("No matching entries")
        return 0

    # list all snapshots
    elif args.list_snapshots:
        snapshots = list(hio.list_snapshots())
        if snapshots:
            for snap in snapshots:
                ts = datetime.fromtimestamp(snap["created_at"]).isoformat()
                print(f"{snap['name']}  {snap['path']} - created at {ts}")
        else:
            print("No snapshots found.")
        return 0

    # diff two snapshots
    elif args.diff:
        symbols = {"added": "+", "removed": "-", "changed": "~"}
        for change, path in hio.diff(*args.diff):
            print(f"{symbols[change]} {path}")
        return 0

    # hash verification
    if args.verify is not None:
        if len(args.verify) == 0:
            if not os.path.exists(config.CACHE_FILENAME):
                print(f"file not found: {config.CACHE_FILENAME}")
                return 0
            for algo, value, miss in hio.verify(config.CACHE_FILENAME):
                print("{0} {1}".format(algo, miss))
        elif len(args.verify) == 1:
            for algo, value, miss in hio.verify(args.verify[0]):
                print("{0} {1}".format(algo, miss))
        elif len(args.verify) == 2:
            for algo, value, miss in hio.verify(*args.verify):
                print("{0} {1}".format(value, miss))
        else:
            print("incorrect number of arguments for --verify")
            return 2
        return 0

    # resolved here too, to decide what to print
    try:
        selection = resolve_selection(args.path)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    paths = selection.paths

    # validate algorithm
    if not get_encoder_class(args.algo):
        print(f"Unsupported algorithm: {args.algo}")
        return 2

    if selection.sequences and args.algo == "null":
        print(
            "Sequence aggregates require a checksum algorithm, not null",
            file=sys.stderr,
        )
        return 2

    verbose = args.verbose
    if (not selection.sequences and all(os.path.isfile(p) for p in paths)) or (
        selection.sequences and verbose
    ):
        verbose = 2
    explicit = {os.path.normcase(p) for p in selection.files}

    pbar = None
    if not verbose and not selection.sequences and sys.stdout.isatty():
        pbar = tqdm(
            desc=common_root(paths),
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            smoothing=0.1,
            dynamic_ncols=True,
            leave=True,
        )

    total_files = total_bytes = 0
    aggregates = []
    error = None
    start_time = time.time()
    results = hio.hash(*args.path)
    try:
        for result in results:
            if result.kind == "sequence":
                aggregates.append(result)
                continue
            total_files += 1
            total_bytes += result.size
            if pbar is not None:
                pbar.update(result.size)
                pbar.set_postfix(file=os.path.basename(result.path)[:50])
            if (
                verbose >= 2
                or (verbose == 1 and not result.cached)
                or os.path.normcase(result.path) in explicit
            ):
                extra = "(cached)" if result.cached else ""
                print(f"{result.value}  {result.display} {extra}".rstrip())

    except KeyboardInterrupt:
        results.close()
        print("stopping...")
        return 130

    except ValueError as e:
        error = e

    finally:
        total_time = time.time() - start_time
        if pbar is not None:
            pbar.refresh()
        if (
            not selection.sequences
            and len(paths) > 1
            and not (args.verbose or args.summarize)
        ):
            print("")

        if args.summarize:
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

    if error is not None:
        print(str(error), file=sys.stderr)
        return 2

    for result in aggregates:
        print("{}  {}".format(result.value, result.path))

    return 0


if __name__ == "__main__":
    sys.exit(main())
