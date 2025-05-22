#!/usr/bin/env python
#
# Copyright (c) 2024, Ryan Galloway (ryan@rsgalloway.com)
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

from hashio import __version__, config
from hashio.encoder import get_encoder_class, verify_caches, verify_checksums
from hashio.logger import logger
from hashio.worker import HashWorker


def parse_args():
    """sys.argv parser, returns args."""

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "path",
        type=str,
        metavar="PATH",
        nargs="?",
        help="path to checksum",
        default=os.getcwd(),
    )
    parser.add_argument(
        "-o",
        "--outfile",
        type=str,
        metavar="OUTFILE",
        help="write results to output OUTFILE",
        default=config.CACHE_FILENAME,
    )
    parser.add_argument(
        "--procs",
        type=int,
        metavar="PROCS",
        help="max number of spawned processes to use",
        default=config.MAX_PROCS,
    )
    parser.add_argument(
        "--start",
        type=str,
        metavar="START",
        help="starting directory for relative paths",
        default=os.getcwd(),
    )
    parser.add_argument(
        "--algo",
        type=str,
        metavar="ALGO",
        help="hashing algorithm to use (default %s)" % config.DEFAULT_ALGO,
        default=config.DEFAULT_ALGO,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="skip ignorables",
    )
    parser.add_argument("--verbose", action="store_true", help="verbose output")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )
    group = parser.add_argument_group("verification")
    group.add_argument(
        "--verify",
        type=str,
        metavar="HASHFILE",
        nargs="+",
        help="verify checksums from a previously created hash.json file",
    )

    args = parser.parse_args()
    return args


def main():
    """Main thread."""

    args = parse_args()

    if args.verbose:
        logger.setLevel(10)

    if args.verify:
        if len(args.verify) == 1:
            for algo, value, miss in verify_checksums(args.verify[0]):
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

    if not get_encoder_class(args.algo):
        print(f"unsupported hash algorithm: {args.algo}")
        return 2

    worker = HashWorker(
        args.path,
        args.outfile,
        procs=args.procs,
        start=args.start,
        algo=args.algo,
        force=args.force,
    )

    try:
        worker.run()

    except KeyboardInterrupt:
        print("stopping...")
        worker.stop()
        return 2

    finally:
        logger.debug(f"done in {worker.total_time} seconds")

    return 0


if __name__ == "__main__":
    sys.exit(main())
