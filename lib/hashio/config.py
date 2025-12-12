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
Contains default hashio configs and settings.
"""

import envstack
import logging
import os
import platform

from envstack.util import safe_eval

envstack.init("hashio")


# default output filename
HOME = os.getenv("HOME", os.path.expanduser("~"))
PLATFORM = platform.system().lower()
CACHE_ROOT = {
    "darwin": f"{HOME}/Library/Caches/hashio",
    "linux": f"{HOME}/.cache/hashio",
    "windows": os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.join(HOME, "AppData", "Local")), "hashio"
    ),
}.get(PLATFORM)

# stores temporary worker cache files
TEMP_CACHE_DIR = os.path.join(CACHE_ROOT, "temp")

# default cache filename
CACHE_FILENAME = os.getenv("HASHIO_FILE", "hash.json")

# default central database filename
DEFAULT_DB_PATH = os.getenv("HASHIO_DB", os.path.join(CACHE_ROOT, "hash.sql"))

# the default hashing algorithm to use
DEFAULT_ALGO = os.getenv("HASHIO_ALGO", "xxh64")

# ignorable file patterns
IGNORABLE = [
    CACHE_FILENAME,
    "*~",
    "~*",
    "*.aspera-ckpt",
    "*.bak",
    "*.pyc",
    "*.pyo",
    "*.partial",
    "*.swp",
    "*.tmp",
    ".git*",
    ".pytest_cache*",
    ".smbdelete*",
    ".svn*",
    "@eaDir*",
    "dist",
    "build",
    ".venv*",
    "venv*",
    "#recycle*",
    "#snapshot*",
    ".jetpart*",
    "*.egg-info",
    "__pycache__",
    ".cache",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
    ".Trash*",
    "lost_found",
]

# allow overrides to the default ignorable patterns
IGNORABLE = os.getenv("HASHIO_IGNORABLE", ",".join(IGNORABLE)).split(",")

# default logging level
LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)

# set the read buffer size for file reads. change this value to optimize performance
# or limit memory usage. default is 1MB.
BUF_SIZE = safe_eval(os.getenv("BUF_SIZE", 1024 * 1024))

# maximum number of search and hash processes to spawn
MAX_PROCS = safe_eval(os.getenv("MAX_PROCS", 1))

# default merge interval in seconds
MERGE_INTERVAL = safe_eval(os.getenv("MERGE_INTERVAL", 5))
