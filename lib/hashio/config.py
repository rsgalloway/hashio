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
Contains configs and settings.
"""

import logging
import os
import platform

# default output filename
CACHE_FILENAME = "hash.json"

# the default hashing algorithm
DEFAULT_ALGO = os.getenv("HASHIO_ALGO", "xxh64")

# ignorable file patterns
IGNORABLE = [
    CACHE_FILENAME,
    "*~",
    "*.pyc",
    "*.pyo",
    "*.bak",
    "*.swp",
    "*.tmp",
    ".git",
    ".svn",
    "dist",
    "build",
    "venv*",
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

# logging level
LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)

# work in 64KB chunks to limit mem usage when reading large files
BUF_SIZE = int(os.getenv("BUF_SIZE", 65536))

# maximum number of search and hash processes to spawn
MAX_PROCS = int(os.getenv("MAX_PROCS", 10))

# cache the name of the platform (linux or windows)
PLATFORM = platform.system().lower()
