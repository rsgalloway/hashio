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
Contains helper classes and functions.
"""

import fnmatch
import os
import re

from hashio import config

# make a regex that matches any patterns in IGNORABLE
ALL_IGNORABLE = re.compile(
    "(" + ")|(".join([fnmatch.translate(i) for i in config.IGNORABLE]) + ")"
)


def get_metadata(path: str):
    """Returns dict of file metadata: atime, ctime, mtime, ino, dev, size,

    Note: disk usage for directories not accurate.
    """
    stats = os.stat(path)
    path_type = "file" if os.path.isfile(path) else "dir"
    return {
        "name": os.path.basename(path),
        "atime": stats.st_atime,
        "ctime": stats.st_ctime,
        "mtime": stats.st_mtime,
        "ino": stats.st_ino,
        "dev": stats.st_dev,
        "size": stats.st_size,
        "type": path_type,
    }


def is_ignorable(path: str):
    """Returns True if path is ignorable. Checks path against patterns
    in the ignorables list, as well as dot files.

    :param path: file system path
    :returns: True if filename matches pattern in ignorables list
    """
    return re.search(ALL_IGNORABLE, path) is not None


def is_subpath(filepath: str, directory: str):
    """Returns True if the common prefix of both is equal to `directory`, e.g.
    if filepath is /a/b/c/d.rst and directory is /a/b the common prefix is /a/b.

    :param filepath: file system path
    :param directory: file system path
    :returns: True if filepath is a subpath of directory
    """
    d = os.path.join(os.path.realpath(directory), "")
    f = os.path.realpath(filepath)
    return os.path.commonprefix([f, d]) == d


def normalize_path(path: str, start: str = None):
    """
    Returns a normalized path:

    - If 'path' is relative, returns it normalized.
    - If 'path' is absolute and starts with 'start', returns the relative path from 'start'.
    - Otherwise, returns the absolute normalized path.

    Resolves symbolic links to avoid mismatches between real and symlinked paths.

    :param path: file system path
    :param start: base path for relative resolution, defaults to current directory
    :returns: normalized path using forward slashes
    """
    if start is None:
        start = os.getcwd()

    path = os.path.normpath(path)

    if not os.path.isabs(path):
        return path.replace("\\", "/")

    # resolve symlinks to compare apples to apples
    real_path = os.path.realpath(path)
    real_start = os.path.realpath(start)

    if real_path.startswith(real_start + os.sep):
        rel = os.path.relpath(real_path, real_start)
        return rel.replace("\\", "/")

    return real_path.replace("\\", "/")


def paths_are_equal(a: str, b: str):
    """Returns True if path a is the same as path b.

    :param a: file system path
    :param b: file system path
    :returns: True if paths are equal
    """
    return normalize_path(a) == normalize_path(b)


def read_file(filepath: str):
    """File reader data generator.

    :param filepath: file to read
    :returns: file data in chunks
    """
    with open(filepath, "rb") as f:
        while True:
            data = f.read(config.BUF_SIZE)
            if not data:
                break
            yield data


def walk(path: str, filetype: str = "f", force: bool = False):
    """Generator that yields file and dir paths that are not excluded by the
    ignorable list in config.

    :param path: the path to the folder being hashed
    :param filetype: file (f), dir (d) or all (a)
    :param force: return all files and directories
    """
    if (force or not is_ignorable(path)) and os.path.isfile(path):
        yield path

    path = os.path.abspath(path)

    for dirname, dirs, files in os.walk(path, topdown=True):
        if not force and is_ignorable(dirname):
            continue
        for d in dirs:
            if not force and is_ignorable(d):
                dirs.remove(d)
        if filetype in ("a", "f"):
            for name in files:
                if force or not is_ignorable(name):
                    yield os.path.join(dirname, name)
        if filetype in ("a", "d"):
            yield dirname
