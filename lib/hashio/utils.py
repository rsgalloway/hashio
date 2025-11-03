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
import platform
import re

from hashio import config

# make a regex that matches any patterns in IGNORABLE
ALL_IGNORABLE = re.compile(
    "(" + ")|(".join([fnmatch.translate(i) for i in config.IGNORABLE]) + ")"
)


def format_bytes(n: int):
    """Formats a number of bytes into a human-readable string.

    :param n: number of bytes.
    :returns: formatted string with appropriate unit (B, KB, MB, GB, TB, PB).
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PB"


def get_block_size(path: str = os.getcwd(), default: int = 4096):
    """Returns the block size of the file system where the path is located.

    :param path: file system path, defaults to current working directory.
    :param default: default block size in bytes if not available, defaults to
        4096.
    :returns: block size in bytes, defaults to 4096 if not available.
    """
    try:
        if platform.system() == "Windows":
            return 128 * 1024
        stat = os.statvfs(path)
        return stat.f_frsize or stat.f_bsize
    except AttributeError:
        return default
    except OSError:
        return default


def get_buffer_size(path: str = os.getcwd(), default: int = config.BUF_SIZE):
    """Determine optimal buffer size for minimizing IOPS during sequential reads.
    If the block size is greater than 128 KiB, use it as the buffer size.

    :param path: file system path, defaults to current working directory.
    :param default: default buffer size in bytes, defaults to config.BUF_SIZE.
    :returns: buffer size in bytes, defaults to config.BUF_SIZE if not
        available.
    """
    try:
        if default > 0:
            return int(default)

        block_size = get_block_size(path)

        if block_size >= 128 * 1024:
            buffer_size = block_size
        else:
            buffer_size = block_size * 32

        return min(max(buffer_size, 128 * 1024), 1024 * 1024)

    except Exception:
        return 256 * 1024


def get_metadata(path: str):
    """Returns dict of file metadata for the given path.

    :param path: file system path
    :returns: dict with file metadata
    """
    stats = os.stat(path)
    path_type = "file" if os.path.isfile(path) else "dir"
    return {
        "name": os.path.basename(path),
        "dirname": os.path.dirname(path),
        "atime": stats.st_atime,
        "ctime": stats.st_ctime,
        "mtime": stats.st_mtime,
        "ino": stats.st_ino,
        "dev": stats.st_dev,
        "nlink": stats.st_nlink,
        "size": stats.st_size,
        "type": path_type,
    }


def is_ignorable(path: str):
    """Returns True if path is ignorable. Checks path against patterns
    in the ignorables list.

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
    """Returns a normalized path:

    - If 'path' is relative, returns it normalized.
    - If 'path' is absolute and starts with 'start', returns the relative path
      from 'start'.
    - Otherwise, returns the absolute normalized path.

    Resolves symbolic links to avoid mismatches between real and symlinked
    paths.

    :param path: file system path
    :param start: base path for relative resolution, defaults to current
        directory
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


def read_file(filepath: str, buffer_size: int = config.BUF_SIZE):
    """File reader data generator.

    :param filepath: file to read
    :param buffer_size: size of each read chunk in bytes ($BUF_SIZE)
    :returns: file data in chunks
    """
    with open(filepath, "rb") as f:
        while True:
            data = f.read(buffer_size)
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
