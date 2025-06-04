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
Contains file export classes and functions.
"""

import json
import os
import time
from datetime import datetime

from hashio import config
from hashio.logger import logger
from hashio.utils import normalize_path


class FileExistsError(Exception):
    """Custom exception for an existing file"""


class BaseExporter:
    """Exporter base class."""

    def __init__(self, filepath: str):
        dirname = os.path.dirname(filepath)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
        self.filepath = filepath

    def close(self):
        raise NotImplementedError

    @classmethod
    def read(cls, filepath: str):
        raise NotImplementedError

    def write(self):
        raise NotImplementedError


class JSONExporter(BaseExporter):
    """JSON streaming exporter. Opens a .json output file pointer to which data
    can be written."""

    ext = ".json"

    def __init__(self, filepath: str):
        super(JSONExporter, self).__init__(filepath)
        fp = open(self.filepath, "w")
        fp.write("{\n")
        fp.close()

    def close(self):
        """Closes file pointer to output file, and writes final closing }. Do not
        call until writing data is completed.
        """
        if config.PLATFORM == "windows":
            offset = -3  # \n\r
        else:
            offset = -2  # \n
        with open(self.filepath, "rb+") as f:
            f.seek(offset, os.SEEK_END)
            f.truncate()
        fp = open(self.filepath, "a+")
        fp.write("\n}")
        fp.close()

    @classmethod
    def read(cls, filepath: str):
        """Reads and returns the json content at a given filepath, or {} if there
        is an error.
        """
        try:
            with open(filepath) as fp:
                data = json.load(fp)
            return data

        except json.decoder.JSONDecodeError as err:
            print(err)
            return {}

    def write(self, path: str, data: dict):
        """
        Writes `data` to file indexed by `path`. The contents of the hash file
        should be data dicts indexed by unique paths.

            {
                path1: {data1},
                ...
                pathN: {dataN}
            }

        Calling write with the same path will overwrite the data in the output
        file.

        The closing } will be written out when close() is called.

        :param path: path-like string
        :param data: the data to write
        """

        with open(self.filepath, "a+") as f:
            try:
                f.write('    "{0}": {1},\n'.format(path, json.dumps(data, indent=8)))
            except Exception as err:
                logger.warning("write error: %s", err)


class CacheExporter(JSONExporter):
    """Cache data exporter. Hash caches are files that contain serialized hash
    and filesystem metadata. All paths in a cache file are relative to the cache
    file itself."""

    ext = ".json"

    def __init__(self, filepath: str):
        super(CacheExporter, self).__init__(filepath)

    @classmethod
    def get_cache(cls, path: str):
        """Returns the cache filename for a given path.

        The cache file will be written to the directory containing the path. For
        example if the path is `/a/b/c/d` then the cache will be written to
        `/a/b/c/hash.json` and the path reference in the cache will be `d`.

        :param path: path being hashed (file or dir)
        """
        dirname = os.path.dirname(os.path.abspath(path))
        return os.path.join(dirname, config.CACHE_FILENAME)

    @classmethod
    def find(cls, path: str, key: str):
        """Searches for path in cached data, and compares mtimes for a given path.

        :param path: filesystem path
        :param key: key name of cached value to return
        :returns: cached value or None
        """
        cache_filename = cls.get_cache(path)
        if os.path.exists(cache_filename):
            try:
                cache_data = cls.read(cache_filename)
                pathkey = normalize_path(path, start=os.path.dirname(cache_filename))
                data = cache_data.get(pathkey)
                if data and data.get("mtime") == os.stat(path).st_mtime:
                    return data.get(key, "invalid cache")
            except Exception as err:
                print(err)
                return None
        return None


class MHLExporter(BaseExporter):
    """MHL 1.1 streaming exporter.

    Note: it seems MHL may only verify md5 hash values, and should not be used
    on directories. The equivalent of the command:

      $ mhl seal <file>

    would be:

      $ hashio <file> --algo md5 -o out.mhl
    """

    ext = ".mhl"

    # maps raw data keys to mhl keys
    keymap = {
        "atime": "lastaccesseddate",
        "ctime": "creationdate",
        "mtime": "lastmodificationdate",
    }

    # MHL timestamp format
    time_format = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, filepath: str):
        super(MHLExporter, self).__init__(filepath)
        fp = open(self.filepath, "w")
        fp.write(
            """<?xml version="1.0" encoding="UTF-8"?>
<hashlist version="1.1">\n"""
        )
        fp.close()

    def close(self):
        fp = open(self.filepath, "a+")
        fp.write("</hashlist>\n")
        fp.close()

    def timestamp(self, ts: int = None):
        """Converts timestamp to MHL supported time format."""
        if ts is None:
            ts = int(time.time())
        return datetime.utcfromtimestamp(ts).strftime(self.time_format)

    @classmethod
    def read(cls, filepath: str):
        """Reads MHL data from file and returns a list of dicts.

        :param filepath: file path to read
        :returns: list of dicts with MHL data
        """
        from lxml import etree

        try:
            with open(filepath, "r") as f:
                tree = etree.parse(f)
                hashes = tree.xpath("/hashlist/hash")
                return {
                    h.findtext("file"): {
                        "hashdate": h.findtext("hashdate"),
                        "lastmodificationdate": h.findtext("lastmodificationdate"),
                        "size": int(h.findtext("size", default="0")),
                        "file": h.findtext("file"),
                        "md5": h.findtext("md5", default=""),
                    }
                    for h in hashes
                }
        except Exception as err:
            print(err)
            return []

    def write(self, path: str, data: dict):
        """Writes out data as MHL-specified XML data.

        Example minimum hash element:

            <hash>
              <lastmodificationdate>...</lastmodificationdate>
              <size>...</size>
              <md5>...</md5>
              <hashdate>...</hashdate>
              <file>...</file>
            </hash>

        :param path: file path that is source of data
        :param data: the data to write
        """

        from lxml import etree

        # normalize the path relative to the output file path
        path = normalize_path(path, start=os.path.dirname(self.filepath))

        # root hash element
        root = etree.Element("hash")

        # file and hash date elements are required
        elem = etree.Element("file")
        elem.text = path
        root.insert(0, elem)

        elem = etree.Element("hashdate")
        elem.text = self.timestamp()
        root.insert(0, elem)

        # write data as xml elements
        for k, v in data.items():
            k = self.keymap.get(k, k)
            elem = etree.Element(k)
            if "date" in k:
                elem.text = self.timestamp(v)
            else:
                elem.text = str(v)
            root.insert(0, elem)

        # write json serialized data to output file
        with open(self.filepath, "a+") as f:
            f.write((etree.tostring(root, pretty_print=True).decode("utf-8")))


class TXTExporter(BaseExporter):
    """TXT flatfile exporter. Writes a newline-separated list of 'hash path'
    values. Does not export file metadata."""

    ext = ".txt"

    def __init__(self, filepath: str, algo: str = None):
        super(TXTExporter, self).__init__(filepath)
        self.fp = open(self.filepath, "w")
        self.algo = algo

    def close(self):
        """Closes the output file."""
        self.fp.close()

    @classmethod
    def read(cls, filepath: str):
        """Reads TXT data from `filepath` and returns a dict with filepath as key
        and metadata dict containing the hash.

        Note: Currently only supports a single hash value per file, using the
        default algorithm.

        :param filepath: file path to read
        :returns: dict with filepath as key and metadata dict as value
        """
        result = {}
        try:
            with open(filepath, "r") as f:
                for line in f:
                    parts = line.strip().split(" ", 1)
                    if len(parts) == 2:
                        checksum, path = parts
                        result[path] = {
                            config.DEFAULT_ALGO: checksum,
                        }
        except Exception as err:
            print(err)
        return result

    def write(self, path: str, data: dict):
        """Writes 'hash path' values to the output file. Uses the default
        algo key to get the hash value $HASHIO_ALGO.

        :param path: path-like string
        :param data: the data to write, expected to contain the algo key
        """
        # if no algo is specified, try to find one in the data
        if not self.algo:
            from hashio.encoder import ENCODER_MAP

            for algo in ENCODER_MAP:
                if data.get(algo):
                    self.algo = algo
                    break

        value = data.get(self.algo, "")
        self.fp.write(f"{value} {path}\n")


def all_exporter_classes(cls: BaseExporter):
    """Returns all exporter classes."""
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_exporter_classes(c)]
    )


# maps of all exporter classes to their extensions
EXPORTER_MAP = {}


def build_exporter_map():
    """Returns exporter class matching ext.

    Note: exporter ext values must be unique.
    """
    global EXPORTER_MAP
    for cls in all_exporter_classes(BaseExporter):
        EXPORTER_MAP[cls.ext] = cls


# build the exporter map on import
build_exporter_map()


def get_exporter_class(ext: str):
    """Returns exporter class matching ext."""
    for cls in all_exporter_classes(BaseExporter):
        if cls.ext == ext:
            return cls
    return BaseExporter
