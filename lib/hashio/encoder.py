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
Contains checksum encoder classes and functions.
"""

import hashlib
import os
import xxhash
import zlib
from typing import List

from hashio import config
from hashio.exporter import CacheExporter
from hashio.logger import logger
from hashio.utils import read_file, walk


def bytes_to_long(data: bytes):
    """Converts a byte string to a long.

    :param data: the byte string to convert.
    :return: the long value.
    """
    result = 0
    for b in data:
        result = result * 256 + ord(b)
    return result


def long_to_bytes(data: int):
    """Converts a long to a byte string.

    :param data: the long value to convert.
    :return: the byte string.
    """
    encoded = []
    while data > 0:
        data, mod = divmod(data, 256)
        encoded.append(chr(mod))
    return "".join(encoded[::-1])


def checksum_file(path: str, encoder: object):
    """Creates a checksum for a given filepath and encoder.
    Note: resets encoder, existing data will be lost.

    :param path: the path to the filepath being hashed
    :param encoder: instance of Encoder subclass
    :return: hexdigest of the checksum
    """
    encoder.reset()
    for data in read_file(path):
        encoder.update(data)
    value = encoder.hexdigest()
    encoder.reset()
    return value


def checksum_folder(path: str, encoder: object):
    """Creates a checksum for the entire contents of a folder as a single
    checksum.

    Note: resets encoder, existing data will be lost.

    :param path: the path to the folder being hashed
    :param encoder: instance of Encoder subclass
    :return: hexdigest of the checksum
    """
    encoder.reset()
    for filepath in walk(path, filetype="f"):
        for data in read_file(filepath):
            encoder.update(data)
    value = encoder.hexdigest()
    encoder.reset()
    return value


def checksum_path(
    path: str, encoder: object, filetype: str = "a", use_cache: bool = True
):
    """Returns a checksum of for a given path, encoder and filetype.

    Note: resets encoder, existing data will be lost.

    :param path: the path to the folder being hashed
    :param encoder: subclass of Encoder
    :param filetype: one of f (file), d (dir), or a (all)
    :param use_cache: cache results to filesystem
    :return: hexdigest of the checksum
    """
    if use_cache:
        cached_value = CacheExporter.find(path, encoder.name)
        if cached_value:
            return cached_value
    if os.path.isfile(path) and filetype in ("a", "f"):
        return checksum_file(path, encoder)
    elif os.path.isdir(path) and filetype in ("a", "d"):
        return checksum_folder(path, encoder)


def checksum_gen(
    path: str,
    encoders: List[str],
    filetype: str,
    recursive: bool,
    use_cache: bool = True,
):
    """Checksum generator that yields tuple of (algo, value, path).

    :param path: path to a file or folder
    :param encoders: list of encoder names
    :param filetype: one of f (file), d (dir), or a (all)
    :param recursive: recurse subdirs
    :param use_cache: cache results to filesystem
    :yields: tuple of (algo, value, path)
    """
    if recursive:
        for subpath in walk(path, filetype):
            for algo in encoders:
                encoder = ENCODER_MAP.get(algo)()
                value = checksum_path(subpath, encoder, filetype, use_cache)
                if value:
                    yield (algo, value, os.path.relpath(subpath))

    else:
        for algo in encoders:
            encoder = ENCODER_MAP.get(algo)()
            value = checksum_path(path, encoder, filetype, use_cache)
            if value:
                yield (algo, value, path)


class CRC32(object):
    """Simple implementation of CRC32 checksum class."""

    def __init__(self):
        self._initial = 0
        self._crc = self._initial

    def update(self, data: bytes):
        """Update the CRC32 checksum with the given data."""
        self._crc = zlib.crc32(data, self._crc)
        return self  # for chaining

    def digest(self):
        """Return the CRC32 checksum as a 4-byte binary string."""
        return (self._crc & 0xFFFFFFFF).to_bytes(4, byteorder="big")

    def hexdigest(self):
        """Return 8-character lowercase hex string."""
        return format(self._crc & 0xFFFFFFFF, "08x")

    def copy(self):
        """Return a copy of the CRC32 object."""
        new = CRC32()
        new._crc = self._crc
        return new

    def reset(self):
        """Reset the CRC32 checksum to its initial value."""
        self._crc = self._initial
        return self


class Encoder(object):
    """Checksum encoder base class."""

    def __init__(self):
        self.hash = hashlib.sha512()

    @classmethod
    def get(cls, name: str):
        ENCODER_MAP = dict([(sc.name, sc) for sc in Encoder.__subclasses__()])
        encoder_class = ENCODER_MAP.get(name)
        try:
            return encoder_class()
        except TypeError:
            raise Exception(f"Invalid encoder: {name}")

    def hexdigest(self):
        return self.hash.hexdigest()

    def reset(self):
        self.__init__()

    def update(self, data: dict):
        self.hash.update(data)


class MD5Encoder(Encoder):
    """MD5 Encoder class."""

    name = "md5"

    def __init__(self):
        super(MD5Encoder, self).__init__()
        self.hash = hashlib.md5()


class SHA256Encoder(Encoder):
    """SHA256 Encoder class."""

    name = "sha256"

    def __init__(self):
        super(SHA256Encoder, self).__init__()
        self.hash = hashlib.sha256()


class SHA512Encoder(Encoder):
    """SHA512 Encoder class."""

    name = "sha512"

    def __init__(self):
        super(SHA512Encoder, self).__init__()
        self.hash = hashlib.sha512()


class XXH64Encoder(Encoder):
    """XXH64 Encoder class."""

    name = "xxh64"

    def __init__(self):
        super(XXH64Encoder, self).__init__()
        self.hash = xxhash.xxh64()


class XXH3_64Encoder(Encoder):
    """XXH3_64 Encoder class."""

    name = "xxh3_64"

    def __init__(self):
        super(XXH3_64Encoder, self).__init__()
        self.hash = xxhash.xxh3_64()


class XXH3_128Encoder(Encoder):
    """XXH3_128 Encoder class."""

    name = "xxh3_128"

    def __init__(self):
        super(XXH3_128Encoder, self).__init__()
        self.hash = xxhash.xxh3_128()


class CRC32Encoder(Encoder):
    """CRC32 Encoder class."""

    name = "crc32"

    def __init__(self):
        super(CRC32Encoder, self).__init__()
        self.hash = CRC32()


class C4Encoder(SHA512Encoder):
    """C4 Encoder class."""

    base = 58
    charset = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    idlen = 90
    lowbyte = "1"
    name = "c4"
    prefix = "c4"

    def __init__(self):
        super(C4Encoder, self).__init__()

    def hexdigest(self):
        hexstr = ""
        shastr = super(C4Encoder, self).hexdigest()
        value = int(shastr, 16)

        while value != 0:
            modulo = value % self.base
            value = value // self.base
            hexstr = self.charset[modulo] + hexstr

        hexstr = self.prefix + hexstr.ljust(self.idlen - 2, self.lowbyte)
        return hexstr


def all_encoder_classes(cls: Encoder):
    """Returns all encoder classes.

    :param cls: Encoder class
    :return: set of encoder classes
    """
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_encoder_classes(c)]
    )


# map of encoder names to encoder classes
ENCODER_MAP = {}


def build_encoder_map():
    """Returns encoder class matching algo name."""
    for cls in all_encoder_classes(Encoder):
        ENCODER_MAP[cls.name] = cls


build_encoder_map()


def get_encoder_class(name: str):
    """Returns encoder class matching algo name on demand.

    :param name: name of the encoder class
    :return: encoder class
    """
    for cls in all_encoder_classes(Encoder):
        if cls.name == name:
            return cls
    return


def dedupe_paths_gen(paths: List[str], algo: str = config.DEFAULT_ALGO):
    """Generator that takes as input one or more directories or a list of files,
    generates checksums, and yields a list of duplicates.

      Step 1: Walk paths and checksum all files.
      Step 2: Look for matches.

    :param paths: one or more dirs or list of files
    :param algo: name of hashing algorithm
    :yields: list of duplicates
    """

    from hashio.utils import normalize_path

    # map of hash value to path list
    hash_map = {}

    # static kwargs
    kwargs = {
        "encoders": [algo],
        "filetype": "f",
        "recursive": True,
        "use_cache": False,
    }

    # iterate over paths and generate checksums
    for path in paths:
        if not path or not os.path.exists(path):
            logger.warning("missing: %s", path)
            continue
        for _, value, filepath in checksum_gen(path=path, **kwargs):
            filepath = normalize_path(filepath)
            if value in hash_map:
                hash_map[value].append(filepath)
            else:
                hash_map[value] = [filepath]

    # find and yield matches
    for matches in hash_map.values():
        if len(matches) > 1:
            yield matches


def dedupe_cache_gen(target: str, source: str, algo: str = config.DEFAULT_ALGO):
    """Generator that takes as input pre-generated cache files and yields a list
    of duplicates.

      Step 1: Verify checksum values in cache files.
      Step 2: Look for matches.

    All comparisons fall into one of four categories:

      - paths match, hashes match (dupe)
      - paths match, hashes do not match (not dupe)
      - paths do not match, hashes match (dupe)
      - paths do not match, hashes do not match (not dupe)

    Examples:

      - file copied from source dir to target dir
      - file copied to target and updated
      - file copied to target and updaed and renamed
      - file copied to taret and renamed or moved
      - file copied to target and source file updated

    :param target: the target cache file for dedupe
    :param source: the source cache file for comparison
    :param algo: name of hashing algorithm
    :yields: list of duplicates
    """
    from hashio.exporter import JSONExporter

    # map of verified hash value to path list
    hash_map = {}

    # stores target files that have been updated
    target_changes = {}

    # stores source files that have been updated
    source_changes = {}

    # look for updated files in target dir
    for _, v, p in verify_checksums(target):
        logger.debug("target file changed: %s", str(p))
        target_changes[p] = v

    # look for updated files in source dir
    for _, v, p in verify_checksums(source):
        logger.debug("source file changed: %s", str(p))
        source_changes[p] = v

    # directory roots that contain files
    target_root = os.path.dirname(target)
    source_root = os.path.dirname(source)

    # read the target and source cache data
    target_data = JSONExporter.read(target)
    source_data = JSONExporter.read(source)

    # iterate over paths in target cache
    for path, target_metadata in target_data.items():
        source_metadata = source_data.get(path)

        # build file paths to target and source files
        target_filepath = os.path.join(target_root, path)
        source_filepath = os.path.join(source_root, path)

        # path not found, don't do anything
        if not source_metadata:
            logger.debug("source path not found: %s", source_filepath)
            continue

        # make sure we're using the correct target hash value and
        # add to hash map
        target_hash = target_changes.get(target_filepath, target_metadata.get(algo))
        if not target_hash:
            logger.debug("target hash not found: %s", path)
            continue
        if target_hash in hash_map:
            hash_map[target_hash].append(target_filepath)
        else:
            hash_map[target_hash] = [target_filepath]

        # make sure we're using the correct source hash value and add
        # to hash map
        source_hash = source_changes.get(source_filepath, source_metadata.get(algo))
        if not source_hash:
            logger.debug("source hash not found: %s", path)
            continue
        if source_hash in hash_map:
            hash_map[source_hash].append(source_filepath)
        else:
            hash_map[source_hash] = [source_filepath]

        # look for and yield matches
        if target_hash == source_hash:
            logger.debug("dupe: %s -> %s", target_filepath, source_filepath)
            yield [target_filepath, source_filepath]


def dedupe_paths(paths: List[str], algo=config.DEFAULT_ALGO):
    """Returns a list of duplicate file pairs for an input list of directories
    or files.

    Note: Generates new checksums for all paths. It may be faster to pregenerate
    checksum cache files for all paths then use `dedupe_caches`.

    See `dedupe_paths_gen` for more info.

    :param paths: one or more dirs or list of files
    :param algo: name of hashing algorithm
    :returns: a list of found duplicates
    """

    return [r for r in dedupe_paths_gen(paths, algo=algo)]


def dedupe_caches(target: str, source: str, algo: str = config.DEFAULT_ALGO):
    """Finds duplicate pairs in a given set of target and source cache files.

    Note: this may be faster than `dedupe_paths`, but it will miss potential
    matches if all files are not in the target cache file.

    See `dedupe_cache_gen` for more info.

    :param target: the target cache for dedupe
    :param source: the source cache for comparison
    :param algo: name of hashing algorithm
    :yields: list of duplicate files
    """

    # nothing to check
    if not target or not source:
        raise Exception(f"bad inputs: {target} {source}")

    # target and source should be unique
    if target == source:
        raise Exception(f"identical inputs: {target} {source}")

    # target input does not exist
    if not os.path.exists(target):
        raise Exception(f"target missing: {target}")

    # source input does not exist
    if not os.path.exists(source):
        raise Exception(f"source missing: {source}")

    # if inputs are cache files
    return [(t, s) for t, s in dedupe_cache_gen(target, source, algo=algo)]


def verify_checksums(path: str, start: str = None):
    """Generator that yields a data tuple for hash misses in a previously
    generated output file. Compares mtimes in the output file with the
    filesystem.

    :param path: path to hash file
    :yields: tuple of (algo, hash value, filepath)
    """
    from hashio.exporter import get_exporter_class

    if not os.path.isfile(path):
        raise Exception(f"file not found: {path}")

    ext = os.path.splitext(path)[-1]
    exporter_class = get_exporter_class(ext)
    data = exporter_class.read(path)

    if start is None or start == os.getcwd():
        root = os.path.dirname(path)
    else:
        root = start

    for filename, metadata in data.items():
        filepath = os.path.join(root, filename)

        # iterate over all the hash algos...
        for algo in ENCODER_MAP.keys():
            if algo not in metadata.keys():
                continue

            # check if file exists and compare mtimes
            if not os.path.exists(filepath):
                logger.warning("missing: %s", filepath)
                continue
            # if mtimes match, skip
            elif metadata.get("mtime") == os.stat(filepath).st_mtime:
                continue

            # if mtimes don't match, re-hash the file
            logger.debug("mtime miss on %s", filepath)
            old_value = metadata.get(algo)
            encoder = ENCODER_MAP.get(algo)()
            new_value = checksum_path(filepath, encoder)

            # hash values don't match, file must have changed
            if (new_value and old_value) and (new_value != old_value):
                logger.debug("hash miss on %s %s", algo, filepath)
                yield (algo, new_value, filepath)


def verify_caches(source: str, other: str, algo: str = config.DEFAULT_ALGO):
    """
    Compare and verify filenames and hashes in two given files.

    :param source: source file (source.json)
    :param other: file to compare (other.json)
    :yield: (algo, value, path) tuple of mismatches
    """
    from hashio.exporter import get_exporter_class

    ext1 = os.path.splitext(source)[-1]
    ext2 = os.path.splitext(other)[-1]

    data_source = get_exporter_class(ext1).read(source)
    data_other = get_exporter_class(ext2).read(other)

    for path, metadata in data_source.items():
        found = False
        name = metadata.get("name")
        value = metadata.get(algo)

        # paths and hashes match
        if data_other.get(path) and data_other[path].get(algo) == value:
            found = True

        # look for matching file names and hashes
        for _, _metadata in data_other.items():
            if _metadata.get("name") == name and _metadata.get(algo) == value:
                found = True
                break

        if found:
            continue

        yield (algo, value, path)
