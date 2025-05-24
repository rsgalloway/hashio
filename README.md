hashio
======

Custom file and directory checksum and verification tool.

## Features

- multiple hash algos: c4, crc32, md5, sha256, sha512, xxh64
- recursively runs checksums on files in directory trees
- ignores predefined file name patterns
- caches results for better performance

## Installation

The easiest way to install:

```bash
$ pip install -U hashio
```

## Usage

Checksum one or more files or directories using one or more hash algorithms
(default is xxh64):

```bash
$ hashio <PATH> [--algo <ALGO>]
```

Recursively checksum and gather metadata all the files in a dir tree, and output
results to a hash.json file:

```bash
$ hashio <DIR>
```

Note that files matching patterns defined in `config.IGNORABLE` will be skipped.

Verify paths in previously generated JSON file by comparing stored mtimes (if
available) or regenerated hash values if mtimes are missing or different:

```bash
$ hashio --verify [HASHFILE]
```

## Environments

You modifiy settings in the `hashio.env`
[envstack](https://github.com/rsgalloway/envstack) file, or create a new
environment stack:

```bash
$ cp hashio.env debug.env
$ vi debug.env  # make edits
$ ./debug.env -- hashio
```

Default config settings are in the config.py module. The following environment
variables are supported:

| Variable      | Description |
|---------------|-------------|
| $BUF_SIZE     | chunk size in bytes when reading files |
| $HASHIO_ALGO  | default hashing algorithm to use |
| $HASHIO_FILE  | default hash file location |
| $LOG_LEVEL    | logging level to use (DEBUG, INFO, etc) |
| $MAX_PROCS    | max number hash processes to spawn |

## Python API

Generate a `hash.json` file for a given path (Default is the current working
directory):

```python
from hashio.worker import HashWorker
worker = HashWorker(path)
worker.run()
```

Verify pre-generated checksums stored in a `hash.json` file:

```python
from hashio.encoder import verify_checksums
for algo, value, miss in verify_checksums("hash.json"):
    print("{0} {1}".format(algo, miss))
```
