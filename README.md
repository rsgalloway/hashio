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

#### Ignorable files

Note that files matching patterns defined in `config.IGNORABLE` will be skipped,
unless using the `--force` option:

```bash
$ hashio .git
path is ignorable: .git
$ hashio .git --force
hashing files: 442file [00:00, 1101.47file/s]
```

Verify paths in previously generated hash file by comparing stored mtimes (if
available) or regenerated hash values if mtimes are missing or different:

```bash
$ hashio --verify hash.json
```

#### Portability

To make a portable hash file, use `--start` to make the paths relative:

```bash
$ hashio <DIR> --start <START> -o hash.json
```

To verify the data in the hash file, run hashio from the parent dir of the data,
or set `--start` to the parent dir:

```bash
$ hashio --verify hash.json
```

## Environment

The following environment variables are supported, and default settings are in
the config.py module. 

| Variable      | Description |
|---------------|-------------|
| $BUF_SIZE     | chunk size in bytes when reading files |
| $HASHIO_ALGO  | default hashing algorithm to use |
| $HASHIO_FILE  | default hash file location |
| $LOG_LEVEL    | logging level to use (DEBUG, INFO, etc) |
| $MAX_PROCS    | max number hash processes to spawn |

Optionally, modify the `hashio.env` file if using [envstack](https://github.com/rsgalloway/envstack),
or create a new env file:

```bash
$ cp hashio.env debug.env
$ vi debug.env  # make edits
$ ./debug.env -- hashio
```

## Python API

Generate a `hash.json` file for a given path (Default is the current working
directory):

```python
from hashio.worker import HashWorker
worker = HashWorker(path, outfile="hash.json")
worker.run()
```

Verify pre-generated checksums stored in a `hash.json` file:

```python
from hashio.encoder import verify_checksums
for algo, value, miss in verify_checksums("hash.json"):
    print("{0} {1}".format(algo, miss))
```
