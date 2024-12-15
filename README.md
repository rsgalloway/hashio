hashio
======

Custom file and directory checksum tool.

## Features

- multiple hash algos: c4, md5, sha256, sha512, xxh64
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
results to a JSON file:

```bash
$ hashio <DIR>
```

Note that files matching patterns defined in `config.IGNORABLE` will be skipped.

Verify paths in previously generated JSON file by comparing stored mtimes (if
available) or regenerated hash values if mtimes are missing or different:

```bash
$ hashio --verify hash.json
```


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
