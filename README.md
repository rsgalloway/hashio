hashio
======

Custom file and directory checksum tool.

## Features

- multiple hash algos: c4, md5, sha256, sha512, xxh64
- multiple export options (Default .json)
- recursively runs checksums on files in directory trees
- ignores predefined file name patterns
- optionally caches results for better performance


## Installation

```bash
$ pip install -U hashio
```

## Usage

Basic usage:

```bash
hashio [-h] [--hash HASH [HASH ...]] [-t FILETYPE] [-o FILEPATH] [-a]
       [-m] [-r] [--cache] [-v] [--version]
       PATH [PATH ...]

positional arguments:
PATH                  one or more files or directories

optional arguments:
-h, --help            show this help message and exit
--hash HASH [HASH ...]
                        checksum algorithm (c4|md5|sha256|sha512|xxh64)
-t FILETYPE, --filetype FILETYPE
                        PATH is of type file (f) or dir (d) or all (a)
-o FILEPATH, --outfile FILEPATH
                        write results to output FILEPATH
-a, --abs             use absolute paths
-m, --metadata        include file metadata in output
-r, --recursive       recurse directory structure
--cache               cache checksum values while walking filesystem
-v, --verify          verify checksum values in PATH
--version             show program's version number and exit
```

## Examples

Checksum one or more files or directories using one or more hash
algorithms (default is xxh64):

```bash
$ hashio <PATH1>..<PATHN> [--hash <ALGO1>..<ALGON>]
```

Recursively checksum and gather metadata all the files in a dir
tree, and output results to a JSON file:

```bash
$ hashio <DIR> -rm -o out.json
```

Note that files matching patterns defined in `$IGNORABLE` will be skipped.

Verify paths in previously generated JSON file by comparing stored mtimes (if available) or regenerated hash values if mtimes are missing or different:

```bash
$ hashio --verify out.json
```
