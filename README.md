hashio
======

Custom file and directory checksum and verification tool.

## Features

- multiple hash algos: c4, crc32, md5, sha256, sha512, xxh64
- supports multiple output options: json, txt and mhl
- recursively runs checksums on files in directory trees
- ignores predefined file name patterns
- collects important file stat metadata
- caches results for better performance

## Installation

The easiest way to install:

```bash
$ pip install -U hashio
```

Note: Starting with hashio 0.4.0, a SQLite-backed cache is used by default.
If your Python installation does not include sqlite3, either:

- Rebuild Python with SQLite support (e.g. libsqlite3-dev, sqlite-devel)
- Or install an earlier version:

```bash
pip install 'hashio<0.4.0'
```

## Usage

Recursively checksum and gather metadata all the files in a dir tree, and output
results to a hash.json file:

```bash
$ hashio <PATH> -o hash.json [--algo ALGO]
```

`hashio` supports .json, .txt or .mhl output formats:

```bash
$ hashio <PATH> -o hash.txt
```

If no output file is specified, the hash results are stored in a cache, defined
by `${HASHIO_DB}`.

#### Quick usage with uvx

You can run `hashio` instantly using uvx:

```bash
$ uvx hashio <PATH>
```

This downloads and runs `hashio` in a temporary, isolated environment — no
installs, no virtualenvs, no cleanup needed. Perfect for quick hash verification
tasks on large directories.

#### Ignorable files

Note that files matching patterns defined in `config.IGNORABLE` will be skipped,
unless using the `--force` option:

```bash
$ hashio .git
path is ignorable: .git
$ hashio .git --force
hashing: 894kB [00:02, 758kB/s, files/s=412.80]
```

Verify paths in previously generated hash file by comparing stored mtimes (if
available) or regenerated hash values if mtimes are missing or different:

```bash
$ hashio --verify hash.json
```

#### Portability

To make a portable hash file, use `-or` to make the paths relative to the
`hash.json` file :

```bash
$ hashio <DIR> -or hash.json
```

or use `--start` to make them relative to the `<START>` value

```bash
$ hashio <DIR> -o hash.json --start <START>
```

To verify the data in the hash file, run hashio from the parent dir of the data,
or set `--start` to the parent dir:

```bash
$ hashio --verify hash.json
```

## Environment

The following environment variables are supported, and default settings are in
the config.py module. 

| Variable     | Description |
|--------------|-------------|
| BUF_SIZE     | chunk size in bytes when reading files |
| HASHIO_ALGO  | default hashing algorithm to use |
| HASHIO_DB    | hashio cache db location |
| HASHIO_FILE  | default hash file location |
| HASHIO_IGNORABLE | comma separated list of ignorable file patterns |
| LOG_LEVEL    | logging level to use (DEBUG, INFO, etc) |
| MAX_PROCS    | max number hash processes to spawn |

Optionally, modify the `hashio.env` file if using [envstack](https://github.com/rsgalloway/envstack),
or create a new env file:

```bash
$ cp hashio.env debug.env
$ vi debug.env  # make edits
$ ./debug.env -- hashio
```

## Metadata

By default `hashio` collects the following file metadata:

| Key   | Value |
|-------|-------------|
| name  | file name |
| atime | file access time (st_atime) |
| ctime | file creattion time (st_ctime) |
| mtime | file modify time (st_mtime) |
| ino   | file inode (st_ino) |
| dev   | filesystem device (st_dev) |
| size  | file size in bytes |
| type  | path type - (f)ile or (d)irectory |

To walk a directory and collect metadata without peforming file checksums, use
the "null" hash algo:

```bash
$ hashio <DIR> -a null
```

To make "null" the default, update `${HASHIO_ALGO}` in the environment or the
`hashio.env` file.

```bash
$ export HASHIO_ALGO=null
```

## Cache File and Snapshots

`hashio` maintains a local SQLite cache file (by default at `~/.cache/hashio/hash.sql`)
to store previously computed file hashes, metadata, and snapshot history. This
dramatically speeds up repeated runs and enables powerful diffing capabilities.

#### Snapshots

Snapshots are point-in-time views. You can optionally record a snapshot of the
current file state using:

```bash
$ hashio --snapshot SNAPSHOT_NAME
```

This links all scanned files to a snapshot named SNAPSHOT_NAME, allowing you to:

- Track changes over time
- Compare file states across points in time
- Build file history for audit/debugging
- Generate change reports (diffs)

Each snapshot is stored in the cache and contains only links to file metadata
entries, no file duplication.

#### Diffing Snapshots

You can compare snapshots using:

```bash
$ hashio --diff SNAP1 SNAP2 [--start PATH]
```

This prints a summary of file-level changes between two snapshots:

```
+ file was added
- file was removed
~ file was modified
```

## Read Buffer Size Optimization

By default, `hashio` uses a fixed read buffer size set in `config.py`. This
conservative default works well across most systems, but it can be suboptimal
on filesystems with large block sizes (e.g. network-attached storage).

#### Optional: Enable Dynamic Buffer Sizing

Dynamic read buffer sizing can be enabled to reduce IOPS and improve performance
on sequential reads. When enabled:

- Filesystem block size is determined via `os.statvfs(path).f_frsize`.
- If the block size is large (≥ 128 KiB), it is used directly
- If the block size is small, it is scaled up and clamped

#### Configuration

You can configure buffer size using the `BUF_SIZE` environment variable:

- If `BUF_SIZE` is set to a positive integer, that value is used
- If `BUF_SIZE` is 0 or a negative value, dynamic buffer sizing is enabled
- If `BUF_SIZE` is unset, the default fixed size from `config.py` is used

This can be configured either:
- In your `hashio.env` file (if using [envstack](https://github.com/rsgalloway/envstack)), or
- Directly in the environment

#### Examples

```bash
# use a fixed 512 KiB read buffer
export BUF_SIZE=524288

# enable dynamic sizing based on block size
export BUF_SIZE=0
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
    print(f"{algo} {miss}")
```

Generate a checksum of a folder:

```python
from hashio.encoder import checksum_folder, XXH64Encoder
encoder = XXH64Encoder()
value = checksum_folder(folder, encoder)
```

## Cache

Run the following command to safely apply the latest schema updates and create
any missing indexes:

```bash
$ hashio --update-cache
```
