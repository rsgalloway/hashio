# hashio

`hashio` is a checksum and verification tool for files and directory trees.

It can recursively hash content, collect filesystem metadata, verify manifests,
emit multiple output formats, and optionally persist results in a local SQLite
cache for snapshots and diffs.

## Highlights

- Multiple hash algorithms: `c4`, `crc32`, `md5`, `sha256`, `sha512`, `xxh64`
- Output formats: `json`, `txt`, and `mhl`
- Recursive hashing and manifest verification
- Configurable ignorable file patterns
- Metadata-only scans with the `null` algorithm
- Optional SQLite-backed cache and snapshot diffing

## Installation

```bash
pip install -U hashio
```

If your Python build does not include `sqlite3`, either rebuild Python with
SQLite support or install a pre-cache release:

```bash
pip install 'hashio<0.4.0'
```

## Quick Start

```bash
hashio <PATH> -o hash.json
hashio --verify hash.json
```

You can also run it directly with `uvx`:

```bash
uvx hashio <PATH>
```

## Documentation

Detailed documentation now lives under [docs/](docs/):

- [Overview](docs/index.md)
- [Getting Started](docs/getting-started.md)
- [Usage](docs/usage.md)
- [Configuration](docs/configuration.md)
- [Cache and Snapshots](docs/cache-and-snapshots.md)
- [Python API](docs/python-api.md)
