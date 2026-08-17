# hashio

`hashio` is a checksum and verification tool for files and directory trees.

It can recursively hash content, collect filesystem metadata, verify manifests,
emit multiple output formats, and optionally persist results in a local SQLite
cache for snapshots and diffs.

## Highlights

- Multiple hash algorithms: `c4`, `crc32`, `md5`, `sha256`, `sha512`, `xxh64`
- Output formats: `json`, `txt`, and `mhl`
- Recursive hashing for file and directory trees
- Configurable ignorable file patterns
- Optional metadata-only scans with the `null` algorithm
- Optional SQLite-backed cache with snapshot diffing
- Optional `.gz` content hashing with `--uncompress`

## Documentation

- [Getting Started](getting-started.md)
- [Usage](usage.md)
- [Configuration](configuration.md)
- [Cache and Snapshots](cache-and-snapshots.md)
- [Python API](python-api.md)

## Quick Example

```bash
hashio <PATH> -o hash.json
hashio --verify hash.json
```

To make manifest paths portable relative to the output file:

```bash
hashio <DIR> -or hash.json
```
