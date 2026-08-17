# Usage

## Basic Hashing

Recursively checksum files in a directory tree and write a manifest:

```bash
hashio <PATH> -o hash.json [--algo ALGO]
```

Supported output extensions are `.json`, `.txt`, and `.mhl`.

## Ignorable Files

Files matching patterns in `config.IGNORABLE` are skipped unless `--force` is
used:

```bash
hashio .git
hashio .git --force
```

## Portable Paths

To store manifest paths relative to the output file location:

```bash
hashio <DIR> -or hash.json
```

Or make them relative to an explicit start directory:

```bash
hashio <DIR> -o hash.json --start <START>
```

To verify a portable manifest, run `hashio` from the appropriate parent
directory or pass the same `--start` value:

```bash
hashio --verify hash.json
```

## Hashing Decompressed `.gz` Content

Use `--uncompress` to hash the decompressed contents of `.gz` files instead of
the archive bytes:

```bash
hashio sample.txt.gz -o hash.json --uncompress
hashio --verify hash.json --uncompress
```

In this mode, manifest entries use the uncompressed filename. `--uncompress`
currently supports `.gz` files only and bypasses the cache.

## Metadata-Only Walks

To walk a directory and collect metadata without computing checksums, use the
`null` algorithm:

```bash
hashio <DIR> -a null
```
