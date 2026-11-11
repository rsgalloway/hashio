# Usage

## Basic Hashing

Recursively checksum files in a directory tree and write a manifest:

```bash
hashio <PATH> -o hash.json [--algo ALGO]
```

Supported output extensions are `.json`, `.txt`, and `.mhl`.

## Sequences and Wildcards

Hashio uses pyseq to resolve compressed sequence references. Existing files and
folders take precedence, even if their names contain pattern characters.

```bash
# Discover existing frames with eight-digit padding.
hashio 'frames/shot.%08d.png' -o hashes.json --start frames

# Select an explicit range, or a stepped range.
hashio 'frames/shot.%08d.png 1001-1010' -o hashes.json --start frames
hashio 'frames/shot.%08d.png 1001-1010x3' -o hashes.json --start frames

# General wildcards and multiple inputs also share one pool and manifest.
hashio 'frames/*.png' reference.jpg -o hashes.json
```

Quote the complete reference, including any range. On Windows Command Prompt,
use double quotes. A pattern without a range selects existing frames only;
an explicit range requires every requested frame to exist. Invalid references,
missing inputs, and unmatched patterns exit with status 2 before creating or
truncating the output manifest.

All selected inputs use a single worker pool (`--procs` limits the total number
of workers). Overlapping selections are deduplicated by normalized absolute
path, and the output manifest itself is excluded. Ignorable-file rules still
apply unless `--force` is used.

Output contains per-file checksums; sequence inputs do not change the manifest
format or generate an aggregate sequence checksum. Use `--start` to choose the
root for relative paths, as with ordinary file inputs.

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
