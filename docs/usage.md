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
# Discover existing frames and print one aggregate checksum.
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

An explicit sequence is one logical asset: by default it prints one aggregate
checksum. A wildcard or literal file input prints per-file checksums. Use `-v`
to show per-frame hashes (including cache hits) before the sequence aggregate:

```bash
hashio 'frames/shot.%08d.png'
hashio 'frames/shot.%08d.png' -v
hashio 'frames/shot.*.png'
```

Each sequence prints `checksum  original-reference`, preserving the compressed
pattern and any range exactly as supplied. Multiple sequence references produce
one aggregate line per reference, even when their frame ranges overlap.

The aggregate uses the selected algorithm on sorted `filename:checksum` entries,
joined by newlines with no trailing newline. It includes each frame's filename
and content checksum, but excludes its parent directory and timestamps. Moving
a sequence preserves its aggregate; changing, renaming, adding, or removing a
frame changes it. Pattern padding, input order, and worker completion order do
not otherwise enter the aggregate. Cache use follows `--cache`/`--no-cache`.

If any selected frame was not hashed, no aggregate is printed and the command
exits with status 2. This includes ignored frames; use `--force` to include them.
The `null` metadata-only algorithm cannot produce sequence aggregates.

With `-o`, the manifest retains per-file checksums for frame-level verification;
the aggregate is still printed to stdout. Use `--start` to choose the root for
manifest paths, as with ordinary file inputs.

## Ignorable Files

Files matching patterns in `config.IGNORABLE` are skipped unless `--force` is
used:

```bash
hashio .git
hashio .git --force
```

## Console Paths

Checksum lines follow the input's path style: relative inputs produce relative
paths (including `../`), and absolute inputs produce absolute paths. This applies
to literal files, wildcard matches, verbose sequence frames, and files found
inside directories. Overlapping selections use the first input's path style.
Sequence aggregate labels preserve the original compressed reference.

`--start` controls manifest paths independently; it does not rewrite console
paths. Path separators in per-file console output use forward slashes.

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
