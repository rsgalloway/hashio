# Cache and Snapshots

`hashio` can maintain a local SQLite cache file, by default at:

```text
~/.cache/hashio/hash.sql
```

The cache stores previously computed file hashes, metadata, and snapshot
history so repeated runs can be faster and file states can be compared over
time.

## Enabling the Cache

Caching is disabled by default for hashing runs. Enable it with:

```bash
hashio --cache <PATH>
```

Or through the environment:

```bash
export HASHIO_USE_CACHE=1
```

Use `--no-cache` to force caching off for a specific run.

## Updating the Cache Schema

Apply schema updates and create any missing indexes with:

```bash
hashio --update-cache
```

## Snapshots

Record a point-in-time snapshot during a scan:

```bash
hashio --snapshot SNAPSHOT_NAME
```

Snapshots make it possible to:

- Track changes over time
- Compare file states across scan points
- Build file history for audit and debugging
- Generate change reports

Each snapshot stores links to file metadata entries in the cache rather than
duplicating file contents.

## Diffing Snapshots

Compare two snapshots with:

```bash
hashio --diff SNAP1 SNAP2 [--start PATH]
```

The diff summary uses:

```text
+ file was added
- file was removed
~ file was modified
```
