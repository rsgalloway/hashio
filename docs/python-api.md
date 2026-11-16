# Python API

## Hashio class

Every CLI command is a method on `Hashio`. Settings are attributes, with
defaults read from the environment (`HASHIO_ALGO`, `HASHIO_USE_CACHE`,
`HASHIO_DB`, `MAX_PROCS`, ...). The CLI builds a `Hashio` from its arguments
and prints what the methods yield, so both always offer the same features.

```python
from hashio import Hashio

h = Hashio(use_cache=True, procs=8)   # or set later: h.algo = "sha256"

for result in h.hash("shots/", "frames/shot.%04d.exr 1001-1100"):
    print(result.value, result.display, result.cached)
# one Result per file as it finishes, then one per sequence with
# kind="sequence" holding the aggregate checksum

for algo, value, path in h.verify("hash.json"): ...    # --verify
for row in h.query("/shots/*"): ...                    # -q
for change, path in h.diff("snap1", "snap2"): ...      # --diff
for snap in h.list_snapshots(): ...                    # --list-snapshots
h.merge("other.sql")                                   # --merge
h.update_cache()                                       # --update-cache
```

Each method has an async twin: `hash_async`, `verify_async`, `query_async`,
`diff_async`, `list_snapshots_async` (async generators) and `merge_async`,
`update_cache_async` (coroutines):

```python
async for result in h.hash_async("shots/"):
    print(result.value, result.path)
```

| Setting | CLI flag | Default |
|---|---|---|
| `algo` | `-a` | `$HASHIO_ALGO` (xxh64) |
| `procs` | `-p` | `$MAX_PROCS` (1) |
| `use_cache` | `--cache` / `--no-cache` | `$HASHIO_USE_CACHE` (off) |
| `db` | `--db` | `$HASHIO_DB` |
| `outfile` | `-o` | none |
| `start` | `-r` | current directory |
| `snapshot` | `--snapshot` | none |
| `force` | `-f` | False |
| `uncompress` | `--uncompress` | False |

Breaking out of a `hash()` loop cancels the scan cleanly. Put code that calls
`hash()` under `if __name__ == "__main__":` in scripts.

## Generate a Manifest

```python
from hashio.worker import HashWorker

worker = HashWorker(path, outfile="hash.json")
worker.run()
```

## Verify a Manifest

```python
from hashio.encoder import verify_checksums

for algo, value, miss in verify_checksums("hash.json"):
    print(f"{algo} {miss}")
```

## Hash a Folder

```python
from hashio.encoder import XXH64Encoder, checksum_folder

encoder = XXH64Encoder()
value = checksum_folder(folder, encoder)
```

## Hash a Sequence or Selection

```python
from hashio.inputs import resolve_inputs
from hashio.worker import HashWorker

paths = resolve_inputs(["frames/shot.%08d.png 1001-1010"])
worker = HashWorker(
    path="frames", paths=paths, outfile="hashes.json", start="frames", procs=4
)
worker.run()
```

`resolve_inputs` accepts literal paths, pyseq references, and wildcards, returning
unique absolute paths for per-file hashing. Use `resolve_selection` from the same
module to retain sequence groups in its `sequences` mapping, as the CLI does
when computing aggregate checksums. `HashWorker.paths` selects inputs for a shared pool;
`path` supplies the scan label and snapshot root. Put process-starting code under
`if __name__ == "__main__":` when calling it from a Python script.
