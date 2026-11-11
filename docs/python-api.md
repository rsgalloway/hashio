# Python API

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
unique absolute paths. `HashWorker.paths` selects inputs for a shared pool;
`path` supplies the scan label and snapshot root. Put process-starting code under
`if __name__ == "__main__":` when calling it from a Python script.
