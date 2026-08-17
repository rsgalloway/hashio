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
