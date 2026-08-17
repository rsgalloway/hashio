# Configuration

## Environment Variables

Default settings live in `lib/hashio/config.py`.

| Variable | Description |
|----------|-------------|
| `BUF_SIZE` | Chunk size in bytes when reading files |
| `HASHIO_ALGO` | Default hashing algorithm |
| `HASHIO_DB` | Cache database location |
| `HASHIO_FILE` | Default hash file location |
| `HASHIO_USE_CACHE` | Enable cache lookups and writes |
| `HASHIO_IGNORABLE` | Comma-separated ignorable file patterns |
| `LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, etc.) |
| `MAX_PROCS` | Maximum number of hash worker processes |

If you use [envstack](https://github.com/rsgalloway/envstack), you can start
from the provided env file:

```bash
cp hashio.env debug.env
./debug.env -- hashio
```

## Metadata Collected

By default `hashio` records the following metadata per path:

| Key | Value |
|-----|-------|
| `name` | File name |
| `atime` | Access time (`st_atime`) |
| `ctime` | Creation time (`st_ctime`) |
| `mtime` | Modification time (`st_mtime`) |
| `ino` | Inode (`st_ino`) |
| `dev` | Filesystem device (`st_dev`) |
| `size` | File size in bytes |
| `type` | Path type: file or directory |

## Buffer Size Tuning

By default `hashio` uses a fixed read buffer size from `config.py`.

Set `BUF_SIZE` to a positive integer to use that exact size:

```bash
export BUF_SIZE=524288
```

Set `BUF_SIZE` to `0` or a negative value to enable dynamic sizing based on
filesystem block size:

```bash
export BUF_SIZE=0
```

Dynamic sizing is useful on filesystems with large block sizes where larger
sequential reads can reduce IOPS.
