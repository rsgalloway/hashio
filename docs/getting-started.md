# Getting Started

## Installation

Install from PyPI:

```bash
pip install -U hashio
```

You can also run `hashio` directly with `uvx`:

```bash
uvx hashio <PATH>
```

This downloads and runs `hashio` in an isolated temporary environment.

## SQLite Note

Starting with `hashio 0.4.0`, caching is backed by SQLite. If your Python
build does not include `sqlite3`, either rebuild Python with SQLite support or
install an earlier release:

```bash
pip install 'hashio<0.4.0'
```

## First Run

Generate a JSON manifest for a directory tree:

```bash
hashio <PATH> -o hash.json
```

Write text output instead:

```bash
hashio <PATH> -o hash.txt
```

If no output file is provided, `hashio` still computes hashes but does not
write a manifest.

## Verifying a Manifest

Verify paths in an existing manifest by comparing stored mtimes when available,
or by regenerating hashes when needed:

```bash
hashio --verify hash.json
```
