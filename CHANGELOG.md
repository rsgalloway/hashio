# Changelog

All notable changes to this project will be documented in this file.

## 0.5.1 - 2026-04-19

### Changed

- Disabled hash cache lookups and writes by default during hashing runs to avoid SQLite contention on concurrent workloads.
- Renamed the cache toggle surface to implementation-agnostic names: `HASHIO_USE_CACHE`, `--cache`, and `--no-cache`.
- Switched worker temp cache queue handling away from `multiprocessing.Manager()` to reduce overhead and sandbox friction.

### Fixed

- Updated cache retry tests to opt into cache usage explicitly and keep temp cache paths isolated under test.
- Refreshed documentation to reflect the new cache defaults and toggle names.

## 0.5.0 - 2026-04-04

### Added

- Added `--uncompress` support for hashing the decompressed contents of `.gz` files.
- Added gzip-aware verification fallback so manifest entries can be checked against matching `.gz` files.
- Added basic GitHub Actions test coverage across multiple operating systems and Python versions.
- Added unit coverage for gzip hashing, verification, and shutdown helper behavior.

### Changed

- Manifest output now uses the uncompressed filename when `--uncompress` is enabled.
- `--uncompress` bypasses the hash cache so compressed-byte and decompressed-content hashes are not mixed.
- Improved shutdown handling so `Ctrl+C` exits more cleanly during worker cleanup and progress-thread teardown.

### Fixed

- Fixed SQLite cache merge retries to clean up attached temp databases between attempts.
- Fixed the lock-failure retry test so it still exercises the intended failure path.
