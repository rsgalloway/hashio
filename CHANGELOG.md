# Changelog

All notable changes to this project will be documented in this file.

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
