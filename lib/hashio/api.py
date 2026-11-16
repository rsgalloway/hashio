"""Python API for hashio. Every CLI command is a method on :class:`Hashio`;
the CLI builds a Hashio from its arguments and prints what the methods yield.

>>> h = Hashio(use_cache=True)
>>> for result in h.hash("shots/", "frames/shot.%04d.exr 1001-1100"):
...     print(result.value, result.display)
>>> async for result in h.hash_async("shots/"):
...     print(result.value, result.path)

Settings default to the environment (HASHIO_ALGO, HASHIO_USE_CACHE, HASHIO_DB,
MAX_PROCS, ...) read by :mod:`hashio.config`. Call ``hash`` under
``if __name__ == "__main__":`` in scripts: it starts worker processes.
"""

import asyncio
import functools
import multiprocessing
import os
import queue
import threading
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple

from hashio import config
from hashio.cache import Cache
from hashio.encoder import (
    composite_hash,
    get_encoder_class,
    verify_caches,
    verify_checksums,
)
from hashio.inputs import resolve_selection
from hashio.worker import HashWorker, Result


def _async(method):
    """Run a blocking method in a thread: ``await h.merge_async(...)``."""

    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        call = functools.partial(method, self, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(None, call)

    return wrapper


def _async_gen(method):
    """Stream a generator method from a thread: ``async for x in h.hash_async()``."""

    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        loop = asyncio.get_running_loop()
        it = method(self, *args, **kwargs)
        done = object()
        try:
            while True:
                item = await loop.run_in_executor(None, next, it, done)
                if item is done:
                    return
                yield item
        finally:
            await loop.run_in_executor(None, it.close)

    return wrapper


def common_root(paths) -> str:
    """Return the folder shared by ``paths``, used as the scan label and root."""
    if len(paths) == 1:
        return paths[0]
    try:
        return os.path.commonpath(
            [p if os.path.isdir(p) else os.path.dirname(p) for p in paths]
        )
    except ValueError:  # inputs on different Windows drives
        return os.getcwd()


@dataclass
class Hashio:
    """Hashio settings plus one method per CLI command.

    Field names match the CLI argument names, so the CLI passes its parsed
    arguments straight in. Set them in the constructor or as attributes.
    """

    algo: str = config.DEFAULT_ALGO
    procs: int = config.MAX_PROCS
    use_cache: bool = config.DEFAULT_USE_CACHE
    db: str = config.DEFAULT_DB_PATH
    outfile: Optional[str] = None
    start: Optional[str] = None
    snapshot: Optional[str] = None
    force: bool = False
    uncompress: bool = False

    def hash(self, *paths) -> Iterator[Result]:
        """Hash files, folders, wildcards, or pyseq sequences (``hashio PATH``).

        Yields one ``Result`` per file as it finishes, then one result with
        ``kind="sequence"`` per sequence holding its aggregate checksum.
        Writes ``outfile``, the cache and ``snapshot`` when those are set.

        :raises ValueError: on missing inputs, an unknown algorithm, or a
            sequence whose frames could not all be hashed
        """
        if not get_encoder_class(self.algo):
            raise ValueError(f"Unsupported algorithm: {self.algo}")
        selection = resolve_selection(paths or [os.getcwd()])
        if selection.sequences and self.algo == "null":
            raise ValueError(
                "Sequence aggregates require a checksum algorithm, not null"
            )
        frames = {f for group in selection.sequences.values() for f in group}
        frame_hashes = {}

        results = multiprocessing.Queue()
        worker = HashWorker(
            path=common_root(selection.paths),
            paths=selection.paths,
            result_queue=results,
            console_roots=selection.console_roots,
            outfile=self.outfile,
            procs=self.procs,
            start=self.start,
            algo=self.algo,
            snapshot=self.snapshot,
            force=self.force,
            uncompress=self.uncompress,
            use_cache=self.use_cache or bool(self.snapshot),
            db=self.db,
        )
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            # is_alive() first: once the pool has exited, every result is queued
            while thread.is_alive() or not results.empty():
                try:
                    result = results.get(timeout=0.1)
                except queue.Empty:
                    continue
                if result.path in frames:
                    frame_hashes[result.path] = result
                yield result
        finally:
            if thread.is_alive():  # consumer stopped early or was interrupted
                # graceful cancel: worker.stop() kills processes that may hold
                # the shared lock; keep draining so none block on exit
                worker.done.set()
                while thread.is_alive():
                    try:
                        results.get(timeout=0.1)
                    except queue.Empty:
                        pass
            thread.join()

        aggregates = []
        for reference, group in selection.sequences.items():
            missing = [f for f in group if f not in frame_hashes]
            if missing:
                raise ValueError(
                    "Cannot aggregate {}: {} frame(s) were not hashed (missing, "
                    "ignored, unreadable, or selected as output)".format(
                        reference, len(missing)
                    )
                )
            entries = [(os.path.basename(f), frame_hashes[f].value) for f in group]
            value = composite_hash(entries, get_encoder_class(self.algo)())
            size = sum(frame_hashes[f].size for f in group)
            aggregates.append(
                Result(reference, value, self.algo, size, False, reference, "sequence")
            )
        yield from aggregates

    def verify(self, manifest: str, other: Optional[str] = None) -> Iterator[Tuple]:
        """Check a manifest against disk, or two manifests (``--verify``).

        :yields: ``(algo, value, path)`` for each mismatch
        """
        if other is None:
            yield from verify_checksums(
                manifest, start=self.start, uncompress=self.uncompress
            )
        else:
            yield from verify_caches(manifest, other)

    def query(self, path: str, since: Optional[str] = None) -> Iterator[Tuple]:
        """Yield cache rows for ``path`` (``--query``, ``--since``)."""
        cache = Cache(self.db)
        try:
            yield from cache.query(path, self.algo, since)
        finally:
            cache.close()

    def list_snapshots(self) -> Iterator[dict]:
        """Yield snapshot records from the cache (``--list-snapshots``)."""
        cache = Cache(self.db)
        try:
            yield from cache.list_snapshots()
        finally:
            cache.close()

    def diff(self, snapshot1: str, snapshot2: str) -> Iterator[Tuple[str, str]]:
        """Yield ``(change, path)`` between two snapshots under ``start`` (``--diff``).

        ``change`` is "added", "removed" or "changed".
        """
        root = os.path.abspath(self.start or os.getcwd())
        cache = Cache(self.db)
        try:
            changes = cache.diff_snapshots(snapshot1, snapshot2, force=self.force)
        finally:
            cache.close()
        for change in ("added", "removed", "changed"):
            for path in changes[change]:
                try:
                    if os.path.commonpath([os.path.abspath(path), root]) == root:
                        yield change, path
                except ValueError:  # different Windows drives
                    continue

    def merge(self, other_db: str) -> None:
        """Merge another cache database into ``db`` (``--merge``)."""
        if not os.path.exists(other_db):
            raise FileNotFoundError(f"Cache file not found: {other_db}")
        cache = Cache(self.db)
        try:
            cache.merge(other_db)
        finally:
            cache.close()

    def update_cache(self) -> None:
        """Apply cache schema updates and indexes (``--update-cache``)."""
        Cache(self.db).close()  # Cache() runs the schema update on open

    hash_async = _async_gen(hash)
    verify_async = _async_gen(verify)
    query_async = _async_gen(query)
    list_snapshots_async = _async_gen(list_snapshots)
    diff_async = _async_gen(diff)
    merge_async = _async(merge)
    update_cache_async = _async(update_cache)
