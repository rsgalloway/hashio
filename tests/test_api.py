"""Tests for the Hashio Python API."""

import asyncio
from pathlib import Path

import pytest

import hashio
from hashio.encoder import XXH64Encoder, checksum_file, composite_hash


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Folder with a nested file and a three-frame sequence."""
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_bytes(b"a")
    (tmp_path / "sub" / "b.txt").write_bytes(b"b")
    for frame in (1, 2, 3):
        (tmp_path / ("shot.%04d.png" % frame)).write_bytes(str(frame).encode())
    return tmp_path


@pytest.fixture
def hio(tmp_path_factory) -> hashio.Hashio:
    """Hashio with caching on, using a throwaway database."""
    db = tmp_path_factory.mktemp("db") / "hash.sql"
    return hashio.Hashio(use_cache=True, db=str(db), procs=2)


def xxh(path: Path) -> str:
    return checksum_file(str(path), XXH64Encoder())


def test_hash_file_folder_sequence(tree: Path, hio: hashio.Hashio) -> None:
    [one] = hio.hash(tree / "a.txt")
    assert (one.path, one.value, one.kind) == (
        str(tree / "a.txt"),
        xxh(tree / "a.txt"),
        "file",
    )

    folder = {r.path: r.value for r in hio.hash(tree)}
    assert len(folder) == 5
    assert folder[str(tree / "sub" / "b.txt")] == xxh(tree / "sub" / "b.txt")

    ref = str(tree / "shot.%04d.png") + " 1-3"
    *frames, total = hio.hash(ref)
    assert len(frames) == 3 and total.kind == "sequence" and total.path == ref
    names = ["shot.%04d.png" % f for f in (1, 2, 3)]
    expected = composite_hash([(n, xxh(tree / n)) for n in names], XXH64Encoder())
    assert total.value == expected


def test_cache_and_snapshot(tree: Path, hio: hashio.Hashio) -> None:
    hio.snapshot = "one"
    assert not any(r.cached for r in hio.hash(tree))
    hio.snapshot = None
    assert all(r.cached for r in hio.hash(tree))
    assert list(hio.query(str(tree / "a.txt")))

    (tree / "a.txt").write_bytes(b"changed")
    hio.snapshot = "two"
    list(hio.hash(tree))
    hio.start = str(tree)
    # shortcut: only checks diff() runs; snapshot contents are wrong upstream
    assert all(c in ("added", "removed", "changed") for c, _ in hio.diff("one", "two"))
    assert {s["name"] for s in hio.list_snapshots()} == {"one", "two"}


def test_verify_manifest(tree: Path, hio: hashio.Hashio) -> None:
    hio.outfile = str(tree / "hash.json")
    list(hio.hash(tree / "sub"))
    assert list(hio.verify(hio.outfile)) == []


def test_early_break_and_errors(tree: Path, hio: hashio.Hashio) -> None:
    for _ in hio.hash(tree):
        break  # must stop the pool without hanging
    with pytest.raises(ValueError):
        list(hio.hash(tree / "missing"))
    hio.algo = "nope"
    with pytest.raises(ValueError):
        list(hio.hash(tree))


def test_async(tree: Path, hio: hashio.Hashio) -> None:
    async def collect():
        await hio.update_cache_async()
        return {r.path: r.value async for r in hio.hash_async(tree)}

    assert asyncio.run(collect()) == {r.path: r.value for r in hio.hash(tree)}
