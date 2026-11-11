"""Sequence selection and real CLI hashing through the shared worker pool."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import List

import pytest

from hashio.inputs import resolve_inputs


@pytest.fixture
def frames(tmp_path: Path) -> Path:
    """Create padded frame fixtures with gaps and nonmatching files.

    :param tmp_path: temporary directory for the test
    :returns: directory containing the fixture files
    """
    directory = tmp_path / "frames with spaces"
    directory.mkdir()
    for frame in (1, 2, 3, 5):
        (directory / ("shot.%08d.png" % frame)).write_bytes(str(frame).encode())
    (directory / "shot.0001.png").write_bytes(b"wrong padding")
    (directory / "shot.abcdefgh.png").write_bytes(b"not a frame")
    return directory


@pytest.mark.parametrize(
    "suffix, expected",
    [
        ("", [1, 2, 3, 5]),
        (" 1-3", [1, 2, 3]),
        (" 1-5x2", [1, 3, 5]),
        (" [1-3, 5]", [1, 2, 3, 5]),
    ],
)
def test_sequence_resolution(frames: Path, suffix: str, expected: List[int]) -> None:
    """Check padding, ranges, steps, and frame ordering.

    :param frames: directory containing frame fixtures
    :param suffix: range suffix appended to the sequence pattern
    :param expected: frame numbers expected in the selection
    """
    actual = resolve_inputs([str(frames / "shot.%08d.png") + suffix])
    assert actual == [str(frames / ("shot.%08d.png" % frame)) for frame in expected]


def test_relative_sequence_resolution(
    frames: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolve a relative sequence path containing spaces.

    :param frames: directory containing frame fixtures
    :param monkeypatch: fixture used to change the working directory
    """
    monkeypatch.chdir(frames.parent)
    assert resolve_inputs(["frames with spaces/shot.%08d.png 2-3"]) == [
        str(frames / "shot.00000002.png"),
        str(frames / "shot.00000003.png"),
    ]


def test_literals_take_precedence(tmp_path: Path) -> None:
    """Preserve existing literal paths containing pattern characters.

    :param tmp_path: temporary directory for the test
    """
    path = tmp_path / "literal.%08d.png"
    path.write_bytes(b"literal")
    assert resolve_inputs([path]) == [str(path)]
    assert resolve_inputs([tmp_path]) == [str(tmp_path)]


def test_glob_and_duplicate_inputs(frames: Path) -> None:
    """Expand wildcards and deduplicate explicitly repeated inputs.

    :param frames: directory containing frame fixtures
    """
    file = frames / "shot.00000001.png"
    assert resolve_inputs([str(frames / "shot.0000000?.png"), file]) == [
        str(frames / ("shot.%08d.png" % frame)) for frame in (1, 2, 3, 5)
    ]


@pytest.mark.parametrize(
    "reference",
    [
        "missing.png",
        "absent.*.png",
        "absent.%08d.png",
        "shot.%08d.png 1-5",
    ],
)
def test_invalid_or_missing_inputs_fail(frames: Path, reference: str) -> None:
    """Reject missing inputs, unmatched patterns, and incomplete ranges.

    :param frames: directory containing frame fixtures
    :param reference: invalid or incomplete input relative to the fixture directory
    """
    with pytest.raises(ValueError):
        resolve_inputs([str(frames / reference)])


def run_cli(args: List[str], tmp_path: Path) -> subprocess.CompletedProcess:
    """Run the CLI in an isolated working directory with a timeout.

    :param args: command-line arguments passed to hashio
    :param tmp_path: working directory for the subprocess
    :returns: completed process with captured text output
    """
    env = os.environ.copy()
    env["HASHIO_DB"] = str(tmp_path / "cache.sql")
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "lib")
    return subprocess.run(
        [sys.executable, "-m", "hashio.cli"] + args,
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize("extension", ["json", "txt", "mhl"])
def test_cli_sequence_and_overlapping_input(
    frames: Path, tmp_path: Path, extension: str
) -> None:
    """Verify sequence hashes and deduplication in each output format.

    :param frames: directory containing frame fixtures
    :param tmp_path: temporary directory for the output manifest
    :param extension: manifest format to exercise
    """
    import xml.etree.ElementTree as ET

    output = tmp_path / ("manifest." + extension)
    result = run_cli(
        [
            str(frames / "shot.%08d.png"),
            str(frames / "shot.00000001.png"),
            "-p",
            "2",
            "-a",
            "md5",
            "-r",
            str(frames),
            "--no-cache",
            "-s",
            "-o",
            str(output),
        ],
        tmp_path,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "total files: 4" in result.stdout
    expected = {
        "shot.%08d.png" % frame: hashlib.md5(str(frame).encode()).hexdigest()
        for frame in (1, 2, 3, 5)
    }
    if extension == "json":
        actual = {
            name: row["md5"] for name, row in json.loads(output.read_text()).items()
        }
    elif extension == "txt":
        rows = [line.split(" ", 1) for line in output.read_text().splitlines()]
        assert len(rows) == 4
        actual = {name: digest for digest, name in rows}
    else:
        rows = ET.parse(str(output)).getroot().findall("hash")
        assert len(rows) == 4
        actual = {
            os.path.basename(row.findtext("file")): row.findtext("md5") for row in rows
        }
    assert actual == expected


def test_cli_missing_range_does_not_touch_output(frames: Path, tmp_path: Path) -> None:
    """Ensure invalid ranges fail before truncating an existing manifest.

    :param frames: directory containing frame fixtures
    :param tmp_path: temporary directory for the output manifest
    """
    output = tmp_path / "manifest.txt"
    output.write_text("keep this")
    result = run_cli([str(frames / "shot.%08d.png 1-5"), "-o", str(output)], tmp_path)
    assert result.returncode == 2
    assert "Missing frames" in result.stderr
    assert output.read_text() == "keep this"


def test_cli_overlapping_directory_and_output_excluded(tmp_path: Path) -> None:
    """Exclude duplicate selections and the output manifest from hashing.

    :param tmp_path: temporary directory for inputs and the manifest
    """
    directory = tmp_path / "input"
    directory.mkdir()
    (directory / "one.txt").write_text("one")
    (directory / "two.txt").write_text("two")
    output = directory / "manifest.json"
    result = run_cli(
        [
            str(directory),
            str(directory / "one.txt"),
            "-o",
            str(output),
            "-r",
            str(directory),
            "-p",
            "2",
            "--no-cache",
            "-s",
            "-f",
        ],
        tmp_path,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "total files: 2" in result.stdout
    assert set(json.loads(output.read_text())) == {"one.txt", "two.txt"}


def expected_aggregate(directory: Path, numbers: List[int]) -> str:
    """Compute the documented aggregate independently of CLI result collection.

    :param directory: directory containing the selected frames
    :param numbers: frame numbers to include
    :returns: MD5 digest of sorted filename and content-hash entries
    """
    entries = []
    for number in numbers:
        name = "shot.%08d.png" % number
        digest = hashlib.md5((directory / name).read_bytes()).hexdigest()
        entries.append("{}:{}".format(name, digest))
    return hashlib.md5("\n".join(sorted(entries)).encode()).hexdigest()


def test_cli_sequence_default_aggregate(frames: Path, tmp_path: Path) -> None:
    """Print the aggregate alongside the original compressed sequence reference.

    :param frames: directory containing frame fixtures
    :param tmp_path: subprocess working directory
    """
    reference = str((frames / "shot.%08d.png").relative_to(tmp_path))
    result = run_cli([reference, "-a", "md5", "-p", "2"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "{}  {}\n".format(
        expected_aggregate(frames, [1, 2, 3, 5]), reference
    )


def test_cli_verbose_sequence_and_wildcard(frames: Path, tmp_path: Path) -> None:
    """Distinguish verbose sequence details from wildcard per-file output.

    :param frames: directory containing frame fixtures
    :param tmp_path: subprocess working directory
    """
    options = ["-a", "md5", "-p", "2", "-r", str(frames)]
    verbose = run_cli([str(frames / "shot.%08d.png"), "-v"] + options, tmp_path)
    assert verbose.returncode == 0, verbose.stderr
    lines = verbose.stdout.strip().splitlines()
    assert len(lines) == 5
    assert lines[-1] == "{}  {}".format(
        expected_aggregate(frames, [1, 2, 3, 5]), frames / "shot.%08d.png"
    )
    assert {line.split()[1] for line in lines[:-1]} == {
        "shot.%08d.png" % frame for frame in (1, 2, 3, 5)
    }
    wildcard = run_cli([str(frames / "shot.0000000?.png")] + options, tmp_path)
    assert wildcard.returncode == 0, wildcard.stderr
    assert len(wildcard.stdout.strip().splitlines()) == 4
    assert expected_aggregate(frames, [1, 2, 3, 5]) not in wildcard.stdout


def test_cli_aggregate_portable_and_sensitive(frames: Path, tmp_path: Path) -> None:
    """Preserve hashes across relocation while detecting content and membership changes.

    :param frames: directory containing frame fixtures
    :param tmp_path: directory for relocated copies and subprocesses
    """
    import shutil

    relocated = tmp_path / "relocated"
    shutil.copytree(frames, relocated)
    options = ["-a", "md5", "-p", "2"]
    expected = expected_aggregate(frames, [1, 2, 3, 5])
    result = run_cli([str(relocated / "shot.%08d.png")] + options, tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split("  ", 1)[0] == expected
    (relocated / "shot.00000002.png").write_bytes(b"changed")
    changed = run_cli([str(relocated / "shot.%08d.png")] + options, tmp_path)
    assert changed.returncode == 0, changed.stderr
    assert changed.stdout.split("  ", 1)[0] == expected_aggregate(
        relocated, [1, 2, 3, 5]
    )
    assert changed.stdout.split("  ", 1)[0] != expected
    (relocated / "shot.00000005.png").rename(relocated / "shot.00000006.png")
    renamed = run_cli([str(relocated / "shot.%08d.png")] + options, tmp_path)
    assert renamed.returncode == 0, renamed.stderr
    assert renamed.stdout.split("  ", 1)[0] == expected_aggregate(
        relocated, [1, 2, 3, 6]
    )
    assert renamed.stdout != changed.stdout


def test_cli_multiple_sequence_aggregates(frames: Path, tmp_path: Path) -> None:
    """Keep overlapping sequence ranges as independent aggregate units.

    :param frames: directory containing frame fixtures
    :param tmp_path: subprocess working directory
    """
    references = [
        str(frames / "shot.%08d.png 1-3"),
        str(frames / "shot.%08d.png 3-5x2"),
    ]
    result = run_cli(references + ["-a", "md5", "-p", "2"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "{}  {}".format(expected_aggregate(frames, [1, 2, 3]), references[0]),
        "{}  {}".format(expected_aggregate(frames, [3, 5]), references[1]),
    ]


def test_cli_sequence_no_incomplete_aggregate(frames: Path, tmp_path: Path) -> None:
    """Reject a sequence when selected frames are excluded from hashing.

    :param frames: directory containing frame fixtures
    :param tmp_path: subprocess working directory
    """
    ignored = tmp_path / "dist"
    ignored.mkdir()
    (ignored / "shot.00000001.tmp").write_bytes(b"test")
    result = run_cli([str(ignored / "shot.%08d.tmp"), "-p", "2"], tmp_path)
    assert result.returncode == 2
    assert "Cannot aggregate" in result.stderr
    assert not result.stdout.strip()


def test_cli_sequence_rejects_null(frames: Path, tmp_path: Path) -> None:
    """Reject metadata-only sequence aggregates.

    :param frames: directory containing frame fixtures
    :param tmp_path: subprocess working directory
    """
    result = run_cli([str(frames / "shot.%08d.png"), "-a", "null"], tmp_path)
    assert result.returncode == 2
    assert "require a checksum algorithm" in result.stderr


def test_cli_cached_sequence_aggregate(frames: Path, tmp_path: Path) -> None:
    """Include cache hits in aggregates and verbose frame output.

    :param frames: directory containing frame fixtures
    :param tmp_path: directory for the isolated cache and subprocess
    """
    from hashio.cache import Cache

    cache = Cache(str(tmp_path / "cache.sql"))
    for number in (1, 2, 3, 5):
        path = frames / ("shot.%08d.png" % number)
        stat = path.stat()
        cache.put(
            str(path),
            stat.st_mtime,
            "md5",
            hashlib.md5(path.read_bytes()).hexdigest(),
            stat.st_size,
            stat.st_ino,
        )
    cache.commit()
    cache.close()
    result = run_cli(
        [str(frames / "shot.%08d.png"), "-a", "md5", "-p", "2", "--cache", "-v"],
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("(cached)") == 4
    assert result.stdout.strip().splitlines()[-1] == "{}  {}".format(
        expected_aggregate(frames, [1, 2, 3, 5]), frames / "shot.%08d.png"
    )
