"""Check that the benchmark gate detects regressions and rejects missing work."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

SPEC = importlib.util.spec_from_file_location(
    "benchmark", Path(__file__).resolve().parents[1] / "scripts/benchmark.py"
)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


@pytest.mark.parametrize(
    "base, candidate, status", [(1.0, 1.15, 0), (0.1, 0.15, 0), (1.0, 1.3, 1)]
)
def test_regression_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base: float,
    candidate: float,
    status: int,
) -> None:
    """Require both absolute and relative slowdown thresholds to be exceeded.

    :param tmp_path: output report directory
    :param monkeypatch: replaces timing with deterministic samples
    :param base: baseline elapsed seconds
    :param candidate: candidate elapsed seconds
    :param status: expected process exit status
    """
    create_data = benchmark.create_data
    monkeypatch.setattr(
        benchmark, "create_data", lambda root, count, size: create_data(root, 1, 8)
    )
    monkeypatch.setattr(
        benchmark,
        "run_cli",
        lambda root, python, source, expected, work, scenario, procs: (
            base if work.name.startswith("base-") else candidate
        ),
    )
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark",
            "--baseline",
            str(benchmark.ROOT),
            "--iterations",
            "6",
            "--procs",
            "1",
            "--json",
            str(output),
        ],
    )
    assert benchmark.main() == status
    rows = json.loads(output.read_text())["results"]
    assert len(rows) == 6
    assert all(row["regression"] == bool(status) for row in rows)
    assert all(len(row["samples_s"]["base"]) == 6 for row in rows)


def test_incomplete_work_is_not_a_speedup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a successful process that skipped hashing input files.

    :param tmp_path: input/output fixture directory
    :param monkeypatch: substitutes an incomplete successful CLI process
    """
    source = tmp_path / "input"
    expected = benchmark.create_data(source, 2, 8)
    work = tmp_path / "work"
    work.mkdir()
    (work / "manifest.json").write_text("{}")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, stdout="", stderr=""
        ),
    )
    with pytest.raises(RuntimeError, match="Incomplete or incorrect"):
        benchmark.run_cli(
            benchmark.ROOT, sys.executable, source, expected, work, "small", 1
        )


def test_noisy_pairs_are_not_confirmed_regressions() -> None:
    """Flag a median slowdown for review when too few pairs reproduce it."""
    result = benchmark.summarize(
        {"base": [1] * 6, "candidate": [1.4, 1.4, 1.4, 1.4, 0.9, 0.9]}, 20, 0.1
    )
    assert result["status"] == "inconclusive"
    assert not result["regression"]


def test_outlier_does_not_hide_repeatable_regression() -> None:
    """Keep the gate effective when most paired samples are consistently slower."""
    result = benchmark.summarize(
        {"base": [1] * 6, "candidate": [1.4] * 5 + [0.9]}, 20, 0.1
    )
    assert result["status"] == "REGRESSION"
    assert result["regression"]
