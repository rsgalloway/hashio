#!/usr/bin/env python3
"""Compare CLI throughput against a base checkout using shared synthetic inputs."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]


def create_data(root: Path, count: int, size: int) -> Dict[str, str]:
    """Create deterministic files and independent MD5 expectations.

    :param root: input directory
    :param count: number of files
    :param size: bytes per file
    :returns: filename to checksum mapping
    """
    root.mkdir()
    expected = {}
    for index in range(count):
        payload = (hashlib.sha256(str(index).encode()).digest() * ((size + 31) // 32))[
            :size
        ]
        name = "frame.%08d.bin" % index
        (root / name).write_bytes(payload)
        expected[name] = hashlib.md5(payload).hexdigest()
    return expected


def run_cli(
    root: Path,
    python: str,
    source: Path,
    expected: Dict[str, str],
    work: Path,
    scenario: str,
    procs: int,
) -> float:
    """Time a fresh CLI process and verify its manifest outside the timed region.

    :param root: checkout whose library will be imported
    :param python: interpreter with that checkout's dependencies
    :param source: shared input fixture directory
    :param expected: expected file checksums
    :param work: isolated output/cache directory for this implementation
    :param scenario: workload name
    :param procs: worker process count
    :returns: elapsed seconds including process startup and shutdown
    """
    work.mkdir(exist_ok=True)
    database = work / "cache.sql"
    if scenario == "cache-cold":
        for path in work.glob("cache.sql*"):
            path.unlink()
    output = work / "manifest.json"
    target = source / "frame.%08d.bin" if scenario == "sequence" else source
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("HASHIO_") and key not in ("PYTHONPATH", "PYTHONHOME")
    }
    env["PYTHONPATH"] = str(root / "lib")
    env["HASHIO_DB"] = str(database)
    warm_cache = scenario == "cache-warm" and database.exists()
    # Top-level setup also runs in spawn/forkserver children.
    driver = work / "driver.py"
    driver.write_text(
        "from hashio import config\n"
        "config.TEMP_CACHE_DIR = " + repr(str(work / "temp-cache")) + "\n"
        "if __name__ == '__main__':\n"
        "    from hashio.cli import main\n"
        "    raise SystemExit(main())\n"
    )
    args = [
        python,
        str(driver),
        str(target),
        "-p",
        str(procs),
        "-a",
        "md5",
        "-r",
        str(source),
        "-o",
        str(output),
        "--cache" if scenario.startswith("cache-") else "--no-cache",
    ]
    if scenario == "verbose" or scenario.startswith("cache-"):
        args.append("-vv" if scenario.startswith("cache-") else "-v")
    start = time.perf_counter()
    result = subprocess.run(
        args, env=env, cwd=str(work), capture_output=True, text=True, timeout=120
    )
    elapsed = time.perf_counter() - start
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    records = json.loads(output.read_text())
    actual = {name: row["md5"] for name, row in records.items()}
    if actual != expected:
        raise RuntimeError("Incomplete or incorrect benchmark manifest: " + scenario)
    if scenario == "verbose" and len(result.stdout.splitlines()) != len(expected):
        raise RuntimeError("Incomplete verbose output")
    if scenario.startswith("cache-"):
        with sqlite3.connect(str(database)) as connection:
            rows = connection.execute("SELECT path, hash FROM files").fetchall()
        if {Path(name).name: digest for name, digest in rows} != expected:
            raise RuntimeError("Incomplete benchmark cache")
        hits = result.stdout.count("(cached)")
        if hits != (len(expected) if warm_cache else 0):
            raise RuntimeError("Unexpected benchmark cache hit count")
    if scenario == "sequence":
        aggregate = hashlib.md5(
            "\n".join(
                "{}:{}".format(name, digest)
                for name, digest in sorted(expected.items())
            ).encode()
        ).hexdigest()
        if result.stdout.split("  ", 1)[0] != aggregate:
            raise RuntimeError("Incorrect sequence aggregate")
    return elapsed


def summarize(
    samples: Dict[str, List[float]], percent_limit: float, seconds_limit: float
) -> dict:
    """Classify paired timings, requiring repeatable slowdowns for the gate.

    :param samples: chronological paired base and candidate timings
    :param percent_limit: minimum relative slowdown percentage
    :param seconds_limit: minimum absolute slowdown in seconds
    :returns: medians, dispersion, paired deltas, and classification
    """
    base = statistics.median(samples["base"])
    candidate = statistics.median(samples["candidate"])
    deltas = [new - old for old, new in zip(samples["base"], samples["candidate"])]
    percentages = [
        (new / old - 1) * 100 for old, new in zip(samples["base"], samples["candidate"])
    ]
    percent = statistics.median(percentages)
    seconds = statistics.median(deltas)
    exceeded = seconds > seconds_limit and percent > percent_limit
    slow_pairs = sum(
        d > seconds_limit and p > percent_limit for d, p in zip(deltas, percentages)
    )
    # This is a repeatability heuristic, not a statistical confidence interval.
    regression = (
        exceeded and len(deltas) >= 4 and slow_pairs >= math.ceil(len(deltas) * 0.75)
    )
    dispersion = {
        label: statistics.median(abs(t - statistics.median(values)) for t in values)
        / statistics.median(values)
        * 100
        for label, values in samples.items()
    }
    noisy = max(dispersion.values()) > 10
    status = (
        "REGRESSION"
        if regression
        else ("inconclusive" if exceeded or noisy or len(deltas) < 4 else "pass")
    )
    return dict(
        base_s=base,
        candidate_s=candidate,
        delta_pct=percent,
        delta_s=seconds,
        mad_pct=dispersion,
        slow_pairs=slow_pairs,
        regression=regression,
        status=status,
    )


def main() -> int:
    """Run paired benchmarks and return nonzero for regressions or invalid results.

    :returns: command exit status
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--baseline-python", default=sys.executable)
    parser.add_argument("--candidate", type=Path, default=ROOT)
    parser.add_argument("--candidate-python", default=sys.executable)
    parser.add_argument("--iterations", type=int, default=6)
    parser.add_argument("--procs", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--full", action="store_true")
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=["small", "large", "verbose", "sequence", "cache-cold", "cache-warm"],
        choices=["small", "large", "verbose", "sequence", "cache-cold", "cache-warm"],
    )
    parser.add_argument("--json", type=Path, default=Path("benchmark.json"))
    parser.add_argument("--regression-pct", type=float, default=20)
    parser.add_argument("--regression-seconds", type=float, default=0.1)
    args = parser.parse_args()
    if args.iterations < 4 or args.iterations % 2 or any(n < 1 for n in args.procs):
        parser.error(
            "iterations must be even and at least four; process counts must be positive"
        )
    if args.regression_pct < 0 or args.regression_seconds < 0:
        parser.error("regression thresholds must be nonnegative")
    roots = {"base": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    pythons = {
        "base": str(Path(args.baseline_python).absolute()),
        "candidate": str(Path(args.candidate_python).absolute()),
    }
    for root in roots.values():
        if not (root / "lib/hashio/cli.py").is_file():
            parser.error("not a hashio checkout: " + str(root))
    report = {
        "schema_version": 2,
        "platform": platform.platform(),
        "python": {},
        "commits": {},
        "iterations": args.iterations,
        "full": args.full,
        "threshold_pct": args.regression_pct,
        "threshold_seconds": args.regression_seconds,
        "results": [],
    }
    for label, root in roots.items():
        report["commits"][label] = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip()
        report["python"][label] = subprocess.check_output(
            [pythons[label], "--version"], text=True
        ).strip()
    failed = False
    with tempfile.TemporaryDirectory(prefix="hashio-benchmark-") as directory:
        temp = Path(directory)
        small, large = temp / "small", temp / "large"
        small_expected = create_data(small, 4096 if args.full else 256, 4096)
        large_expected = create_data(
            large, 8 if args.full else 4, (64 if args.full else 8) * 1024 * 1024
        )
        cache_source = temp / "cache-input"
        cache_expected = create_data(cache_source, 4096 if args.full else 1024, 4096)
        for procs in args.procs:
            for scenario in args.scenarios:
                source = (
                    large
                    if scenario == "large"
                    else (cache_source if scenario.startswith("cache-") else small)
                )
                expected = (
                    large_expected
                    if scenario == "large"
                    else (
                        cache_expected
                        if scenario.startswith("cache-")
                        else small_expected
                    )
                )
                work = {
                    label: temp / (label + "-" + scenario + "-" + str(procs))
                    for label in roots
                }
                samples: Dict[str, List[float]] = {label: [] for label in roots}
                # Warm filesystem/import caches; this also primes the warm SQLite case.
                for label in roots:
                    run_cli(
                        roots[label],
                        pythons[label],
                        source,
                        expected,
                        work[label],
                        scenario,
                        procs,
                    )
                for iteration in range(args.iterations):
                    order = (
                        ("base", "candidate")
                        if iteration % 2 == 0
                        else ("candidate", "base")
                    )
                    for label in order:
                        samples[label].append(
                            run_cli(
                                roots[label],
                                pythons[label],
                                source,
                                expected,
                                work[label],
                                scenario,
                                procs,
                            )
                        )
                summary = summarize(
                    samples, args.regression_pct, args.regression_seconds
                )
                failed |= summary["regression"]
                report["results"].append(
                    dict(
                        scenario=scenario,
                        procs=procs,
                        files=len(expected),
                        bytes=sum(p.stat().st_size for p in source.iterdir()),
                        samples_s=samples,
                        **summary,
                    )
                )
    args.json.write_text(json.dumps(report, indent=2) + "\n")
    print(
        "| Workload | Workers | Base (s) | Candidate (s) | Paired change | MAD base/new | Slow pairs | Status |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---|")
    for row in report["results"]:
        print(
            "| {scenario} | {procs} | {base_s:.3f} | {candidate_s:.3f} | {delta_pct:+.1f}% | {base_mad:.1f}% / {candidate_mad:.1f}% | {slow_pairs}/{iterations} | {status} |".format(
                **row,
                base_mad=row["mad_pct"]["base"],
                candidate_mad=row["mad_pct"]["candidate"],
                iterations=args.iterations,
            )
        )
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
