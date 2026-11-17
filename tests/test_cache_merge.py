#!/usr/bin/env python

import os
import subprocess
import sys


def run_hashio(cwd, env, *args):
    cmd = [sys.executable, "-m", "hashio.cli", *args]
    result = subprocess.run(
        cmd, cwd=cwd, env=env, capture_output=True, text=True, check=True
    )
    return result.stdout


def test_snapshot_diff_with_parallel_workers(tmp_path):
    root = tmp_path / "snap"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("a\n")
    (root / "sub" / "b.txt").write_text("b\n")
    for i in range(1, 4):
        (root / f"shot.000{i}.png").write_text(f"{i}\n")

    env = dict(os.environ)
    env["HASHIO_DB"] = str(tmp_path / "snap.sql")
    lib = os.path.join(os.path.dirname(__file__), "..", "lib")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [lib, env.get("PYTHONPATH")]))

    run_hashio(root, env, "--snapshot", "one", "-p", "2", ".")
    (root / "a.txt").write_text("changed\n")
    run_hashio(root, env, "--snapshot", "two", "-f", "-p", "2", ".")

    out = run_hashio(root, env, "--diff", "one", "two")
    assert out.splitlines() == [f"~ {root / 'a.txt'}"]

    # fully cached run must still produce a complete snapshot
    run_hashio(root, env, "--snapshot", "three", "-p", "2", ".")
    assert run_hashio(root, env, "--diff", "two", "three").strip() == ""


MIXED_RUN = """
import os, sqlite3, sys
from hashio import config
from hashio.worker import HashWorker
root, n = sys.argv[1], int(sys.argv[2])
for i in range(n):
    open(os.path.join(root, f"f{i}_0"), "w").write(str(i))
HashWorker(root, procs=4, use_cache=True).run()
for i in range(n):
    open(os.path.join(root, f"f{i}_1"), "w").write(f"n{i}")
# merge_interval=0: every cached hit triggers a periodic merge mid-run
HashWorker(root, procs=4, use_cache=True, merge_interval=0).run()
rows = sqlite3.connect(config.DEFAULT_DB_PATH).execute(
    "select count(*) from files").fetchone()[0]
print(rows, len(os.listdir(config.TEMP_CACHE_DIR)))
"""


def test_periodic_merge_keeps_other_workers_results(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    env = dict(os.environ)
    env["HOME"] = str(tmp_path)  # isolates TEMP_CACHE_DIR
    env["HASHIO_DB"] = str(tmp_path / "db.sql")
    lib = os.path.join(os.path.dirname(__file__), "..", "lib")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [lib, env.get("PYTHONPATH")]))

    n = 500
    out = subprocess.run(
        [sys.executable, "-c", MIXED_RUN, str(root), str(n)],
        env=env, capture_output=True, text=True, check=True,
    ).stdout.split()
    assert out == [str(2 * n), "0"]  # every hash cached, no orphaned temp dbs
