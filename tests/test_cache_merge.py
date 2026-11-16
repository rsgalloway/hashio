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

