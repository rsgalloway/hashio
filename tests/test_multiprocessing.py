"""Exercise real worker processes, including Python 3.14's forkserver default."""

import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest


WORKER_SCRIPT = """
import multiprocessing
import os
import sys
import threading
import time

from hashio import config
from hashio.worker import HashWorker

# This also runs when spawn/forkserver imports the test driver in a child.
config.TEMP_CACHE_DIR = os.path.join(os.path.dirname(__file__), 'temp-cache')

if __name__ == '__main__':
    method, source, output, cache, startup = sys.argv[1:]
    if method != 'default':
        multiprocessing.set_start_method(method)
    worker = HashWorker(path=source, outfile=output, start=source,
                        algo='md5', procs=2, use_cache=cache == 'on')
    if startup == 'delayed-thread':
        time.sleep(1)
        thread = threading.Thread(target=worker.run)
        thread.start()
        thread.join(15)
        assert not thread.is_alive()
    else:
        worker.run()
    assert worker.is_done()
    assert worker.progress_count() == 12, worker.progress_count()
    assert not multiprocessing.active_children()
"""


@pytest.mark.parametrize(
    "method",
    ["default"]
    + [
        method
        for method in ("spawn", "forkserver")
        if method in multiprocessing.get_all_start_methods()
    ],
)
@pytest.mark.parametrize("startup", ["immediate", "delayed-thread"])
@pytest.mark.parametrize("cache", ["off", "on"])
@pytest.mark.parametrize("extension", ["txt", "json", "mhl"])
def test_worker_processes_hash_and_export(
    tmp_path: Path, method: str, cache: str, extension: str, startup: str
) -> None:
    """Verify checksums and cleanup with immediate and delayed threaded startup.

    :param tmp_path: isolated input, output, and cache directory
    :param method: multiprocessing start method
    :param cache: whether to enable cache writes
    :param extension: output manifest format
    :param startup: immediate call or delayed call from a thread
    """
    source = tmp_path / "input"
    source.mkdir()
    expected = {}
    for index in range(12):
        name = "file-{}.txt".format(index)
        payload = ("payload {}\n".format(index) * 100).encode()
        (source / name).write_bytes(payload)
        expected[name] = hashlib.md5(payload).hexdigest()

    output = tmp_path / ("manifest." + extension)
    database = tmp_path / "cache.sql"
    env = os.environ.copy()
    env["HASHIO_DB"] = str(database)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "lib")
    driver = tmp_path / "run_worker.py"
    driver.write_text(WORKER_SCRIPT)
    result = subprocess.run(
        [sys.executable, str(driver), method, str(source), str(output), cache, startup],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    if extension == "txt":
        records = [line.split(" ", 1) for line in output.read_text().splitlines()]
        assert len(records) == len(expected)
        actual = {name: digest for digest, name in records}
    elif extension == "json":
        records = json.loads(output.read_text())
        actual = {name: metadata["md5"] for name, metadata in records.items()}
    else:
        records = ET.parse(str(output)).getroot().findall("hash")
        assert len(records) == len(expected)
        actual = {
            os.path.basename(row.findtext("file")): row.findtext("md5")
            for row in records
        }
    assert actual == expected

    if cache == "on":
        with sqlite3.connect(str(database)) as connection:
            rows = connection.execute("SELECT path, hash FROM files").fetchall()
        assert len(rows) == len(expected)
        assert {os.path.basename(path): digest for path, digest in rows} == expected
    else:
        assert not database.exists()


@pytest.mark.parametrize(
    "method",
    ["default"]
    + [
        m
        for m in ("spawn", "forkserver")
        if m in multiprocessing.get_all_start_methods()
    ],
)
@pytest.mark.parametrize("action", ["empty", "stop"])
def test_worker_startup_cleanup(tmp_path: Path, method: str, action: str) -> None:
    """Ensure waiting workers can stop and empty scans finish without hanging.

    :param tmp_path: isolated directory for the driver and empty input
    :param method: multiprocessing start method
    :param action: run an empty scan or stop before calling run
    """
    source = tmp_path / "input"
    source.mkdir()
    driver = tmp_path / "startup_cleanup.py"
    driver.write_text(
        """
import multiprocessing
import sys
import time
from hashio.worker import HashWorker

if __name__ == '__main__':
    method, source, action = sys.argv[1:]
    if method != 'default':
        multiprocessing.set_start_method(method)
    worker = HashWorker(source, procs=2, use_cache=False)
    time.sleep(1)
    if action == 'stop':
        worker.stop()
    else:
        worker.run()
    assert worker.is_done()
    assert worker.progress_count() == 0
    assert not multiprocessing.active_children()
"""
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "lib")
    env["HASHIO_DB"] = str(tmp_path / "cache.sql")
    result = subprocess.run(
        [sys.executable, str(driver), method, str(source), action],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
