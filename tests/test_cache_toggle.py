#!/usr/bin/env python

import importlib
import multiprocessing
import os
import queue
import sys


def reload_hashio_modules():
    """Reload hashio config-dependent modules after env changes."""
    for name in list(sys.modules):
        if name in {"hashio.config", "hashio.cli", "hashio.worker"}:
            sys.modules.pop(name)

    importlib.import_module("hashio")
    config = importlib.import_module("hashio.config")
    cli = importlib.import_module("hashio.cli")
    importlib.import_module("hashio.encoder")
    return config, cli


def test_config_cache_defaults_off(monkeypatch):
    monkeypatch.delenv("HASHIO_USE_CACHE", raising=False)

    config, _ = reload_hashio_modules()

    assert config.DEFAULT_USE_CACHE is False


def test_config_cache_reads_env(monkeypatch):
    monkeypatch.setenv("HASHIO_USE_CACHE", "1")

    config, _ = reload_hashio_modules()

    assert config.DEFAULT_USE_CACHE is True


def test_parse_args_honors_env_and_cli_override(monkeypatch, tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("hello")
    monkeypatch.setenv("HASHIO_USE_CACHE", "1")

    _, cli = reload_hashio_modules()

    assert cli.parse_args([str(path)]).use_cache is True
    assert cli.parse_args(["--no-cache", str(path)]).use_cache is False
    assert cli.parse_args(["--cache", str(path)]).use_cache is True


def test_main_disables_cache_by_default(monkeypatch, tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("hello")
    monkeypatch.delenv("HASHIO_USE_CACHE", raising=False)

    _, cli = reload_hashio_modules()
    captured = []

    class DummyWorker:
        def __init__(self, **kwargs):
            captured.append(kwargs["use_cache"])
            self.path = kwargs["path"]
            self.total_time = 0

        def run(self):
            return None

        def progress_count(self):
            return 0

        def progress_bytes(self):
            return 0

    monkeypatch.setattr(cli, "HashWorker", DummyWorker)
    monkeypatch.setattr(cli, "get_encoder_class", lambda algo: object())

    assert cli.main([str(path)]) == 0
    assert cli.main(["--cache", str(path)]) == 0

    assert captured == [False, True]


def test_main_enables_cache_for_snapshot(monkeypatch, tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("hello")
    monkeypatch.delenv("HASHIO_USE_CACHE", raising=False)

    _, cli = reload_hashio_modules()
    captured = []

    class DummyWorker:
        def __init__(self, **kwargs):
            captured.append(kwargs["use_cache"])
            self.path = kwargs["path"]
            self.total_time = 0

        def run(self):
            return None

        def progress_count(self):
            return 0

        def progress_bytes(self):
            return 0

    monkeypatch.setattr(cli, "HashWorker", DummyWorker)
    monkeypatch.setattr(cli, "get_encoder_class", lambda algo: object())

    assert cli.main(["--snapshot", "snap1", str(path)]) == 0

    assert captured == [True]


def test_merge_drains_temp_db_queue(monkeypatch, tmp_path):
    reload_hashio_modules()

    from hashio.worker import HashWorker
    import hashio.worker as worker_module

    merged = []

    class FakeCache:
        def __init__(self, *args, **kwargs):
            pass

        def merge(self, path):
            merged.append(path)

    worker = HashWorker.__new__(HashWorker)
    worker.use_cache = True
    worker.temp_cache = None
    worker.lock = multiprocessing.Lock()
    worker.temp_db_queue = queue.Queue()

    db_paths = [tmp_path / "a.sql", tmp_path / "b.sql"]
    for path in db_paths:
        path.write_text("temp")
        worker.temp_db_queue.put(os.fspath(path))

    monkeypatch.setattr(worker_module, "Cache", FakeCache)

    worker.merge()

    assert merged == [os.fspath(path) for path in db_paths]
    assert all(not path.exists() for path in db_paths)


def test_cache_requires_sqlite(monkeypatch):
    reload_hashio_modules()

    import hashio.cache as cache_module

    monkeypatch.setattr(cache_module, "sqlite3", None)

    try:
        cache_module.Cache()
        assert False, "expected sqlite guard to raise"
    except cache_module.SQLiteUnavailableError:
        pass


def test_cli_hashing_without_cache_does_not_require_sqlite(monkeypatch, tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("hello")

    _, cli = reload_hashio_modules()
    import hashio.cache as cache_module

    monkeypatch.setattr(cache_module, "sqlite3", None)
    captured = []

    class DummyWorker:
        def __init__(self, **kwargs):
            captured.append(kwargs["use_cache"])
            self.path = kwargs["path"]
            self.total_time = 0

        def run(self):
            return None

        def progress_count(self):
            return 0

        def progress_bytes(self):
            return 0

    monkeypatch.setattr(cli, "HashWorker", DummyWorker)
    monkeypatch.setattr(cli, "get_encoder_class", lambda algo: object())

    assert cli.main([str(path)]) == 0
    assert captured == [False]


def test_cli_cache_commands_require_sqlite(monkeypatch, tmp_path, capsys):
    path = tmp_path / "file.txt"
    path.write_text("hello")

    _, cli = reload_hashio_modules()
    import hashio.cache as cache_module

    monkeypatch.setattr(cache_module, "sqlite3", None)

    assert cli.main(["--cache", str(path)]) == 2
    assert "sqlite3 support is required" in capsys.readouterr().err
