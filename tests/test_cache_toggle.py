#!/usr/bin/env python

import importlib
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
