#!/usr/bin/env python

__doc__ = """
Contains hashio unit tests.
"""

import gzip
import hashlib
import os
import shutil
import tempfile
import threading
import unittest

import hashio


def flatten(l):
    """returns a flattened list."""
    return [i for s in l for i in s]


def write_to_file(filepath, data):
    """writes data to a file."""
    fp = open(filepath, "w")
    fp.write(data)
    fp.close()


def write_gzip_file(filepath, data):
    """writes gzipped text data to a file."""
    with gzip.open(filepath, "wb") as fp:
        fp.write(data.encode("utf-8"))


class TestUtils(unittest.TestCase):
    """Tests the utils module."""

    def test_get_metadata(self):
        from hashio.utils import get_metadata

        md = get_metadata(os.path.abspath(__file__))
        self.assertTrue(md is not None)
        # test for minimum required keys
        self.assertTrue("name" in md)
        self.assertTrue("mtime" in md)
        self.assertTrue("size" in md)
        self.assertTrue("type" in md)

    def test_is_ignorable(self):
        from hashio.utils import is_ignorable
        from hashio.config import CACHE_FILENAME

        self.assertTrue(is_ignorable(CACHE_FILENAME))

    def test_is_subpath(self):
        from hashio.utils import is_subpath

        self.assertTrue(is_subpath("/a/b/c/d.txt", "/a/b/c"))
        self.assertTrue(is_subpath("/a/b/c.txt", "/a/b/"))
        self.assertFalse(is_subpath("/a/b.txt", "/a/b/c"))

    def test_normalize_path(self):
        from hashio.utils import normalize_path

        # subpaths should not change
        p = "out.json"
        n = normalize_path(p)
        self.assertEqual(n, p)

        # nested subpaths should not change
        p = "nested/folder/out.json"
        n = normalize_path(p)
        self.assertEqual(n, p)

        # trailing slashes should be removed
        n = normalize_path("some/folder/")
        self.assertEqual(n, "some/folder")

        # abs paths should not change
        p = "/var/tmp/out.json"
        n = normalize_path(p)
        self.assertEqual(n, p)

        # abs paths where start is subpath of file
        p = "/var/tmp/out.json"
        n = normalize_path(p, start="/var/tmp")
        self.assertEqual(n, "out.json")

        p = "/var/tmp/out.json"
        n = normalize_path(p, start="/var")
        self.assertEqual(n, "tmp/out.json")

        # rel path where start is cwd (the default)
        p = os.path.relpath(__file__)
        n = normalize_path(p)
        self.assertEqual(n, p)

    def test_paths_are_equal(self):
        from hashio.utils import paths_are_equal

        self.assertTrue(paths_are_equal("/a/b/c.json", "/a/b/c.json"))
        self.assertTrue(
            paths_are_equal(
                __file__,
                os.path.join(
                    os.path.dirname(__file__), "..", "tests", "test_hashio.py"
                ),
            )
        )

    def test_read_file(self):
        from hashio.utils import read_file

        d1 = b""
        for d in read_file(__file__):
            d1 += d
        fp = open(__file__, "rb")
        d2 = fp.read()
        fp.close()
        self.assertEqual(d1, d2)

    def test_get_uncompressed_path(self):
        from hashio.utils import get_uncompressed_path

        self.assertEqual(get_uncompressed_path("sample.txt.gz"), "sample.txt")
        self.assertEqual(get_uncompressed_path("sample.txt"), "sample.txt")


class TestDedupe(unittest.TestCase):
    """Tests dedupe functions."""

    tempdir = None

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir)

    def test_dedupe_files(self):
        from hashio.encoder import dedupe_paths

        # create test source dir
        s1 = os.path.join(self.tempdir, "files")
        os.makedirs(s1)
        s2 = os.path.join(self.tempdir, "files", "nested")
        os.makedirs(s2)

        # write some files to source
        f1 = os.path.join(s1, "a.txt")
        f2 = os.path.join(s1, "b.txt")
        f3 = os.path.join(s1, "c.txt")
        f4 = os.path.join(s2, "d.txt")
        f5 = os.path.join(s2, "e.txt")

        # create some test files with dupe content
        write_to_file(f1, "foo")
        write_to_file(f2, "bar")
        write_to_file(f3, "foo")
        write_to_file(f4, "foo")
        write_to_file(f5, "bar")
        self.assertEqual(len(os.listdir(s1)), 4)

        # find all the dupes
        dupes = dedupe_paths([f1, f2])
        self.assertEqual(len(dupes), 0)

        dupes = dedupe_paths([f1, f5])
        self.assertEqual(len(dupes), 0)

        dupes = dedupe_paths([f1, f3])
        self.assertEqual(len(dupes), 1)

        dupes = dedupe_paths([f1, f2, f3])
        self.assertEqual(len(dupes), 1)

        dupes = dedupe_paths([f1, f2, f3, f4])
        self.assertEqual(len(dupes), 1)

        dupes = dedupe_paths([f1, f2, f3, f4, f5])
        self.assertEqual(len(dupes), 2)

        dupes = dedupe_paths([f1, f2, f3, f4, f5])
        all_files = set(flatten(dupes))
        self.assertEqual(len(all_files), 5)

        # update all of the files and make them unique
        write_to_file(f1, "a")
        write_to_file(f2, "b")
        write_to_file(f3, "c")
        write_to_file(f4, "d")
        write_to_file(f5, "e")
        dupes = dedupe_paths([f1, f2, f3, f4, f5])
        self.assertEqual(len(dupes), 0)

    def test_dedupe_dirs(self):
        from hashio.encoder import dedupe_paths

        # create test source dir
        s1 = os.path.join(self.tempdir, "dirs", "s1")
        os.makedirs(s1)

        # create a test target dir
        t1 = os.path.join(self.tempdir, "dirs", "t1")
        os.makedirs(t1)

        # create second target dir
        t2 = os.path.join(self.tempdir, "dirs", "t2")
        os.makedirs(t2)

        # write some files to source
        write_to_file(os.path.join(s1, "a.txt"), "foo")
        write_to_file(os.path.join(s1, "b.txt"), "bar")
        write_to_file(os.path.join(s1, "c.txt"), "baz")
        self.assertEqual(len(os.listdir(s1)), 3)

        # duplicate source files in target
        write_to_file(os.path.join(t1, "a.txt"), "foo")
        write_to_file(os.path.join(t1, "b.txt"), "bar")
        write_to_file(os.path.join(t1, "c.txt"), "baz")
        self.assertEqual(len(os.listdir(t1)), 3)

        # add a new file to target
        write_to_file(os.path.join(t1, "d.txt"), "qux")
        self.assertEqual(len(os.listdir(t1)), 4)

        # find dupes between t1 and s1
        dupes = dedupe_paths([t1, s1])
        self.assertEqual(len(dupes), 3)

        # change order of inputs
        dupes = dedupe_paths([s1, t1])
        self.assertEqual(len(dupes), 3)

        # test all files in set of dupes
        all_files = set(flatten(dupes))
        self.assertEqual(len(all_files), 6)

        # make ine input invalid
        dupes = dedupe_paths([s1, None])
        self.assertEqual(len(dupes), 0)

        # make ine input missing
        dupes = dedupe_paths([t1, "/this/dir/is/missing"])
        self.assertEqual(len(dupes), 0)

        # target dir is empty, no dupes
        self.assertEqual(len(dedupe_paths([t2, s1])), 0)

        # one dupe
        write_to_file(os.path.join(t2, "d.txt"), "foo")
        self.assertEqual(len(dedupe_paths([t2, s1])), 1)

        # two dupes
        write_to_file(os.path.join(t2, "e.txt"), "bar")
        self.assertEqual(len(dedupe_paths([t2, s1])), 2)

        # third dupe in subdir
        t2a = os.path.join(t2, "nested")
        os.makedirs(t2a)
        write_to_file(os.path.join(t2a, "c.txt"), "baz")
        self.assertEqual(len(dedupe_paths([t2, s1])), 3)

        # new file in target not in source
        write_to_file(os.path.join(t2, "g.txt"), "qux")
        self.assertEqual(len(dedupe_paths([t2, s1])), 3)

        # test three dirs
        write_to_file(os.path.join(t2, "g.txt"), "qux")
        self.assertEqual(len(dedupe_paths([t2, t1, s1])), 4)

        # update one of the target files
        write_to_file(os.path.join(t2, "d.txt"), "quuz")
        self.assertEqual(len(dedupe_paths([t2, s1])), 2)

        # update another of the target files
        write_to_file(os.path.join(t2, "e.txt"), "corge")
        self.assertEqual(len(dedupe_paths([t2, s1])), 1)

        # change it back
        write_to_file(os.path.join(t2, "e.txt"), "bar")
        self.assertEqual(len(dedupe_paths([t2, s1])), 2)


class TestEncoders(unittest.TestCase):
    """Tests the encoder module."""

    def test_get_encoder_class(self):
        from hashio.encoder import get_encoder_class

        self.assertEqual(get_encoder_class("c4"), hashio.encoder.C4Encoder)
        self.assertEqual(get_encoder_class("crc32"), hashio.encoder.CRC32Encoder)
        self.assertEqual(get_encoder_class("md5"), hashio.encoder.MD5Encoder)
        self.assertEqual(get_encoder_class("sha256"), hashio.encoder.SHA256Encoder)
        self.assertEqual(get_encoder_class("sha512"), hashio.encoder.SHA512Encoder)
        self.assertEqual(get_encoder_class("xxh64"), hashio.encoder.XXH64Encoder)

    def test_crc32_encoder(self):
        import zlib
        from hashio.encoder import CRC32Encoder
        from hashio.encoder import checksum_file

        h = zlib.crc32(b"some data")
        h_hex = format(h & 0xFFFFFFFF, "08x")

        encoder = CRC32Encoder()
        self.assertEqual(encoder.name, "crc32")

        encoder.update(b"some data")
        self.assertEqual(h_hex, encoder.hexdigest())

        encoder.update(b"more data  ")
        self.assertNotEqual(h_hex, encoder.hexdigest())

        # hash this file using standard lib
        fp = open(__file__, "rb")
        h = zlib.crc32(fp.read())
        h_hex = format(h & 0xFFFFFFFF, "08x")
        fp.close()

        # test standard lib against CRC32Encoder
        self.assertEqual(h_hex, checksum_file(__file__, encoder))

        # confirm rehashing results in same hash
        self.assertEqual(h_hex, checksum_file(__file__, encoder))

    def test_md5_encoder(self):
        from hashio.encoder import MD5Encoder
        from hashio.encoder import checksum_file

        h = hashlib.md5()
        h.update(b"some data")

        encoder = MD5Encoder()
        self.assertEqual(encoder.name, "md5")

        encoder.update(b"some data")
        self.assertEqual(h.hexdigest(), encoder.hexdigest())

        encoder.update(b"more data  ")
        self.assertNotEqual(h.hexdigest(), encoder.hexdigest())

        # hash this file using standard lib
        fp = open(__file__, "rb")
        h = hashlib.md5()
        h.update(fp.read())
        fp.close()

        # test standard lib against our encoder
        self.assertEqual(h.hexdigest(), checksum_file(__file__, encoder))

        # confirm rehashing results in same hash
        self.assertEqual(h.hexdigest(), checksum_file(__file__, encoder))

    def test_md5_encoder_uncompress_gzip(self):
        from hashio.encoder import MD5Encoder
        from hashio.encoder import checksum_file

        tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tempdir)

        filepath = os.path.join(tempdir, "payload.txt.gz")
        payload = "some gzipped payload"
        write_gzip_file(filepath, payload)

        expected = hashlib.md5(payload.encode("utf-8")).hexdigest()
        encoder = MD5Encoder()

        self.assertEqual(expected, checksum_file(filepath, encoder, uncompress=True))

    def test_xxh64_encoder(self):
        import xxhash
        from hashio.encoder import XXH64Encoder
        from hashio.encoder import checksum_file

        h = xxhash.xxh64()
        h.update(b"some data")

        encoder = XXH64Encoder()
        self.assertEqual(encoder.name, "xxh64")

        encoder.update(b"some data")
        self.assertEqual(h.hexdigest(), encoder.hexdigest())

        encoder.update(b"more data  ")
        self.assertNotEqual(h.hexdigest(), encoder.hexdigest())

        # hash this file using standard lib
        fp = open(__file__, "rb")
        h = xxhash.xxh64()
        h.update(fp.read())
        fp.close()

        # test standard lib against our encoder
        self.assertEqual(h.hexdigest(), checksum_file(__file__, encoder))

        # confirm rehashing results in same hash
        self.assertEqual(h.hexdigest(), checksum_file(__file__, encoder))


class TestExporters(unittest.TestCase):
    """Tests the exporter module."""

    tempdir = None

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir)

    def test_json_exporter(self):
        import json
        from hashio.exporter import JSONExporter

        filepath = os.path.join(self.tempdir, "test_json_exporter.json")

        # export some data
        exporter = JSONExporter(filepath)
        exporter.write("/a/b/c1", {"a": 1, "b": 2})
        exporter.write("/a/b/c2", {"c": 3, "d": 4})
        exporter.write("/a/b/c3", {"e": 5, "f": 6})
        exporter.close()

        # verify export is valid json data
        fp = open(filepath)
        d1 = json.load(fp)
        fp.close()
        d2 = JSONExporter.read(filepath)
        self.assertEqual(d1, d2)

        self.assertEqual(list(d1.keys()), ["/a/b/c1", "/a/b/c2", "/a/b/c3"])
        self.assertEqual(d1["/a/b/c1"]["a"], 1)
        self.assertEqual(d2["/a/b/c2"]["d"], 4)
        self.assertEqual(d1["/a/b/c3"]["e"], 5)

    def test_mhl_exporter(self):
        from lxml import etree
        from hashio.exporter import MHLExporter

        filepath = os.path.join(self.tempdir, "test_mhl_exporter.json")

        # export some data
        exporter = MHLExporter(filepath)
        exporter.write("/a/b/c1", {"a": 1, "b": 2})
        exporter.write("/a/b/c2", {"c": 3, "d": 4})
        exporter.write("/a/b/c3", {"e": 5, "f": 6})
        exporter.close()

        # verify export is valid xml data
        root = etree.parse(filepath)
        self.assertEqual(type(root), etree._ElementTree)
        data = etree.tostring(root)
        self.assertEqual(type(data), bytes)

    def test_checksum_data(self):
        from hashio.encoder import checksum_data, XXH64Encoder

        text_data = b"hello, world\n"
        expected_xxh64 = "abdc2a61f1f91f4c"
        encoder = XXH64Encoder()
        checksum = checksum_data(text_data, encoder)
        self.assertEqual(checksum, expected_xxh64)

    def test_checksum_text(self):
        from hashio.encoder import checksum_text, XXH64Encoder

        text_data = "hello, world\n"
        expected_xxh64 = "abdc2a61f1f91f4c"
        encoder = XXH64Encoder()
        checksum = checksum_text(text_data, encoder)
        self.assertEqual(checksum, expected_xxh64)


class TestCompositeHash(unittest.TestCase):
    """Tests the composite_hash function."""

    def test_composite_hash(self):
        from hashio.encoder import XXH64Encoder, composite_hash

        # prepare test data
        hashlist = [
            ("file1.txt", "hash1"),
            ("file2.txt", "hash2"),
            ("file3.txt", "hash3"),
        ]

        # create a composite hash
        encoder = XXH64Encoder()
        composite = composite_hash(hashlist, encoder)

        # verify the composite hash is not empty
        self.assertIsNotNone(composite)
        self.assertIsInstance(composite, str)

        # verify the length of the composite hash (XXH64 produces a 16-character
        # hex string)
        self.assertEqual(len(composite), 16)

        # check if the composite hash is deterministic
        composite2 = composite_hash(hashlist, encoder)
        self.assertEqual(composite, composite2)

    def test_empty_hashlist(self):
        from hashio.encoder import XXH64Encoder, composite_hash, checksum_text

        # test with an empty hashlist
        encoder = XXH64Encoder()
        composite = composite_hash([], encoder)

        # verify the composite hash for an empty list
        self.assertIsNotNone(composite)
        self.assertEqual(composite, checksum_text("", encoder))


class TestUncompress(unittest.TestCase):
    """Tests gzip uncompress support."""

    tempdir = None

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir)

    def test_worker_output_helpers_uncompress_gzip(self):
        from hashio.worker import get_output_path, update_output_metadata

        source_dir = os.path.join(self.tempdir, "worker")
        os.makedirs(source_dir, exist_ok=True)

        gzip_path = os.path.join(source_dir, "sample.txt.gz")
        metadata = {
            "name": "sample.txt.gz",
            "size": 999,
        }
        payload_size = len(b"hello from gzip worker")

        self.assertEqual(
            get_output_path(gzip_path, start=source_dir, uncompress=True),
            "sample.txt",
        )

        adjusted = update_output_metadata(
            metadata, gzip_path, payload_size, uncompress=True
        )
        self.assertEqual(adjusted["name"], "sample.txt")
        self.assertEqual(adjusted["size"], payload_size)

    def test_verify_checksums_uses_gzip_fallback(self):
        from hashio.encoder import verify_checksums
        from hashio.exporter import JSONExporter

        source_dir = os.path.join(self.tempdir, "verify")
        os.makedirs(source_dir, exist_ok=True)

        manifest_path = os.path.join(source_dir, "hash.json")
        gzip_path = os.path.join(source_dir, "sample.txt.gz")
        manifest_name = "sample.txt"
        payload = "initial gzip payload"
        payload_hash = hashlib.md5(payload.encode("utf-8")).hexdigest()

        write_gzip_file(gzip_path, payload)

        exporter = JSONExporter(manifest_path)
        exporter.write(
            manifest_name,
            {
                "name": manifest_name,
                "mtime": 0,
                "size": len(payload.encode("utf-8")),
                "md5": payload_hash,
            },
        )
        exporter.close()

        self.assertEqual(
            [],
            list(verify_checksums(manifest_path, start=source_dir, uncompress=True)),
        )

        write_gzip_file(gzip_path, "changed gzip payload")

        misses = list(
            verify_checksums(manifest_path, start=source_dir, uncompress=True)
        )
        self.assertEqual(len(misses), 1)
        self.assertEqual(misses[0][0], "md5")
        self.assertEqual(misses[0][2], os.path.join(source_dir, manifest_name))


class TestShutdown(unittest.TestCase):
    """Tests shutdown and interrupt helpers."""

    def test_safe_join_thread_suppresses_keyboard_interrupt(self):
        from hashio.cli import safe_join_thread

        class DummyThread:
            def join(self, timeout=None):
                raise KeyboardInterrupt()

            def is_alive(self):
                return True

        self.assertFalse(safe_join_thread(DummyThread(), timeout=0.1))

    def test_stop_workers_suppresses_keyboard_interrupt(self):
        from hashio.cli import stop_workers

        calls = []

        class DummyWorker:
            def __init__(self, name, should_interrupt=False):
                self.name = name
                self.should_interrupt = should_interrupt

            def stop(self):
                calls.append(self.name)
                if self.should_interrupt:
                    raise KeyboardInterrupt()

        stop_workers(
            [
                DummyWorker("first", should_interrupt=True),
                DummyWorker("second"),
            ]
        )
        self.assertEqual(calls, ["first", "second"])

    def test_hashworker_stop_sets_done_before_finish(self):
        from hashio.worker import HashWorker

        worker = HashWorker.__new__(HashWorker)
        worker.done = threading.Event()
        worker.verbose = True

        class DummyPool:
            def terminate(self):
                return None

            def join(self):
                return None

        worker.pool = DummyPool()

        state = {"done_before_finish": False}

        def finish():
            state["done_before_finish"] = worker.done.is_set()
            raise KeyboardInterrupt()

        worker.finish = finish
        worker.stop()
        self.assertTrue(state["done_before_finish"])

    def test_hashworker_run_sets_done_before_finish(self):
        from hashio.worker import HashWorker

        worker = HashWorker.__new__(HashWorker)
        worker.start_time = 0.0
        worker.path = "dummy"
        worker.done = threading.Event()
        worker.add_path_to_queue = lambda path: None

        class DummyPool:
            def close(self):
                return None

            def join(self):
                return None

        worker.pool = DummyPool()

        state = {"done_before_finish": False}

        def finish():
            state["done_before_finish"] = worker.done.is_set()

        worker.finish = finish
        worker.run()
        self.assertTrue(state["done_before_finish"])


if __name__ == "__main__":
    unittest.main()
