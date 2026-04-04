"""
Smoke tests for PersistentDb after migration from python-rocksdb to rocksdict.
Tests every method in PersistentDb to verify rocksdict behaves correctly.
"""
import tempfile
import unittest

from quarkchain.db import PersistentDb


class TestPersistentDb(unittest.TestCase):
    def setUp(self):
        # Use a temporary directory that is cleaned up after each test
        self.tmp_dir = tempfile.mkdtemp()
        self.db = PersistentDb(self.tmp_dir)

    def tearDown(self):
        self.db.close()

    def test_put_and_get_bytes_key(self):
        self.db.put(b"key1", b"value1")
        self.assertEqual(self.db.get(b"key1"), b"value1")

    def test_put_and_get_str_key(self):
        # keys can be str (auto-encoded to bytes internally)
        self.db.put("key2", b"value2")
        self.assertEqual(self.db.get("key2"), b"value2")

    def test_get_missing_key_returns_default(self):
        self.assertIsNone(self.db.get(b"nonexistent"))
        self.assertEqual(self.db.get(b"nonexistent", b"default"), b"default")

    def test_put_bytearray_value(self):
        # bytearray values are accepted and stored as bytes
        self.db.put(b"key3", bytearray(b"value3"))
        self.assertEqual(self.db.get(b"key3"), b"value3")

    def test_contains(self):
        self.db.put(b"key4", b"value4")
        self.assertIn(b"key4", self.db)
        self.assertNotIn(b"missing", self.db)

    def test_delete(self):
        self.db.put(b"key5", b"value5")
        self.assertIn(b"key5", self.db)
        self.db.delete(b"key5")
        self.assertNotIn(b"key5", self.db)
        self.assertIsNone(self.db.get(b"key5"))

    def test_remove(self):
        # remove() is an alias for delete()
        self.db.put(b"key6", b"value6")
        self.db.remove(b"key6")
        self.assertNotIn(b"key6", self.db)

    def test_getitem_raises_on_missing(self):
        with self.assertRaises(KeyError):
            _ = self.db[b"nonexistent"]

    def test_getitem_returns_value(self):
        self.db.put(b"key7", b"value7")
        self.assertEqual(self.db[b"key7"], b"value7")

    def test_multi_get(self):
        self.db.put(b"k1", b"v1")
        self.db.put(b"k2", b"v2")
        result = self.db.multi_get([b"k1", b"k2", b"missing"])
        self.assertEqual(result[b"k1"], b"v1")
        self.assertEqual(result[b"k2"], b"v2")
        self.assertIsNone(result[b"missing"])

    def test_range_iter_ascending(self):
        # Keys are sorted lexicographically by RocksDB
        for i in range(5):
            self.db.put(i.to_bytes(4, "big"), f"val_{i}".encode())

        results = list(self.db.range_iter(
            (1).to_bytes(4, "big"),
            (4).to_bytes(4, "big"),
        ))
        # Should yield keys 1, 2, 3 (start inclusive, end exclusive)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][0], (1).to_bytes(4, "big"))
        self.assertEqual(results[1][0], (2).to_bytes(4, "big"))
        self.assertEqual(results[2][0], (3).to_bytes(4, "big"))

    def test_reversed_range_iter_descending(self):
        for i in range(5):
            self.db.put(i.to_bytes(4, "big"), f"val_{i}".encode())

        results = list(self.db.reversed_range_iter(
            (3).to_bytes(4, "big"),
            (0).to_bytes(4, "big"),
        ))
        # Should yield keys 3, 2, 1 (start inclusive, end exclusive)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][0], (3).to_bytes(4, "big"))
        self.assertEqual(results[1][0], (2).to_bytes(4, "big"))
        self.assertEqual(results[2][0], (1).to_bytes(4, "big"))

    def test_data_persists_after_close_and_reopen(self):
        # The most important property of a persistent DB
        self.db.put(b"persist_key", b"persist_value")
        self.db.close()

        # Re-open the same path
        db2 = PersistentDb(self.tmp_dir)
        self.assertEqual(db2.get(b"persist_key"), b"persist_value")
        db2.close()

        # Re-assign so tearDown's close() doesn't double-close
        self.db = PersistentDb(self.tmp_dir)

    def test_clean_flag_wipes_existing_data(self):
        self.db.put(b"wipe_me", b"value")
        self.db.close()

        # Re-open with clean=True — should destroy previous data
        self.db = PersistentDb(self.tmp_dir, clean=True)
        self.assertIsNone(self.db.get(b"wipe_me"))


if __name__ == "__main__":
    unittest.main()
