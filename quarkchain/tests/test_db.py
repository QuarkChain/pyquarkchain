import unittest
from quarkchain.db import InMemoryDb, ShardedDb


class TestShardedDb(unittest.TestCase):

    def testSimple(self):
        db = InMemoryDb()
        db1 = ShardedDb(db, fullShardId=0)
        db2 = ShardedDb(db, fullShardId=1)

        db1.put(b"abc", b"efg")
        db2.put(b"abc", b"aaa")

        self.assertEqual(db1.get(b"abc"), b"efg")
        self.assertEqual(db2.get(b"abc"), b"aaa")
        db1.remove(b"abc")

        self.assertEqual(db2.get(b"abc"), b"aaa")
        self.assertFalse(b"abc" in db1)
