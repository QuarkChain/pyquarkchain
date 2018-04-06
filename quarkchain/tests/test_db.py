import unittest
from quarkchain.db import RefcountedDb, InMemoryDb, ShardedDb


class TestRefcountedDb(unittest.TestCase):

    def testSimple(self):
        db = InMemoryDb()
        rdb = RefcountedDb(db)

        rdb.put(b"abc", b"efg")
        rdb.put(b"abc", b"efg")

        self.assertEqual(rdb.get(b"abc"), b"efg")
        self.assertEqual(rdb.getRefcount(b"abc"), 2)
        rdb.remove(b"abc")
        self.assertEqual(rdb.get(b"abc"), b"efg")
        rdb.remove(b"abc")
        self.assertFalse(b"abc" in rdb)


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
