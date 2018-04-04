import unittest
from quarkchain.db import RefcountedDb, InMemoryDb


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
