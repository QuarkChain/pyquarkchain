import unittest

from quarkchain.core import Branch
from quarkchain.cluster.neighbor import is_neighbor


class TestNeighbor(unittest.TestCase):
    def test_is_neighbor_same_chain_id(self):
        b1 = Branch(2 << 16 | 2 | 1)
        b2 = Branch(2 << 16 | 2 | 0)
        self.assertTrue(is_neighbor(b1, b2))

    def test_is_neighbor_same_shard_id(self):
        b1 = Branch(1 << 16 | 2 | 1)
        b2 = Branch(3 << 16 | 2 | 1)
        self.assertTrue(is_neighbor(b1, b2))

    def test_not_neighbor_diff_chain_id_and_diff_shard_id(self):
        b1 = Branch(1 << 16 | 2 | 0)
        b2 = Branch(3 << 16 | 2 | 1)
        self.assertFalse(is_neighbor(b1, b2))
