import unittest

from quarkchain.core import Branch
from quarkchain.cluster.neighbor import is_neighbor


class TestNeighbor(unittest.TestCase):

    def test_is_neighbor_small_shard_size(self):
        b1 = Branch.create(32, 0)
        b2 = Branch.create(32, 7)
        self.assertTrue(is_neighbor(b1, b2))

    def test_is_neighbor_one_bit_difference(self):
        b1 = Branch.create(64, 5)
        b2 = Branch.create(64, 7)
        self.assertTrue(is_neighbor(b1, b2))

    def test_not_neighbor_two_bit_difference(self):
        b1 = Branch.create(64, 4)
        b2 = Branch.create(64, 7)
        self.assertFalse(is_neighbor(b1, b2))

    def test_raise_different_shard_size(self):
        b1 = Branch.create(64, 4)
        b2 = Branch.create(32, 5)
        with self.assertRaises(AssertionError):
            is_neighbor(b1, b2)

    def test_raise_same_shard_id(self):
        b1 = Branch.create(64, 5)
        b2 = Branch.create(64, 5)
        with self.assertRaises(AssertionError):
            is_neighbor(b1, b2)