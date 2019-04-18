import unittest
import os

from quarkchain.evm.securetrie import SecureTrie
from quarkchain.evm.trie import Trie
from quarkchain.db import InMemoryDb

test_case_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'fixtures', 'TrieTests')


class TestLocalData(unittest.TestCase):
    def __test_with_file(self, filename):
        t = SecureTrie(Trie(InMemoryDb()))
        with open(os.path.join(test_case_path, filename)) as f:
            for line in f:
                values = line.strip("\n\r").split(" ")
                self.assertEqual(len(values), 3)

                key = int(values[0])
                value = bytes.fromhex(values[1])
                trie_hash = bytes.fromhex(values[2])

                t.update(key.to_bytes(4, byteorder="little"), value)
                self.assertEqual(t.trie.root_hash, trie_hash)

    def test_all(self):
        self.__test_with_file("test_case_0")
        self.__test_with_file("test_case_1")
        self.__test_with_file("test_case_2")
        self.__test_with_file("test_case_3")
        self.__test_with_file("test_case_4")
        self.__test_with_file("test_case_5")
