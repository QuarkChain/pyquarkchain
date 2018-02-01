import unittest
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import RootBlock
from quarkchain.genesis import create_genesis_root_block


class TestGenesisRootBlock(unittest.TestCase):

    def testBlockSerialization(self):
        block = create_genesis_root_block(DEFAULT_ENV)
        s = block.serialize(bytearray())
        nblock = RootBlock.deserialize(s)
        self.assertEqual(block, nblock)
