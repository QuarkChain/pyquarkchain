import unittest

from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.core import Branch, MinorBlockHeader, MinorBlock, MinorBlockMeta
from quarkchain.db import InMemoryDb
from quarkchain.env import DEFAULT_ENV


class TestShardDbOperator(unittest.TestCase):
    def test_get_minor_block_by_hash(self):
        db = ShardDbOperator(InMemoryDb(), DEFAULT_ENV, Branch(2))
        block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
        block_hash = block.header.get_hash()
        db.put_minor_block(block, [])
        self.assertEqual(db.get_minor_block_by_hash(block_hash), block)
        self.assertIsNone(db.get_minor_block_by_hash(b""))

        self.assertEqual(db.get_minor_block_header_by_hash(block_hash), block.header)
        self.assertIsNone(db.get_minor_block_header_by_hash(b""))
