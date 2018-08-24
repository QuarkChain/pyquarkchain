import unittest

from quarkchain.config import (
    ConsensusType,
    POWConfig,
    ShardConfig,
    RootConfig,
    QuarkChainConfig,
)


class TestShardConfig(unittest.TestCase):

    def testBasic(self):
        config = QuarkChainConfig()
        config.ROOT = RootConfig()
        config.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        config.ROOT.CONSENSUS_CONFIG = POWConfig()
        config.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 60

        config.SHARD_LIST = []
        for i in range(2):
            s = ShardConfig()
            s.CONSENSUS_TYPE = ConsensusType.POW_SHA3SHA3
            s.CONSENSUS_CONFIG = POWConfig()
            config.SHARD_LIST.append(s)

        for i in range(2):
            s = ShardConfig()
            s.CONSENSUS_TYPE = ConsensusType.POW_ETHASH
            s.CONSENSUS_CONFIG = POWConfig()
            s.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 15
            config.SHARD_LIST.append(s)

        for i in range(1):
            s = ShardConfig()
            config.SHARD_LIST.append(s)

        expected_json = """{
    "ROOT": {
        "CONSENSUS_TYPE": 3,
        "CONSENSUS_CONFIG": {
            "TARGET_BLOCK_TIME": 60
        }
    },
    "SHARD_LIST": [
        {
            "CONSENSUS_TYPE": 2,
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10
            }
        },
        {
            "CONSENSUS_TYPE": 2,
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10
            }
        },
        {
            "CONSENSUS_TYPE": 1,
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15
            }
        },
        {
            "CONSENSUS_TYPE": 1,
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15
            }
        },
        {
            "CONSENSUS_TYPE": 0
        }
    ]
}"""
        self.assertEqual(config.to_json(), expected_json)
        deserialized_config = QuarkChainConfig.from_json(expected_json)
        self.assertEqual(deserialized_config.to_json(), expected_json)