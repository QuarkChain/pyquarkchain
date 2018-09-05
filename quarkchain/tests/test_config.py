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
    "SHARD_SIZE": 8,
    "MAX_NEIGHBORS": 32,
    "MINOR_BLOCK_DEFAULT_REWARD": 100000000000000000000,
    "NETWORK_ID": 3,
    "TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD": 10000,
    "BLOCK_EXTRA_DATA_SIZE_LIMIT": 1024,
    "P2P_PROTOCOL_VERSION": 0,
    "P2P_COMMAND_SIZE_LIMIT": 4294967295,
    "SKIP_ROOT_DIFFICULTY_CHECK": false,
    "SKIP_MINOR_DIFFICULTY_CHECK": false,
    "ROOT": {
        "MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF": 60,
        "CONSENSUS_TYPE": "POW_SIMULATE",
        "CONSENSUS_CONFIG": {
            "TARGET_BLOCK_TIME": 60
        },
        "GENESIS": {
            "VERSION": 0,
            "HEIGHT": 0,
            "SHARD_SIZE": 32,
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5,
            "HASH_PREV_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
            "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
            "TIMESTAMP": 1519147489,
            "DIFFICULTY": 1000000,
            "NONCE": 0
        }
    },
    "SHARD_LIST": [
        {
            "CONSENSUS_TYPE": "POW_SHA3SHA3",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
                "COINBASE_AMOUNT": 5,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "NONCE": 0,
                "ALLOC": {},
                "HASH": null
            }
        },
        {
            "CONSENSUS_TYPE": "POW_SHA3SHA3",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
                "COINBASE_AMOUNT": 5,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "NONCE": 0,
                "ALLOC": {},
                "HASH": null
            }
        },
        {
            "CONSENSUS_TYPE": "POW_ETHASH",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
                "COINBASE_AMOUNT": 5,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "NONCE": 0,
                "ALLOC": {},
                "HASH": null
            }
        },
        {
            "CONSENSUS_TYPE": "POW_ETHASH",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
                "COINBASE_AMOUNT": 5,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "NONCE": 0,
                "ALLOC": {},
                "HASH": null
            }
        },
        {
            "CONSENSUS_TYPE": "NONE"
        }
    ]
}"""
        self.assertEqual(config.to_json(), expected_json)
        deserialized_config = QuarkChainConfig.from_json(expected_json)
        self.assertEqual(deserialized_config.to_json(), expected_json)