import unittest
from fractions import Fraction

from quarkchain.config import (
    ConsensusType,
    POWConfig,
    ShardConfig,
    RootConfig,
    QuarkChainConfig,
)


class TestShardConfig(unittest.TestCase):
    def test_basic(self):
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
    "NETWORK_ID": 3,
    "TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD": 10000,
    "BLOCK_EXTRA_DATA_SIZE_LIMIT": 1024,
    "GUARDIAN_PUBLIC_KEY": "ab856abd0983a82972021e454fcf66ed5940ed595b0898bcd75cbe2d0a51a00f5358b566df22395a2a8bf6c022c1d51a2c3defe654e91a8d244947783029694d",
    "GUARDIAN_PRIVATE_KEY": null,
    "P2P_PROTOCOL_VERSION": 0,
    "P2P_COMMAND_SIZE_LIMIT": 4294967295,
    "SKIP_ROOT_DIFFICULTY_CHECK": false,
    "SKIP_MINOR_DIFFICULTY_CHECK": false,
    "ROOT": {
        "MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF": 60,
        "CONSENSUS_TYPE": "POW_SIMULATE",
        "CONSENSUS_CONFIG": {
            "TARGET_BLOCK_TIME": 60,
            "REMOTE_MINE": false
        },
        "GENESIS": {
            "VERSION": 0,
            "HEIGHT": 0,
            "SHARD_SIZE": 32,
            "HASH_PREV_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
            "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
            "TIMESTAMP": 1519147489,
            "DIFFICULTY": 1000000,
            "NONCE": 0
        },
        "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
        "COINBASE_AMOUNT": 120000000000000000000,
        "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 40,
        "DIFFICULTY_ADJUSTMENT_FACTOR": 1024
    },
    "SHARD_LIST": [
        {
            "CONSENSUS_TYPE": "POW_SHA3SHA3",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10,
                "REMOTE_MINE": false
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "GAS_LIMIT": 12000000,
                "NONCE": 0,
                "ALLOC": {}
            },
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5000000000000000000,
            "GAS_LIMIT_EMA_DENOMINATOR": 1024,
            "GAS_LIMIT_ADJUSTMENT_FACTOR": 1024,
            "GAS_LIMIT_MINIMUM": 5000,
            "GAS_LIMIT_MAXIMUM": 9223372036854775807,
            "GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR": 3,
            "GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR": 2,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3
        },
        {
            "CONSENSUS_TYPE": "POW_SHA3SHA3",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 10,
                "REMOTE_MINE": false
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "GAS_LIMIT": 12000000,
                "NONCE": 0,
                "ALLOC": {}
            },
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5000000000000000000,
            "GAS_LIMIT_EMA_DENOMINATOR": 1024,
            "GAS_LIMIT_ADJUSTMENT_FACTOR": 1024,
            "GAS_LIMIT_MINIMUM": 5000,
            "GAS_LIMIT_MAXIMUM": 9223372036854775807,
            "GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR": 3,
            "GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR": 2,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3
        },
        {
            "CONSENSUS_TYPE": "POW_ETHASH",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15,
                "REMOTE_MINE": false
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "GAS_LIMIT": 12000000,
                "NONCE": 0,
                "ALLOC": {}
            },
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5000000000000000000,
            "GAS_LIMIT_EMA_DENOMINATOR": 1024,
            "GAS_LIMIT_ADJUSTMENT_FACTOR": 1024,
            "GAS_LIMIT_MINIMUM": 5000,
            "GAS_LIMIT_MAXIMUM": 9223372036854775807,
            "GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR": 3,
            "GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR": 2,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3
        },
        {
            "CONSENSUS_TYPE": "POW_ETHASH",
            "CONSENSUS_CONFIG": {
                "TARGET_BLOCK_TIME": 15,
                "REMOTE_MINE": false
            },
            "GENESIS": {
                "ROOT_HEIGHT": 0,
                "VERSION": 0,
                "HEIGHT": 0,
                "HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
                "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
                "EXTRA_DATA": "497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73",
                "TIMESTAMP": 1519147489,
                "DIFFICULTY": 10000,
                "GAS_LIMIT": 12000000,
                "NONCE": 0,
                "ALLOC": {}
            },
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5000000000000000000,
            "GAS_LIMIT_EMA_DENOMINATOR": 1024,
            "GAS_LIMIT_ADJUSTMENT_FACTOR": 1024,
            "GAS_LIMIT_MINIMUM": 5000,
            "GAS_LIMIT_MAXIMUM": 9223372036854775807,
            "GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR": 3,
            "GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR": 2,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3
        },
        {
            "CONSENSUS_TYPE": "NONE",
            "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
            "COINBASE_AMOUNT": 5000000000000000000,
            "GAS_LIMIT_EMA_DENOMINATOR": 1024,
            "GAS_LIMIT_ADJUSTMENT_FACTOR": 1024,
            "GAS_LIMIT_MINIMUM": 5000,
            "GAS_LIMIT_MAXIMUM": 9223372036854775807,
            "GAS_LIMIT_USAGE_ADJUSTMENT_NUMERATOR": 3,
            "GAS_LIMIT_USAGE_ADJUSTMENT_DENOMINATOR": 2,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3
        }
    ],
    "REWARD_TAX_RATE": 0.5
}"""
        print(config.to_json())
        self.assertEqual(config.to_json(), expected_json)
        deserialized_config = QuarkChainConfig.from_json(expected_json)
        self.assertEqual(deserialized_config.to_json(), expected_json)


class TestQuarkChainConfig(unittest.TestCase):
    def test_reward_tax_rate(self):
        config = QuarkChainConfig()
        self.assertEqual(config.reward_tax_rate, Fraction(1, 2))
        config.REWARD_TAX_RATE = 0.33
        self.assertEqual(config.reward_tax_rate, Fraction(33, 100))
        config.REWARD_TAX_RATE = 0.8
        self.assertEqual(config.reward_tax_rate, Fraction(4, 5))
        config.REWARD_TAX_RATE = 0.123
        with self.assertRaises(AssertionError):
            _ = config.reward_tax_rate
