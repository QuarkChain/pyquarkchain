import unittest
from fractions import Fraction

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.config import (
    ChainConfig,
    ConsensusType,
    POWConfig,
    QuarkChainConfig,
    RootConfig,
    ShardConfig,
)
from quarkchain.constants import PRECOMPILED_CONTRACTS_AFTER_EVM_ENABLED
from quarkchain.env import Env
from quarkchain.evm.specials import specials


class TestQuarkChainConfig(unittest.TestCase):
    def test_serialization(self):
        config = QuarkChainConfig()
        config.ROOT = RootConfig()
        config.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        config.ROOT.CONSENSUS_CONFIG = POWConfig()
        config.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 60

        config.CHAIN_SIZE = 3
        config.CHAINS = []
        for i in range(config.CHAIN_SIZE):
            chain_config = ChainConfig()
            chain_config.CHAIN_ID = i
            chain_config.ETH_CHAIN_ID = config.BASE_ETH_CHAIN_ID + 1 + i
            chain_config.SHARD_SIZE = 2
            chain_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            chain_config.CONSENSUS_CONFIG = POWConfig()
            config.CHAINS.append(chain_config)

        expected_json = """
{
    "CHAIN_SIZE": 3,
    "BASE_ETH_CHAIN_ID": 100000,
    "MAX_NEIGHBORS": 32,
    "NETWORK_ID": 3,
    "TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD": 10000,
    "BLOCK_EXTRA_DATA_SIZE_LIMIT": 1024,
    "GUARDIAN_PUBLIC_KEY": "ab856abd0983a82972021e454fcf66ed5940ed595b0898bcd75cbe2d0a51a00f5358b566df22395a2a8bf6c022c1d51a2c3defe654e91a8d244947783029694d",
    "ROOT_SIGNER_PRIVATE_KEY": null,
    "P2P_PROTOCOL_VERSION": 0,
    "P2P_COMMAND_SIZE_LIMIT": 134217728,
    "SKIP_ROOT_DIFFICULTY_CHECK": false,
    "SKIP_ROOT_COINBASE_CHECK": false,
    "SKIP_MINOR_DIFFICULTY_CHECK": false,
    "GENESIS_TOKEN": "QKC",
    "ROOT": {
        "MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF": 22500,
        "MAX_ROOT_BLOCKS_IN_MEMORY": 256,
        "CONSENSUS_TYPE": "POW_SIMULATE",
        "CONSENSUS_CONFIG": {
            "TARGET_BLOCK_TIME": 60,
            "REMOTE_MINE": false
        },
        "GENESIS": {
            "VERSION": 0,
            "HEIGHT": 0,
            "HASH_PREV_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
            "HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
            "TIMESTAMP": 1519147489,
            "DIFFICULTY": 1000000,
            "NONCE": 0
        },
        "COINBASE_ADDRESS": "000000000000000000000000000000000000000000000000",
        "COINBASE_AMOUNT": 120000000000000000000,
        "EPOCH_INTERVAL": 2100000,
        "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 40,
        "DIFFICULTY_ADJUSTMENT_FACTOR": 1024,
        "POSW_CONFIG": {
            "ENABLED": false,
            "ENABLE_TIMESTAMP": 0,
            "DIFF_DIVIDER": 1000,
            "WINDOW_SIZE": 4320,
            "TOTAL_STAKE_PER_BLOCK": 240000000000000000000000,
            "BOOST_TIMESTAMP": 0,
            "BOOST_MULTIPLIER_PER_STEP": 2,
            "BOOST_STEPS": 10,
            "BOOST_STEP_INTERVAL": 172800
        }
    },
    "CHAINS": [
        {
            "CHAIN_ID": 0,
            "ETH_CHAIN_ID": 100001,
            "SHARD_SIZE": 2,
            "DEFAULT_CHAIN_TOKEN": "QKC",
            "CONSENSUS_TYPE": "POW_DOUBLESHA256",
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
            "EPOCH_INTERVAL": 12600000,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3,
            "POSW_CONFIG": {
                "ENABLED": false,
                "ENABLE_TIMESTAMP": 0,
                "DIFF_DIVIDER": 20,
                "WINDOW_SIZE": 256,
                "TOTAL_STAKE_PER_BLOCK": 1000000000000000000000000000,
                "BOOST_TIMESTAMP": 0,
                "BOOST_MULTIPLIER_PER_STEP": 2,
                "BOOST_STEPS": 10,
                "BOOST_STEP_INTERVAL": 43200
            },
            "MAX_MINOR_BLOCKS_IN_MEMORY": 1536
        },
        {
            "CHAIN_ID": 1,
            "ETH_CHAIN_ID": 100002,
            "SHARD_SIZE": 2,
            "DEFAULT_CHAIN_TOKEN": "QKC",
            "CONSENSUS_TYPE": "POW_DOUBLESHA256",
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
            "EPOCH_INTERVAL": 12600000,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3,
            "POSW_CONFIG": {
                "ENABLED": false,
                "ENABLE_TIMESTAMP": 0,
                "DIFF_DIVIDER": 20,
                "WINDOW_SIZE": 256,
                "TOTAL_STAKE_PER_BLOCK": 1000000000000000000000000000,
                "BOOST_TIMESTAMP": 0,
                "BOOST_MULTIPLIER_PER_STEP": 2,
                "BOOST_STEPS": 10,
                "BOOST_STEP_INTERVAL": 43200
            },
            "MAX_MINOR_BLOCKS_IN_MEMORY": 1536
        },
        {
            "CHAIN_ID": 2,
            "ETH_CHAIN_ID": 100003,
            "SHARD_SIZE": 2,
            "DEFAULT_CHAIN_TOKEN": "QKC",
            "CONSENSUS_TYPE": "POW_DOUBLESHA256",
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
            "EPOCH_INTERVAL": 12600000,
            "DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
            "DIFFICULTY_ADJUSTMENT_FACTOR": 512,
            "EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK": 3,
            "POSW_CONFIG": {
                "ENABLED": false,
                "ENABLE_TIMESTAMP": 0,
                "DIFF_DIVIDER": 20,
                "WINDOW_SIZE": 256,
                "TOTAL_STAKE_PER_BLOCK": 1000000000000000000000000000,
                "BOOST_TIMESTAMP": 0,
                "BOOST_MULTIPLIER_PER_STEP": 2,
                "BOOST_STEPS": 10,
                "BOOST_STEP_INTERVAL": 43200
            },
            "MAX_MINOR_BLOCKS_IN_MEMORY": 1536
        }
    ],
    "SHARDS": null,
    "REWARD_TAX_RATE": 0.5,
    "BLOCK_REWARD_DECAY_FACTOR": 0.5,
    "ENABLE_TX_TIMESTAMP": null,
    "TX_WHITELIST_SENDERS": [],
    "ENABLE_EVM_TIMESTAMP": null,
    "ENABLE_QKCHASHX_HEIGHT": null,
    "ENABLE_EIP155_SIGNER_TIMESTAMP": null,
    "ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP": null,
    "ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP": null,
    "ENABLE_POSW_STAKING_DECAY_TIMESTAMP": null,
    "MIN_TX_POOL_GAS_PRICE": 1000000000,
    "MIN_MINING_GAS_PRICE": 1000000000,
    "XSHARD_GAS_DDOS_FIX_ROOT_HEIGHT": 90000,
    "DISABLE_POW_CHECK": false,
    "ROOT_CHAIN_POSW_CONTRACT_BYTECODE_HASH": "0000000000000000000000000000000000000000000000000000000000000000"
}
    """
        expected_json = expected_json.strip()
        print(config.to_json())
        self.assertEqual(config.to_json(), expected_json)
        deserialized_config = QuarkChainConfig.from_json(expected_json)
        self.assertEqual(deserialized_config.to_json(), expected_json)

        self.assertEqual(deserialized_config.get_shard_size_by_chain_id(0), 2)
        self.assertEqual(deserialized_config.get_shard_size_by_chain_id(1), 2)

    def test_missing_one_shard_config(self):
        config = QuarkChainConfig()
        config.CHAIN_SIZE = 1
        config.shards = dict()
        for i in range(1):
            s = ShardConfig(ChainConfig())
            s.CHAIN_ID = 0
            s.SHARD_SIZE = 2
            s.SHARD_ID = i
            s.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            s.CONSENSUS_CONFIG = POWConfig()
            config.shards[0 | 2 | i] = s

        with self.assertRaises(AssertionError):
            config.init_and_validate()

    def test_bad_shard_size(self):
        config = QuarkChainConfig()
        config.CHAIN_SIZE = 1
        config.shards = dict()
        for i in range(3):
            s = ShardConfig(ChainConfig())
            s.CHAIN_ID = 0
            s.SHARD_SIZE = 3  # not power of 2
            s.SHARD_ID = i
            s.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            s.CONSENSUS_CONFIG = POWConfig()
            config.shards[0 | 2 | i] = s

        with self.assertRaises(AssertionError):
            config.init_and_validate()

    def test_shard_size_not_match(self):
        config = QuarkChainConfig()
        config.CHAIN_SIZE = 1
        config.shards = dict()
        for i in range(2):
            s = ShardConfig(ChainConfig())
            s.CHAIN_ID = 0
            s.SHARD_SIZE = i
            s.SHARD_ID = i
            s.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            s.CONSENSUS_CONFIG = POWConfig()
            config.shards[0 | 2 | i] = s

        with self.assertRaises(AssertionError):
            config.init_and_validate()

    def test_bad_shard_id(self):
        config = QuarkChainConfig()
        config.CHAIN_SIZE = 1
        config.shards = dict()
        for i in range(2):
            s = ShardConfig(ChainConfig())
            s.CHAIN_ID = 0
            s.SHARD_SIZE = 2
            s.SHARD_ID = 0
            s.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            s.CONSENSUS_CONFIG = POWConfig()
            config.shards[0 | 2 | i] = s

        with self.assertRaises(AssertionError):
            config.init_and_validate()

    def test_bad_chain_id(self):
        config = QuarkChainConfig()
        config.CHAIN_SIZE = 1
        config.shards = dict()
        for i in range(2):
            s = ShardConfig(ChainConfig())
            s.CHAIN_ID = 1
            s.SHARD_SIZE = 2
            s.SHARD_ID = i
            s.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
            s.CONSENSUS_CONFIG = POWConfig()
            config.shards[1 << 16 | 2 | i] = s

        with self.assertRaises(AssertionError):
            config.init_and_validate()

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

    def test_special_contract_enable_ts(self):
        env = Env()
        for addr in PRECOMPILED_CONTRACTS_AFTER_EVM_ENABLED:
            self.assertEqual(specials[addr][1], 0)
        cluster_config = ClusterConfig()
        cluster_config.QUARKCHAIN.ENABLE_EVM_TIMESTAMP = 123
        env.cluster_config = cluster_config
        for addr in PRECOMPILED_CONTRACTS_AFTER_EVM_ENABLED:
            self.assertEqual(specials[addr][1], 123)

    def test_get_diff_divider(self):
        block_timestamp = 1646064000
        config = QuarkChainConfig().ROOT.POSW_CONFIG
        config.BOOST_TIMESTAMP = 0
        self.assertEqual(config.DIFF_DIVIDER, config.get_diff_divider(block_timestamp))
        config.BOOST_TIMESTAMP = block_timestamp + 1
        self.assertEqual(config.DIFF_DIVIDER, config.get_diff_divider(block_timestamp))
        config.BOOST_TIMESTAMP = block_timestamp - 1
        self.assertEqual(config.DIFF_DIVIDER * config.BOOST_MULTIPLIER_PER_STEP,
                         config.get_diff_divider(block_timestamp))
        config.BOOST_TIMESTAMP = block_timestamp - config.BOOST_STEP_INTERVAL * config.BOOST_STEPS + 1
        self.assertEqual(config.DIFF_DIVIDER * pow(config.BOOST_MULTIPLIER_PER_STEP, config.BOOST_STEPS),
                         config.get_diff_divider(block_timestamp))
        config.BOOST_TIMESTAMP = block_timestamp - config.BOOST_STEP_INTERVAL * config.BOOST_STEPS - 1
        self.assertEqual(config.DIFF_DIVIDER * pow(config.BOOST_MULTIPLIER_PER_STEP, config.BOOST_STEPS),
                         config.get_diff_divider(block_timestamp))

