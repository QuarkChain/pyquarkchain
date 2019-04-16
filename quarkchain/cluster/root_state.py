import asyncio
import json
import time
from fractions import Fraction
from typing import Optional, List, Dict

from quarkchain.cluster.guardian import Guardian
from quarkchain.cluster.miner import validate_seal
from quarkchain.core import (
    Address,
    MinorBlockHeader,
    PrependedSizeListSerializer,
    RootBlock,
    RootBlockHeader,
    Serializable,
    calculate_merkle_root,
    TokenBalanceMap,
)
from quarkchain.constants import ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.genesis import GenesisManager
from quarkchain.utils import Logger, check, time_ms
from quarkchain.evm.trie import BLANK_ROOT


class LastMinorBlockHeaderList(Serializable):
    FIELDS = [("header_list", PrependedSizeListSerializer(4, MinorBlockHeader))]

    def __init__(self, header_list):
        self.header_list = header_list


class RootDb:
    """ Storage for all validated root blocks and minor blocks

    On initialization it will try to recover the recent blocks (max_num_blocks_to_recover) of the best chain
    from local database with tip referenced by "tipHash".
    Note that we only recover the blocks on the best chain than including the forking blocks because
    we don't save "tipHash"s for the forks and thus their consistency state is hard to reason about.
    For example, a root block might not be received by all the shards when the cluster is down.
    Forks can always be downloaded again from peers if they ever became the best chain.
    """

    def __init__(self, db, quark_chain_config, count_minor_blocks=False):
        # TODO: evict old blocks from memory
        self.db = db
        self.quark_chain_config = quark_chain_config
        self.max_num_blocks_to_recover = (
            quark_chain_config.ROOT.max_root_blocks_in_memory
        )
        self.count_minor_blocks = count_minor_blocks
        # TODO: May store locally to save memory space (e.g., with LRU cache)
        self.m_hash_dict = dict()
        self.r_header_pool = dict()
        self.tip_header = None

        self.__recover_from_db()

    def __recover_from_db(self):
        """ Recover the best chain from local database.
        """
        Logger.info("Recovering root chain from local database...")

        if b"tipHash" not in self.db:
            return None

        r_hash = self.db.get(b"tipHash")
        r_block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))
        if r_block.header.height <= 0:
            return None
        # use the parent of the tipHash block as the new tip
        # since it's guaranteed to have been accepted by all the shards
        # while shards might not have seen the block of tipHash
        r_hash = r_block.header.hash_prev_block
        r_block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))
        self.tip_header = r_block.header  # type: RootBlockHeader

        while len(self.r_header_pool) < self.max_num_blocks_to_recover:
            self.r_header_pool[r_hash] = r_block.header
            for m_header in r_block.minor_block_header_list:
                mtokens = TokenBalanceMap.deserialize(
                    self.db.get(b"mheader_" + m_header.get_hash())
                ).balance_map
                self.m_hash_dict[m_header.get_hash()] = mtokens

            if r_block.header.height <= 0:
                break

            r_hash = r_block.header.hash_prev_block
            r_block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))

    def get_tip_header(self):
        return self.tip_header

    # ------------------------- Root block db operations --------------------------------
    def put_root_block(
        self, root_block, last_minor_block_header_list, root_block_hash=None
    ):
        if root_block_hash is None:
            root_block_hash = root_block.header.get_hash()

        last_list = LastMinorBlockHeaderList(header_list=last_minor_block_header_list)
        self.db.put(b"rblock_" + root_block_hash, root_block.serialize())
        self.db.put(b"lastlist_" + root_block_hash, last_list.serialize())
        self.r_header_pool[root_block_hash] = root_block.header

    def update_tip_hash(self, block_hash):
        self.db.put(b"tipHash", block_hash)

    def get_root_block_by_hash(self, h, consistency_check=True):
        """
        Consistency check being true means whatever in memory should already have enough
        information (no missing root block, minor block etc). Skipping the check by reading
        directly from database may have unwanted consequences.
        """
        if consistency_check and h not in self.r_header_pool:
            return None

        raw_block = self.db.get(b"rblock_" + h, None)
        if not raw_block:
            return None
        return RootBlock.deserialize(raw_block)

    def get_root_block_header_by_hash(self, h, consistency_check=True):
        header = self.r_header_pool.get(h)
        if not header and not consistency_check:
            block = self.get_root_block_by_hash(h, consistency_check=False)
            if block:
                header = block.header
        return header

    def get_root_block_last_minor_block_header_list(self, h):
        if h not in self.r_header_pool:
            return None
        return LastMinorBlockHeaderList.deserialize(
            self.db.get(b"lastlist_" + h)
        ).header_list

    def contain_root_block_by_hash(self, h):
        return h in self.r_header_pool

    def put_root_block_index(self, block):
        self.db.put(b"ri_%d" % block.header.height, block.header.get_hash())

        if not self.count_minor_blocks:
            return

        # Count minor blocks by miner address
        if block.header.height > 0:
            shard_recipient_cnt = self.get_block_count(block.header.height - 1)
        else:
            shard_recipient_cnt = dict()

        for header in block.minor_block_header_list:
            full_shard_id = header.branch.get_full_shard_id()
            recipient = header.coinbase_address.recipient.hex()
            old_count = shard_recipient_cnt.get(full_shard_id, dict()).get(recipient, 0)
            new_count = old_count + 1
            shard_recipient_cnt.setdefault(full_shard_id, dict())[recipient] = new_count

        for full_shard_id, r_c in shard_recipient_cnt.items():
            data = bytearray()
            for recipient, count in r_c.items():
                data.extend(bytes.fromhex(recipient))
                data.extend(count.to_bytes(4, "big"))
            check(len(data) % 24 == 0)
            self.db.put(b"count_%d_%d" % (full_shard_id, block.header.height), data)

    def get_block_count(self, root_height):
        """Returns a dict(full_shard_id, dict(miner_recipient, block_count))"""
        shard_recipient_cnt = dict()
        if not self.count_minor_blocks:
            return shard_recipient_cnt

        full_shard_ids = self.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
            root_height
        )
        for full_shard_id in full_shard_ids:
            data = self.db.get(b"count_%d_%d" % (full_shard_id, root_height), None)
            if data is None:
                continue
            check(len(data) % 24 == 0)
            for i in range(0, len(data), 24):
                recipient = data[i : i + 20].hex()
                count = int.from_bytes(data[i + 20 : i + 24], "big")
                shard_recipient_cnt.setdefault(full_shard_id, dict())[recipient] = count
        return shard_recipient_cnt

    def get_root_block_by_height(self, height):
        key = b"ri_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_root_block_by_hash(block_hash, consistency_check=False)

    # ------------------------- Minor block db operations --------------------------------
    def contain_minor_block_by_hash(self, h):
        if h in self.m_hash_dict:
            return True

        tokens = self.db.get(b"mheader_" + h)
        if tokens is None:
            return False

        self.m_hash_dict[h] = TokenBalanceMap.deserialize(tokens).balance_map
        return True

    def put_minor_block_coinbase(self, m_hash: bytes, coinbase_tokens: dict):
        tokens = TokenBalanceMap(coinbase_tokens)
        self.db.put(b"mheader_" + m_hash, tokens.serialize())
        self.m_hash_dict[m_hash] = coinbase_tokens

    def get_minor_block_coinbase_tokens(self, h: bytes):
        if not self.contain_minor_block_by_hash(h):
            raise KeyError()

        return self.m_hash_dict[h]

    # ------------------------- Common operations -----------------------------------------
    def put(self, key, value):
        self.db.put(key, value)

    def get(self, key, default=None):
        return self.db.get(key, default)

    def __getitem__(self, key):
        return self[key]


class RootState:
    """ State of root
    """

    def __init__(self, env, diff_calc=None):
        self.env = env
        if not diff_calc:
            cutoff = env.quark_chain_config.ROOT.DIFFICULTY_ADJUSTMENT_CUTOFF_TIME
            diff_factor = env.quark_chain_config.ROOT.DIFFICULTY_ADJUSTMENT_FACTOR
            min_diff = env.quark_chain_config.ROOT.GENESIS.DIFFICULTY
            check(cutoff > 0 and diff_factor > 0 and min_diff > 0)
            diff_calc = EthDifficultyCalculator(
                cutoff=cutoff, diff_factor=diff_factor, minimum_diff=min_diff
            )
        self.diff_calc = diff_calc
        self.raw_db = env.db
        self.db = RootDb(
            self.raw_db,
            env.quark_chain_config,
            count_minor_blocks=env.cluster_config.ENABLE_TRANSACTION_HISTORY,
        )

        persisted_tip = self.db.get_tip_header()
        if persisted_tip:
            self.tip = persisted_tip
            Logger.info(
                "Recovered root state with tip height {}".format(self.tip.height)
            )
        else:
            self.__create_genesis_block()
            Logger.info("Created genesis root block")

    def __create_genesis_block(self):
        genesis_manager = GenesisManager(self.env.quark_chain_config)
        genesis_block = genesis_manager.create_root_block()
        self.db.put_root_block(genesis_block, [])
        self.db.put_root_block_index(genesis_block)
        self.tip = genesis_block.header

    def get_tip_block(self):
        return self.db.get_root_block_by_hash(self.tip.get_hash())

    def add_validated_minor_block_hash(self, hash: bytes, coinbase_tokens: Dict):
        self.db.put_minor_block_coinbase(hash, coinbase_tokens)

    def get_next_block_difficulty(self, create_time=None):
        if create_time is None:
            create_time = max(self.tip.create_time + 1, int(time.time()))
        return self.diff_calc.calculate_diff_with_parent(self.tip, create_time)

    def _calculate_root_block_coinbase(
        self, m_hash_list: List[bytes], height: int
    ) -> Dict:
        """
        assumes all minor blocks in m_hash_list have been processed by slaves and thus available when looking up
        """
        assert all(
            [self.db.contain_minor_block_by_hash(m_hash) for m_hash in m_hash_list]
        )
        epoch = height // self.env.quark_chain_config.ROOT.EPOCH_INTERVAL
        numerator = (
            self.env.quark_chain_config.block_reward_decay_factor.numerator ** epoch
        )
        denominator = (
            self.env.quark_chain_config.block_reward_decay_factor.denominator ** epoch
        )
        coinbase_amount = (
            self.env.quark_chain_config.ROOT.COINBASE_AMOUNT * numerator // denominator
        )
        reward_tax_rate = self.env.quark_chain_config.reward_tax_rate
        # the ratio of minor block coinbase
        ratio = (1 - reward_tax_rate) / reward_tax_rate  # type: Fraction
        reward_tokens_map = TokenBalanceMap({})
        for m_hash in m_hash_list:
            reward_tokens_map.add(self.db.get_minor_block_coinbase_tokens(m_hash))
        reward_tokens = reward_tokens_map.balance_map
        # note the minor block fee is after tax
        reward_tokens = {
            k: v * ratio.denominator // ratio.numerator
            for k, v in reward_tokens.items()
        }
        genesis_token = self.env.quark_chain_config.genesis_token
        reward_tokens[genesis_token] = (
            reward_tokens.get(genesis_token, 0) + coinbase_amount
        )
        return reward_tokens

    def create_block_to_mine(self, m_header_list, address=None, create_time=None):
        if not address:
            address = Address.create_empty_account()
        if create_time is None:
            create_time = max(self.tip.create_time + 1, int(time.time()))
        tracking_data = {
            "inception": time_ms(),
            "cluster": self.env.cluster_config.MONITORING.CLUSTER_ID,
        }

        difficulty = self.diff_calc.calculate_diff_with_parent(self.tip, create_time)
        block = self.tip.create_block_to_append(
            create_time=create_time, address=address, difficulty=difficulty
        )
        block.minor_block_header_list = m_header_list

        coinbase_tokens = self._calculate_root_block_coinbase(
            [header.get_hash() for header in m_header_list], block.header.height
        )

        tracking_data["creation_ms"] = time_ms() - tracking_data["inception"]
        block.tracking_data = json.dumps(tracking_data).encode("utf-8")
        return block.finalize(coinbase_tokens=coinbase_tokens, coinbase_address=address)

    def validate_block_header(self, block_header: RootBlockHeader, block_hash=None):
        """ Validate the block header.
        """
        height = block_header.height
        if height < 1:
            raise ValueError("unexpected height")

        if not self.db.contain_root_block_by_hash(block_header.hash_prev_block):
            raise ValueError("previous hash block mismatch")
        prev_block_header = self.db.get_root_block_header_by_hash(
            block_header.hash_prev_block
        )

        if prev_block_header.height + 1 != height:
            raise ValueError("incorrect block height")

        if (
            block_header.create_time
            > time_ms() // 1000 + ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION
        ):
            raise ValueError("block too far into future")

        if block_header.create_time <= prev_block_header.create_time:
            raise ValueError(
                "incorrect create time tip time {}, new block time {}".format(
                    block_header.create_time, prev_block_header.create_time
                )
            )

        if (
            len(block_header.extra_data)
            > self.env.quark_chain_config.BLOCK_EXTRA_DATA_SIZE_LIMIT
        ):
            raise ValueError("extra_data in block is too large")

        header_hash = block_header.get_hash()
        if block_hash is None:
            block_hash = header_hash

        # Check difficulty, potentially adjusted by guardian mechanism
        adjusted_diff = None  # type: Optional[int]
        if not self.env.quark_chain_config.SKIP_ROOT_DIFFICULTY_CHECK:
            diff = self.diff_calc.calculate_diff_with_parent(
                prev_block_header, block_header.create_time
            )
            if diff != block_header.difficulty:
                raise ValueError("incorrect difficulty")
            # lower the difficulty for root block signed by guardian
            if block_header.verify_signature(
                self.env.quark_chain_config.guardian_public_key
            ):
                adjusted_diff = Guardian.adjust_difficulty(diff, block_header.height)

        if block_header.difficulty + prev_block_header.total_difficulty != block_header.total_difficulty:
            raise ValueError("incorrect total difficulty")

        # Check PoW if applicable
        consensus_type = self.env.quark_chain_config.ROOT.CONSENSUS_TYPE
        validate_seal(block_header, consensus_type, adjusted_diff=adjusted_diff)

        return block_hash

    def __is_same_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_root_block_header_by_hash(
                header.hash_prev_block, consistency_check=False
            )
        return header == shorter_block_header

    def validate_block(self, block, block_hash=None):
        """Raise on valiadtion errors """
        if block.header.version != 0:
            raise ValueError("incorrect root block version")

        if not self.db.contain_root_block_by_hash(block.header.hash_prev_block):
            raise ValueError("previous hash block mismatch")

        block_hash = self.validate_block_header(block.header, block_hash)

        if (
            len(block.tracking_data)
            > self.env.quark_chain_config.BLOCK_EXTRA_DATA_SIZE_LIMIT
        ):
            raise ValueError("tracking_data in block is too large")

        # Check the merkle tree
        merkle_hash = calculate_merkle_root(block.minor_block_header_list)
        if merkle_hash != block.header.hash_merkle_root:
            raise ValueError("incorrect merkle root")

        # Check the trie
        if block.header.hash_evm_state_root != BLANK_ROOT:
            raise ValueError("incorrect evm state root")

        # Check coinbase
        if not self.env.quark_chain_config.SKIP_ROOT_COINBASE_CHECK:
            expected_coinbase_amount = self._calculate_root_block_coinbase(
                [header.get_hash() for header in block.minor_block_header_list],
                block.header.height,
            )
            actual_coinbase_amount = block.header.coinbase_amount_map.balance_map
            if expected_coinbase_amount != actual_coinbase_amount:
                raise ValueError(
                    "Bad coinbase amount for root block {}. expect {} but got {}.".format(
                        block.header.get_hash().hex(),
                        expected_coinbase_amount,
                        actual_coinbase_amount,
                    )
                )

        # Check whether all minor blocks are ordered, validated (and linked to previous block)
        headers_map = dict()  # shard_id -> List[MinorBlockHeader]
        full_shard_id = (
            block.minor_block_header_list[0].branch.get_full_shard_id()
            if block.minor_block_header_list
            else None
        )
        for m_header in block.minor_block_header_list:
            if not self.db.contain_minor_block_by_hash(m_header.get_hash()):
                raise ValueError(
                    "minor block is not validated. {}-{}".format(
                        m_header.branch.get_full_shard_id(), m_header.height
                    )
                )
            if m_header.create_time > block.header.create_time:
                raise ValueError(
                    "minor block create time is larger than root block {} > {}".format(
                        m_header.create_time, block.header.create_time
                    )
                )
            if not self.__is_same_chain(
                self.db.get_root_block_header_by_hash(block.header.hash_prev_block),
                self.db.get_root_block_header_by_hash(
                    m_header.hash_prev_root_block, consistency_check=False
                ),
            ):
                raise ValueError(
                    "minor block's prev root block must be in the same chain"
                )

            if m_header.branch.get_full_shard_id() < full_shard_id:
                raise ValueError("shard id must be ordered")
            elif m_header.branch.get_full_shard_id() > full_shard_id:
                full_shard_id = m_header.branch.get_full_shard_id()

            headers_map.setdefault(m_header.branch.get_full_shard_id(), []).append(
                m_header
            )

        # check minor block headers are linked
        prev_last_minor_block_header_list = self.db.get_root_block_last_minor_block_header_list(
            block.header.hash_prev_block
        )
        prev_header_map = dict()  # shard_id -> MinorBlockHeader or None
        for header in prev_last_minor_block_header_list:
            prev_header_map[header.branch.get_full_shard_id()] = header

        full_shard_ids_to_check_proof_of_progress = self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
            block.header.height
        )
        for full_shard_id, headers in headers_map.items():
            check(len(headers) > 0)

            shard_config = self.env.quark_chain_config.shards[full_shard_id]
            if len(headers) > shard_config.max_blocks_per_shard_in_one_root_block:
                raise ValueError(
                    "too many minor blocks in the root block for shard {}".format(
                        full_shard_id
                    )
                )

            if full_shard_id not in full_shard_ids_to_check_proof_of_progress:
                raise ValueError(
                    "found minor block header in root block {} for uninitialized shard {}".format(
                        block_hash.hex(), full_shard_id
                    )
                )
            prev_header_in_last_root_block = prev_header_map.get(full_shard_id, None)
            if not prev_header_in_last_root_block:
                # no header in previous root block then it must start with genesis block
                if headers[0].height != 0:
                    raise ValueError(
                        "genesis block height is not 0 for shard {} block hash {}".format(
                            full_shard_id, headers[0].get_hash().hex()
                        )
                    )
            else:
                headers = [prev_header_in_last_root_block] + headers
            for i in range(len(headers) - 1):
                if headers[i + 1].hash_prev_minor_block != headers[i].get_hash():
                    raise ValueError(
                        "minor block {} does not link to previous block {}".format(
                            headers[i + 1].get_hash(), headers[i].get_hash()
                        )
                    )

            prev_header_map[full_shard_id] = headers[-1]

        return block_hash, prev_header_map.values()

    def __rewrite_block_index_to(self, block):
        """ Find the common ancestor in the current chain and rewrite index till block """
        while block.header.height >= 0:
            orig_block = self.db.get_root_block_by_height(block.header.height)
            if orig_block and orig_block.header == block.header:
                break
            self.db.put_root_block_index(block)
            block = self.db.get_root_block_by_hash(block.header.hash_prev_block)

    def add_block(self, block, block_hash=None):
        """ Add new block.
        return True if a longest block is added, False otherwise
        There are a couple of optimizations can be done here:
        - the root block could only contain minor block header hashes as long as the shards fully validate the headers
        - the header (or hashes) are un-ordered as long as they contains valid sub-chains from previous root block
        """
        start_ms = time_ms()
        block_hash, last_minor_block_header_list = self.validate_block(
            block, block_hash
        )

        self.db.put_root_block(
            block, last_minor_block_header_list, root_block_hash=block_hash
        )

        tracking_data_str = block.tracking_data.decode("utf-8")
        if tracking_data_str != "":
            tracking_data = json.loads(tracking_data_str)
            sample = {
                "time": time_ms() // 1000,
                "shard": "R",
                "network": self.env.cluster_config.MONITORING.NETWORK_NAME,
                "cluster": self.env.cluster_config.MONITORING.CLUSTER_ID,
                "hash": block.header.get_hash().hex(),
                "height": block.header.height,
                "original_cluster": tracking_data["cluster"],
                "inception": tracking_data["inception"],
                "creation_latency_ms": tracking_data["creation_ms"],
                "add_block_latency_ms": time_ms() - start_ms,
                "mined": tracking_data.get("mined", 0),
                "propagation_latency_ms": start_ms - tracking_data.get("mined", 0),
                "num_tx": len(block.minor_block_header_list),
            }
            asyncio.ensure_future(
                self.env.cluster_config.kafka_logger.log_kafka_sample_async(
                    self.env.cluster_config.MONITORING.PROPAGATION_TOPIC, sample
                )
            )

        if self.tip.total_difficulty < block.header.total_difficulty:
            self.tip = block.header
            self.db.update_tip_hash(block_hash)
            self.__rewrite_block_index_to(block)
            return True
        return False
