import time

from quarkchain.cluster.genesis import create_genesis_blocks, create_genesis_evm_list
from quarkchain.config import NetworkId
from quarkchain.core import RootBlock, MinorBlockHeader
from quarkchain.core import (
    calculate_merkle_root,
    Serializable,
    PrependedSizeListSerializer,
)
from quarkchain.utils import Logger


class LastMinorBlockHeaderList(Serializable):
    FIELDS = [("header_list", PrependedSizeListSerializer(4, MinorBlockHeader))]

    def __init__(self, header_list):
        self.header_list = header_list


class RootDb:
    """ Storage for all validated root blocks and minor blocks

    On initialization it will try to recover the recent blocks (max_num_blocks_to_recover) of the best chain
    from local database.
    Block referenced by "tipHash" is skipped because it's consistency is not guaranteed within the cluster.
    Note that we only recover the blocks on the best chain than including the forking blocks because
    we don't save "tipHash"s for the forks and thus their consistency state is hard to reason about.
    They can always be downloaded again from peers if they ever became the best chain.

    """

    def __init__(self, db, max_num_blocks_to_recover):
        # TODO: evict old blocks from memory
        self.db = db
        self.max_num_blocks_to_recover = max_num_blocks_to_recover
        # TODO: May store locally to save memory space (e.g., with LRU cache)
        self.m_hash_set = set()
        self.r_header_pool = dict()
        self.tip_header = None

        self.__recover_from_db()

    def __recover_from_db(self):
        """ Recover the best chain from local database.
        """
        Logger.info("Recovering root chain from local database...")

        if b"tipHash" not in self.db:
            return None

        # The block referenced by tip_hash might not have been fully propagated within the cluster
        # when the cluster was down, but its parent is guaranteed to have been accepted by all shards.
        # Therefore we use its parent as the new tip.
        r_hash = self.db.get(b"tipHash")
        r_block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))
        while (
            r_block.header.height >= 1
            and len(self.r_header_pool) < self.max_num_blocks_to_recover
        ):
            r_hash = r_block.header.hash_prev_block
            r_block = RootBlock.deserialize(self.db.get(b"rblock_" + r_hash))

            if self.tip_header is None:
                self.tip_header = r_block.header

            self.r_header_pool[r_hash] = r_block.header
            for m_header in r_block.minor_block_header_list:
                self.m_hash_set.add(m_header.get_hash())

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
        if consistency_check and h not in self.r_header_pool:
            return None

        raw_block = self.db.get(b"rblock_" + h, None)
        if not raw_block:
            return None
        return RootBlock.deserialize(raw_block)

    def get_root_block_header_by_hash(self, h, consistency_check=True):
        header = self.r_header_pool.get(h, None)
        if not header and not consistency_check:
            block = self.get_root_block_by_hash(h, False)
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

    def get_root_block_by_height(self, height):
        key = b"ri_%d" % height
        if key not in self.db:
            return None
        block_hash = self.db.get(key)
        return self.get_root_block_by_hash(block_hash, False)

    # ------------------------- Minor block db operations --------------------------------
    def contain_minor_block_by_hash(self, h):
        return h in self.m_hash_set

    def put_minor_block_hash(self, m_hash):
        self.db.put(b"mheader_" + m_hash, b"")
        self.m_hash_set.add(m_hash)

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

    def __init__(self, env):
        self.env = env
        self.diff_calc = self.env.config.ROOT_DIFF_CALCULATOR
        self.raw_db = env.db
        self.db = RootDb(self.raw_db, env.config.MAX_ROOT_BLOCK_IN_MEMORY)

        persisted_tip = self.db.get_tip_header()
        if persisted_tip:
            self.tip = persisted_tip
            Logger.info(
                "Recovered root state with tip height {}".format(self.tip.height)
            )
        else:
            self.__create_genesis_blocks()
            Logger.info("Created genesis root block")

    def __create_genesis_blocks(self):
        evm_list = create_genesis_evm_list(env=self.env)
        genesis_root_block0, genesis_root_block1, g_minor_block_list0, g_minor_block_list1 = create_genesis_blocks(
            env=self.env, evm_list=evm_list
        )

        self.db.put_root_block(
            genesis_root_block0, [b.header for b in g_minor_block_list0]
        )
        self.db.put_root_block_index(genesis_root_block0)
        self.db.put_root_block(
            genesis_root_block1, [b.header for b in g_minor_block_list1]
        )
        self.db.put_root_block_index(genesis_root_block1)
        self.tip = genesis_root_block1.header
        for b in g_minor_block_list0:
            self.add_validated_minor_block_hash(b.header.get_hash())
        for b in g_minor_block_list1:
            self.add_validated_minor_block_hash(b.header.get_hash())

    def get_tip_block(self):
        return self.db.get_root_block_by_hash(self.tip.get_hash())

    def add_validated_minor_block_hash(self, h):
        self.db.put_minor_block_hash(h)

    def get_next_block_difficulty(self, create_time=None):
        if create_time is None:
            create_time = max(self.tip.create_time + 1, int(time.time()))
        return self.diff_calc.calculate_diff_with_parent(self.tip, create_time)

    def create_block_to_mine(self, m_header_list, address, create_time=None):
        if create_time is None:
            create_time = max(self.tip.create_time + 1, int(time.time()))
        difficulty = self.diff_calc.calculate_diff_with_parent(self.tip, create_time)
        block = self.tip.create_block_to_append(
            create_time=create_time, address=address, difficulty=difficulty
        )
        block.minor_block_header_list = m_header_list

        coinbase_amount = 0
        for header in m_header_list:
            coinbase_amount += header.coinbase_amount

        coinbase_amount = coinbase_amount // 2
        return block.finalize(quarkash=coinbase_amount, coinbase_address=address)

    def validate_block_header(self, block_header, block_hash=None):
        """ Validate the block header.
        """
        if block_header.height <= 1:
            raise ValueError("unexpected height")

        if not self.db.contain_root_block_by_hash(block_header.hash_prev_block):
            raise ValueError("previous hash block mismatch")
        prev_block_header = self.db.get_root_block_header_by_hash(
            block_header.hash_prev_block
        )

        if prev_block_header.height + 1 != block_header.height:
            raise ValueError("incorrect block height")

        if block_header.create_time <= prev_block_header.create_time:
            raise ValueError(
                "incorrect create time tip time {}, new block time {}".format(
                    block_header.create_time, prev_block_header.create_time
                )
            )

        if block_hash is None:
            block_hash = block_header.get_hash()

        # Check difficulty
        if not self.env.config.SKIP_ROOT_DIFFICULTY_CHECK:
            if self.env.config.NETWORK_ID == NetworkId.MAINNET:
                diff = self.diff_calc.calculate_diff_with_parent(
                    prev_block_header, block_header.create_time
                )
                if diff != block_header.difficulty:
                    raise ValueError("incorrect difficulty")
                metric = diff * int.from_bytes(block_hash, byteorder="big")
                if metric >= 2 ** 256:
                    raise ValueError("insufficient difficulty")
            elif (
                block_header.coinbase_address.recipient
                != self.env.config.TESTNET_MASTER_ACCOUNT.recipient
            ):
                raise ValueError("incorrect master to create the block")

        return block_hash

    def __is_same_chain(self, longer_block_header, shorter_block_header):
        if shorter_block_header.height > longer_block_header.height:
            return False

        header = longer_block_header
        for i in range(longer_block_header.height - shorter_block_header.height):
            header = self.db.get_root_block_header_by_hash(header.hash_prev_block)
        return header == shorter_block_header

    def validate_block(self, block, block_hash=None):
        if not self.db.contain_root_block_by_hash(block.header.hash_prev_block):
            raise ValueError("previous hash block mismatch")
        prev_last_minor_block_header_list = self.db.get_root_block_last_minor_block_header_list(
            block.header.hash_prev_block
        )

        block_hash = self.validate_block_header(block.header, block_hash)

        # Check the merkle tree
        merkle_hash = calculate_merkle_root(block.minor_block_header_list)
        if merkle_hash != block.header.hash_merkle_root:
            raise ValueError("incorrect merkle root")

        # Check whether all minor blocks are ordered, validated (and linked to previous block)
        shard_id = 0
        prev_header = prev_last_minor_block_header_list[0]
        last_minor_block_header_list = []
        block_count_in_shard = 0
        for idx, m_header in enumerate(block.minor_block_header_list):
            if m_header.branch.get_shard_id() != shard_id:
                if m_header.branch.get_shard_id() != shard_id + 1:
                    raise ValueError("shard id must be ordered")
                if block_count_in_shard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                    raise ValueError("fail to prove progress")
                if m_header.create_time > block.header.create_time:
                    raise ValueError(
                        "minor block create time is too large {}>{}".format(
                            m_header.create_time, block.header.create_time
                        )
                    )
                if not self.__is_same_chain(
                    self.db.get_root_block_header_by_hash(block.header.hash_prev_block),
                    self.db.get_root_block_header_by_hash(
                        prev_header.hash_prev_root_block
                    ),
                ):
                    raise ValueError(
                        "minor block's prev root block must be in the same chain"
                    )

                last_minor_block_header_list.append(
                    block.minor_block_header_list[idx - 1]
                )
                shard_id += 1
                block_count_in_shard = 0
                prev_header = prev_last_minor_block_header_list[shard_id]

            if not self.db.contain_minor_block_by_hash(m_header.get_hash()):
                raise ValueError(
                    "minor block is not validated. {}-{}".format(
                        m_header.branch.get_shard_id(), m_header.height
                    )
                )

            if m_header.hash_prev_minor_block != prev_header.get_hash():
                raise ValueError("minor block doesn't link to previous minor block")
            block_count_in_shard += 1
            prev_header = m_header
            # TODO: Add coinbase

        if (
            shard_id != block.header.shard_info.get_shard_size() - 1
            and self.env.config.PROOF_OF_PROGRESS_BLOCKS != 0
        ):
            raise ValueError("fail to prove progress")
        if block_count_in_shard < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
            raise ValueError("fail to prove progress")
        if m_header.create_time > block.header.create_time:
            raise ValueError(
                "minor block create time is too large {}>{}".format(
                    m_header.create_time, block.header.create_time
                )
            )
        if not self.__is_same_chain(
            self.db.get_root_block_header_by_hash(block.header.hash_prev_block),
            self.db.get_root_block_header_by_hash(m_header.hash_prev_root_block),
        ):
            raise ValueError("minor block's prev root block must be in the same chain")
        last_minor_block_header_list.append(m_header)

        return block_hash, last_minor_block_header_list

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
        block_hash, last_minor_block_header_list = self.validate_block(
            block, block_hash
        )

        self.db.put_root_block(
            block, last_minor_block_header_list, root_block_hash=block_hash
        )

        if self.tip.height < block.header.height:
            self.tip = block.header
            self.db.update_tip_hash(block_hash)
            self.__rewrite_block_index_to(block)
            return True
        return False

    # -------------------------------- Root block db related operations ------------------------------
    def get_root_block_by_hash(self, h):
        return self.db.get_root_block_by_hash(h)

    def contain_root_block_by_hash(self, h):
        return self.db.contain_root_block_by_hash(h)

    def get_root_block_header_by_hash(self, h):
        return self.db.get_root_block_header_by_hash(h)

    def get_root_block_by_height(self, height):
        return self.db.get_root_block_by_height(height)

    # --------------------------------- Minor block db related operations ----------------------------
    def is_minor_block_validated(self, h):
        return self.db.contain_minor_block_by_hash(h)
