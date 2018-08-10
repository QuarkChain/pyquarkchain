#!/usr/bin/python3

# Changelog:
# 0.1: A fully-connected peer to peer network to simulate blockchains
# without latency

# TODO list
# - Network latency is 0 and blocks are propagated to all peers, i.e., there is no fork
# - Add malicious node to test robustness
# - Add random transactions to test actual throughput
# - Calculate sha using actual SHA3
# - Calculate merkle root of both major and minor block
# - Calculate using 256 bit and compact version of float

import asyncio
import copy
from quarkchain.experimental import diff
from quarkchain.experimental import proof_of_work
import random
import time

NODE_SIZE = 18
NODE_FIX_MINER_SIZE = 0
NODE_POWERFUL_MINER_SIZE = 2
NODE_POWERFUL_MAJOR_MINER_SIZE = 0
NODE_DEFAULT_HASH_POWER = 100
NODE_POWERFUL_HASH_POWER = 10000
TOTAL_HASH_POWER = (
    NODE_POWERFUL_HASH_POWER * NODE_POWERFUL_MINER_SIZE
    + (NODE_SIZE - NODE_POWERFUL_MINER_SIZE) * NODE_DEFAULT_HASH_POWER
)

SHARD_SIZE = 8
MINOR_BLOCK_RATE_SEC = 10
MINOR_BLOCK_GENSIS_DIFF = 1 / TOTAL_HASH_POWER * 2 * SHARD_SIZE / MINOR_BLOCK_RATE_SEC
MINOR_BLOCK_REWARD = 100
MAJOR_BLOCK_RATE_SEC = 150
MAJOR_BLOCK_GENSIS_DIFF = 1 / TOTAL_HASH_POWER * 2 / MAJOR_BLOCK_RATE_SEC
MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS = 1


class Transaction:
    pass


class MinorBlockHeader:
    def __init__(
        self,
        hash,
        n_branch,
        height,
        hash_prev_major_block=0,
        hash_prev_minor_block=0,
        hash_merkle_root=0,
        n_time=0,
        n_bits=0,
        n_nonce=0,
    ):
        self.n_branch = n_branch
        self.hash_prev_major_block = hash_prev_major_block
        self.hash_prev_minor_block = hash_prev_minor_block
        self.hash_merkle_root = hash_merkle_root
        self.n_time = n_time
        self.n_bits = n_bits
        self.n_nonce = n_nonce
        self.hash = hash
        self.mined_diff = 1.0
        # TODO: Should be derive from n_bits
        self.required_diff = 0.0
        self.block_reward = MINOR_BLOCK_REWARD
        self.height = height

    def calculate_hash(self):
        return self.hash

    def meet_diff(self):
        return self.mined_diff <= self.required_diff

    def get_mining_eco(self):
        return self.block_reward * self.required_diff


class MinorBlock:
    def __init__(self, header):
        self.header = header

    def get_shard_id(self):
        return self.header.n_branch

    def get_hash(self):
        return self.header.hash

    def get_required_diff(self):
        return self.header.required_diff

    def get_create_time_sec(self):
        return self.header.n_time

    def get_mining_eco(self):
        return self.header.get_mining_eco()


class MajorBlockHeader:
    def __init__(
        self,
        hash,
        n_shard,
        height,
        hash_prev_block=0,
        hash_merkle_root=0,
        hash_coinbase=0,
        n_time=0,
        n_bits=0,
        n_nonce=0,
    ):
        self.hash = hash
        self.n_shard = n_shard
        self.hash_prev_block = hash_prev_block
        self.hash_merkle_root = hash_merkle_root
        self.hash_coinbase = hash_coinbase
        self.n_time = n_time
        self.n_bits = n_bits
        self.n_nonce = n_nonce
        self.diff = 1.0
        self.required_diff = 0.0
        self.block_reward = MINOR_BLOCK_REWARD  # TODO
        self.height = height

    def meet_diff(self):
        return self.mined_diff <= self.required_diff

    def get_mining_eco(self):
        return self.block_reward * self.required_diff


class MajorBlock:
    def __init__(self, header, minor_block_map={}):
        self.header = header
        self.minor_block_map = minor_block_map
        block_reward = 0
        for block in minor_block_map.values():
            block_reward += block.header.block_reward
        self.header.block_reward = block_reward

    def get_hash(self):
        return self.header.hash

    def get_required_diff(self):
        return self.header.required_diff

    def get_create_time_sec(self):
        return self.header.n_time

    def get_mining_eco(self):
        return self.header.get_mining_eco()

    def add_minor_block(self, minor_block):
        self.minor_block_map[minor_block.get_hash()] = minor_block


# hash before 1024 are used for genesis block
hash_counter = 1024


def get_global_hash():
    global hash_counter
    hash_counter = hash_counter + 1
    return hash_counter


def create_genesis_major_block(shard_size, hash):
    header = MajorBlockHeader(hash, shard_size, height=0)
    header.required_diff = MAJOR_BLOCK_GENSIS_DIFF
    header.n_time = time.time()
    return MajorBlock(header)


def create_genesis_minor_block(shard_size, shard_id, hash):
    header = MinorBlockHeader(hash=hash, n_branch=shard_id, height=0)
    header.required_diff = MINOR_BLOCK_GENSIS_DIFF
    header.n_time = time.time()
    return MinorBlock(header)


class MinorBlockChain:
    def __init__(self, shard_size, shard_id, hash):
        self.shard_size = shard_size
        self.shard_id = shard_id
        self.genesis_block = create_genesis_minor_block(shard_size, shard_id, hash)
        # Map from block hash to block
        self.block_map = {hash: self.genesis_block}
        self.best_chain = [self.genesis_block]
        self.diff_calc = diff.MADifficultyCalculator(
            ma_samples=1440,
            bootstrap_samples=64,
            slide_size=1,
            target_interval_sec=MINOR_BLOCK_RATE_SEC,
        )

    def try_append_block(self, minor_block):
        if minor_block.header.hash_prev_minor_block != self.best_chain[-1].header.hash:
            return False

        self.block_map[minor_block.header.hash] = minor_block
        self.best_chain.append(minor_block)
        return True

    def get_block_to_mine(self):
        header = MinorBlockHeader(
            hash=get_global_hash(),
            n_branch=self.shard_id,
            height=self.best_chain[-1].header.height + 1,
            hash_prev_minor_block=self.best_chain[-1].get_hash(),
        )
        header.required_diff = self.diff_calc.calculate_diff(self.best_chain)
        return MinorBlock(header)


class MajorBlockChain:
    def __init__(self, shard_size, hash):
        self.shard_size = shard_size
        self.genesis_block = create_genesis_major_block(shard_size, hash)
        self.block_map = {hash: self.genesis_block}
        self.best_chain = [self.genesis_block]
        self.diff_calc = diff.MADifficultyCalculator(
            ma_samples=144,
            bootstrap_samples=64,
            slide_size=1,
            target_interval_sec=MAJOR_BLOCK_RATE_SEC,
        )
        self.pending_minor_block_map = {}

    def add_minor_block_to_confirm(self, minor_block):
        self.pending_minor_block_map[minor_block.header.hash] = minor_block

    # Check if the major block can be appended to the best chain
    def try_append_block(self, major_block):
        # TODO validate major_block
        if major_block.header.hash_prev_block != self.best_chain[-1].header.hash:
            return False

        # May sure all hashs are unique
        hash_set = set()
        for block_hash in major_block.minor_block_map:
            if block_hash in hash_set:
                return False
            hash_set.add(block_hash)

        # May sure local map contains all minor blocks (since we don't support
        # fork now)
        for block_hash in major_block.minor_block_map:
            if block_hash not in self.pending_minor_block_map:
                return False

        for block_hash in major_block.minor_block_map:
            self.pending_minor_block_map.pop(block_hash)

        self.block_map[major_block.header.hash] = major_block
        self.best_chain.append(major_block)
        return True

    def get_block_to_mine(self):
        header = MajorBlockHeader(
            hash=get_global_hash(),
            n_shard=self.shard_size,
            height=self.best_chain[-1].header.height + 1,
            hash_prev_block=self.best_chain[-1].get_hash(),
        )
        header.required_diff = self.diff_calc.calculate_diff(self.best_chain)
        return MajorBlock(header, copy.copy(self.pending_minor_block_map))


class DynamicChainSelector:
    def select(major_chain, minor_chain_list):
        # Find the most economical chain
        best_block = major_chain.get_block_to_mine()
        max_eco = best_block.get_mining_eco()
        best_chain_id = 0  # Major is 0
        max_dup_count = 1
        for chain_id, chain in enumerate(minor_chain_list):
            block = chain.get_block_to_mine()
            eco = block.get_mining_eco()
            if eco > max_eco:
                max_eco = eco
                best_block = block
                best_chain_id = chain_id + 1
                max_dup_count = 1
            elif eco == max_eco:
                max_dup_count += 1
                # Random select if there are multiple max eco chains
                if random.random() < 1 / max_dup_count:
                    max_eco = eco
                    best_block = block
                    best_chain_id = chain_id + 1

        # Mine major block.  Examine whether the major block contains at least
        # MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS blocks in each shard.  If not, try
        # to mine the shard.
        if best_chain_id == 0 and MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS != 0:
            block_count_in_shard = {x: 0 for x in range(major_chain.shard_size)}
            for block in major_chain.pending_minor_block_map.values():
                block_count_in_shard[block.get_shard_id()] += 1
            for i in range(major_chain.shard_size):
                if block_count_in_shard[i] < MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS:
                    block = minor_chain_list[i].get_block_to_mine()
                    eco = block.get_mining_eco()
                    return (i + 1, block, eco)

        return (best_chain_id, best_block, max_eco)


class FixChainSelector:
    def __init__(self, minor_chain_id):
        self.minor_chain_id = minor_chain_id

    def select(self, major_chain, minor_chain_list):
        best_block = minor_chain_list[self.minor_chain_id].get_block_to_mine()
        return (self.minor_chain_id + 1, best_block, best_block.get_mining_eco())


class FixMajorChainSelector:
    def __init__(self):
        pass

    def select(self, major_chain, minor_chain_list):
        best_block = major_chain.get_block_to_mine()
        max_eco = best_block.get_mining_eco()
        best_chain_id = 0  # Major is 0
        return (best_chain_id, best_block, max_eco)


# A full node of the network
class Node:
    node_id = 0

    @classmethod
    def get_next_node_id(cls):
        cls.node_id = cls.node_id + 1
        return cls.node_id

    def __init__(
        self, hash_power=NODE_DEFAULT_HASH_POWER, chain_selector=DynamicChainSelector
    ):
        self.node_id = Node.get_next_node_id()
        self.peers = []
        self.major_chain = MajorBlockChain(SHARD_SIZE, 0)
        self.chain_selector = chain_selector

        # Create a list of minor blockchains
        minor_chain_list = []
        for i in range(SHARD_SIZE):
            minor_chain_list.append(MinorBlockChain(SHARD_SIZE, i, hash=i + 1))
        self.minor_chain_list = minor_chain_list
        print("node %d created" % self.node_id)
        self.hash_power = hash_power
        self.pow = proof_of_work.PoW(self.hash_power)
        self.rewards = 0

    async def mine_one_chain(self):
        best_chain_id, best_block, max_eco = self.chain_selector.select(
            self.major_chain, self.minor_chain_list
        )

        mine_time = self.pow.mine(best_block.get_required_diff())
        # print("Node %d mining on chain %d with height %d with work %.2f and used time %.2f" %
        #       (self.node_id, best_chain_id, best_block.header.height, 1 / best_block.header.required_diff, mine_time))
        self.mine_future = asyncio.ensure_future(asyncio.sleep(mine_time))
        self.mine_chain_id = best_chain_id
        self.mine_eco = max_eco
        try:
            await self.mine_future
        except asyncio.CancelledError:
            return

        best_block.header.n_time = time.time()
        if best_chain_id == 0:
            if self.major_chain.try_append_block(best_block):
                print(
                    "Node %d mined major block height %d, used time %.2f"
                    % (self.node_id, best_block.header.height, mine_time)
                )
                self.broadcast_major_block(best_block)
                self.rewards += best_block.header.block_reward
        else:
            if self.minor_chain_list[best_block.get_shard_id()].try_append_block(
                best_block
            ):
                print(
                    "Node %d mined minor block height %d on minor chain %d, used time %.2f"
                    % (
                        self.node_id,
                        best_block.header.height,
                        best_block.get_shard_id(),
                        mine_time,
                    )
                )
                self.major_chain.add_minor_block_to_confirm(best_block)
                self.broadcast_minor_block(best_block)
                self.rewards += best_block.header.block_reward

    async def start(self):
        while True:
            await self.mine_one_chain()

    def get_peers(self):
        return self.peers

    def rpc_get_major_block(self, major_block):
        assert self.major_chain.try_append_block(major_block)
        if self.mine_chain_id == 0:
            self.mine_future.cancel()

    def rpc_get_minor_block(self, minor_block):
        assert self.minor_chain_list[minor_block.get_shard_id()].try_append_block(
            minor_block
        )
        self.major_chain.add_minor_block_to_confirm(minor_block)
        # TODO cancel mining on major if minor eco is smaller
        if (
            self.mine_chain_id - 1 == minor_block.get_shard_id()
            or self.mine_chain_id == 0
        ):
            self.mine_future.cancel()

    def broadcast_major_block(self, major_block):
        for peer in self.peers:
            block = copy.deepcopy(major_block)
            peer.rpc_get_major_block(block)

    def broadcast_minor_block(self, minor_block):
        for peer in self.peers:
            block = copy.deepcopy(minor_block)
            peer.rpc_get_minor_block(block)

    def add_peer(self, peer):
        self.peers.append(peer)


async def stats_printer(node_list):
    start_time = time.time()
    while True:
        used_time = (time.time() - start_time) + 0.0001
        powerful_rewards = 0
        weak_rewards = 0
        print("====================================")
        for i, node in enumerate(node_list):
            if i < NODE_POWERFUL_MINER_SIZE:
                powerful_rewards += node.rewards
            else:
                weak_rewards += node.rewards
            print(
                "Node %d, rewards %d, mining %d"
                % (
                    node.node_id,
                    node.rewards,
                    node.mine_chain_id if hasattr(node, "mineChainId") else -1,
                )
            )
        if weak_rewards != 0:
            print(
                "Powerful/weak rewards ratio: %.2f" % (powerful_rewards / weak_rewards)
            )
        print("------------------------------------")
        print(
            "Major chain height %d, reward %d, work %.2f, blocks interval %.2f"
            % (
                node.major_chain.best_chain[-1].header.height,
                node.major_chain.best_chain[-1].header.block_reward,
                1 / node.major_chain.get_block_to_mine().header.required_diff,
                used_time / node.major_chain.best_chain[-1].header.height
                if node.major_chain.best_chain[-1].header.height > 0
                else 0,
            )
        )
        for cid, chain in enumerate(node.minor_chain_list):
            print(
                "Minor chain %d, height %d, work %.2f, block interval %.2f"
                % (
                    cid,
                    chain.best_chain[-1].header.height,
                    1 / chain.get_block_to_mine().header.required_diff,
                    used_time / chain.best_chain[-1].header.height
                    if chain.best_chain[-1].header.height > 0
                    else 0,
                )
            )
        print("====================================")
        await asyncio.sleep(5)


def main():
    node_list = []
    for i in range(NODE_SIZE):
        if i < NODE_POWERFUL_MINER_SIZE:
            node = Node(hash_power=NODE_POWERFUL_HASH_POWER)
        elif i < NODE_FIX_MINER_SIZE + NODE_POWERFUL_MINER_SIZE:
            node = Node(chain_selector=FixChainSelector(i))
        else:
            node = Node()
        for peer in node_list:
            node.add_peer(peer)
            peer.add_peer(node)
        node_list.append(node)
    # shard_id = 0
    # for i in range(NODE_SIZE):
    #     if i < NODE_POWERFUL_MINER_SIZE:
    #         if i < NODE_POWERFUL_MAJOR_MINER_SIZE:
    #             node = Node(hash_power=NODE_POWERFUL_HASH_POWER, chain_selector=FixMajorChainSelector())
    #         else:
    #             node = Node(hash_power=NODE_POWERFUL_HASH_POWER)
    #     else:
    #         node = Node(hash_power=NODE_DEFAULT_HASH_POWER, chain_selector=FixChainSelector(shard_id))
    #         shard_id = (shard_id + 1) % SHARD_SIZE

    #     for peer in node_list:
    #         node.add_peer(peer)
    #         peer.add_peer(node)
    #     node_list.append(node)

    task_list = [node.start() for node in node_list]
    task_list.append(stats_printer(node_list))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*task_list))


if __name__ == "__main__":
    main()
