#!/usr/bin/python3

# This is a demo to illustrate if there is a network latency, reward ratio of two miners
#
# A common question is that if two miner mines a chain at the same time,
# what is the expected reward ratio between them?   If there is no network latency, the number is
# simple: the ratio equals to the ratio between their hash power.
#
# However, if there is network latency, two fork on the chain could happen, and the forks will
# compete until one fork is longer.  In such case, the simulation will
# tell us what the ratio should be.
#
# Some interesting results
# Parameters:
# - Hash power: (200, 100)
# - Diff: 0.01
#
# Ratios of different latencies:
# - Latency: 0.0 => 1.93
# - Latency: 0.1 => 2.35
# - Latency: 0.2 => 2.92
# - Latency: 0.3 => 3.30
# - Latency: 0.4 => 3.84
#
# Ratios of different miner1 hash power (latency 0.1)
# - Power : 200 => 2.35
# - Power : 300 => 4.54
# - Power : 400 => 6.81
# - Power : 500 => 10.30
# - Power : 600 => 15.78

import argparse
import event_driven_simulator as simulator
from quarkchain.experimental import diff
import proof_of_work


class Block:
    """ Immutable block
    """

    block_id = 0

    def __init__(self, height, owner):
        self.block_id = Block.get_next_block_id()
        self.height = height
        self.owner = owner

    @classmethod
    def get_next_block_id(cls):
        cls.block_id += 1
        return cls.block_id

    genesis_block = None

    @classmethod
    def get_genesis_block(cls):
        return Block(0, None)

    def __repr__(self):
        return str(self.height)


class Network:
    """ A fully-connected network
    """

    def __init__(self, scheduler, latency, peers=[]):
        self.scheduler = scheduler
        self.latency = latency
        self.peers = peers

    def add_node(self, peer):
        self.peers.append(peer)

    def broadcast_new_block(self, source, block_data):
        for peer in self.peers:
            if peer == source:
                continue

            self.scheduler.schedule_after(
                self.latency, peer.rpc_handle_receive_block, block_data
            )


class Miner:

    miner_id = 0

    @classmethod
    def get_next_miner_id(cls):
        cls.miner_id += 1
        return cls.miner_id

    def __init__(self, network, hash_power, scheduler, nblock, diff_calc):
        self.miner_id = Miner.get_next_miner_id()
        self.chain = [Block.get_genesis_block()]
        self.network = network
        self.hash_power = hash_power
        self.pow = proof_of_work.PoW(hash_power)
        self.scheduler = scheduler
        self.nblock = nblock
        self.diff_calc = diff_calc
        self.mine_task = None
        self.wasted_blocks = 0

    def rpc_handle_receive_block(self, ts, block_data):
        global args
        block, chain = block_data

        if args.verbose >= 1:
            print(
                "%.2f, Node %d: Receive block height %d"
                % (ts, self.miner_id, block.height)
            )

        # Local chain is longer, skip the RPC
        if self.chain[-1].height >= block.height:
            return

        # Peer chain is longer, copy the chain to local
        # genesis block should be the same
        waste_block = 0
        for height in range(len(self.chain) - 1, 0, -1):
            if self.chain[height] != chain[height]:
                self.chain[height] = chain[height]
                waste_block += 1
            else:
                break
        for height in range(self.chain[-1].height + 1, block.height + 1):
            self.chain.append(chain[height])
            waste_block += 1

        if args.verbose >= 1:
            print(
                "%.2f, Node %d: Fork resolve, wasted block %d"
                % (ts, self.miner_id, waste_block)
            )
        self.wasted_blocks += waste_block

        self.check_chain_integrity()

        if self.mine_task is not None:
            self.mine_task.cancel()
            self.mine_task = None
            self.mine_next()

    def get_block_to_mine(self):
        return Block(self.chain[-1].height + 1, self)

    def mined(self, ts, block):
        global args
        if args.verbose >= 1:
            print(
                "%.2f, Node %d: Mined block height %d"
                % (ts, self.miner_id, block.height)
            )
        self.chain.append(block)
        self.network.broadcast_new_block(self, (block, self.chain))
        self.mine_next()

    def mine_next(self):
        if len(self.chain) >= self.nblock:
            return

        block = self.get_block_to_mine()
        time_to_mine = self.pow.mine(self.diff_calc.calculate_diff(self.chain))
        self.mine_task = self.scheduler.schedule_after(time_to_mine, self.mined, block)

    def check_chain_integrity(self):
        for i in range(len(self.chain)):
            assert self.chain[i].height == i


args = None


def main():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("--miner1_hash_power", type=int, default=200)
    parser.add_argument("--miner2_hash_power", type=int, default=100)
    parser.add_argument("--diff", type=float, default=0.01)
    parser.add_argument("--nblocks", type=int, default=10000)
    parser.add_argument("--latency", type=float, default=0.0)
    parser.add_argument("--verbose", type=int, default=1)
    args = parser.parse_args()

    diff_calc = diff.FixedDifficultyCalculator(args.diff)

    scheduler = simulator.Scheduler()
    network = Network(scheduler, args.latency)

    m1 = Miner(network, args.miner1_hash_power, scheduler, args.nblocks, diff_calc)
    m2 = Miner(network, args.miner2_hash_power, scheduler, args.nblocks, diff_calc)

    network.add_node(m1)
    network.add_node(m2)

    m1.mine_next()
    m2.mine_next()
    scheduler.loop_until_no_task()

    r1 = 0
    r2 = 0
    for block in m1.chain:
        if block.owner == m1:
            r1 += 1
        elif block.owner == m2:
            r2 += 1

    agree_blocks = 0
    for i in range(args.nblocks):
        if m1.chain[i] == m2.chain[i]:
            agree_blocks += 1

    print(
        "Miner1 reward %d, Miner2 reward %d, ratio %.2f, agree %.2f%%"
        % (r1, r2, r1 / r2, agree_blocks / args.nblocks * 100)
    )
    print(
        "Miner1 stale blocks %d, Miner2 stale blocks %d"
        % (m1.wasted_blocks, m2.wasted_blocks)
    )


if __name__ == "__main__":
    main()
