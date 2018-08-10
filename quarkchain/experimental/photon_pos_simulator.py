
from collections import deque
from quarkchain.experimental.event_driven_simulator import Scheduler
from quarkchain.utils import check
import numpy.random
import argparse
import time

ROOT_BLOCK_INTERVAL_SEC = 90
MINOR_BLOCK_INTERVAL_SEC = 10
MINOR_BLOCK_PER_ROOT_BLOCK = ROOT_BLOCK_INTERVAL_SEC // MINOR_BLOCK_INTERVAL_SEC
MINOR_BLOCK_ROOT_HASH_PREVIOUS = 2  # Should be >= 2
MINOR_BLOCK_GENESIS_EPOCH = 1  # Should be >= 1 and <= MINOR_BLOCK_ROOT_HASH_PREVIOUS

ROOT_BLOCK_NUM = 10000  # Number of root blocks to simulate
ROOT_BLOCK_MINOR_BLOCK_LIMIT = MINOR_BLOCK_PER_ROOT_BLOCK


class MinorBlock:
    def __init__(self, root_block, root_index, block_height, mined_time):
        self.root_block = root_block
        self.root_index = root_index
        self.block_height = block_height
        self.mined_time = mined_time


class RootBlock:
    def __init__(self, block_height, minor_block_list, mining_time, mined_time):
        self.block_height = block_height
        self.minor_block_list = minor_block_list
        self.mining_time = mining_time
        self.mined_time = mined_time


def get_next_interval(expected_interval):
    return numpy.random.exponential(expected_interval)


class Simulator:
    def __init__(self, args):
        self.pending_minor_block_queue = deque()
        self.minor_block_list = []
        self.scheduler = Scheduler()
        self.args = args

        # Create genesis root blocks
        self.root_block_list = []
        for i in range(args.rprevious):
            self.root_block_list.append(
                RootBlock(
                    block_height=i, minor_block_list=[], mining_time=0, mined_time=0
                )
            )

        # Create genesis minor blocks
        self.minor_block_list = []
        for i in range(args.minor_genesis_epoch):
            for j in range(MINOR_BLOCK_PER_ROOT_BLOCK):
                m_block = MinorBlock(
                    root_block=self.root_block_list[i],
                    root_index=j,
                    block_height=i * MINOR_BLOCK_PER_ROOT_BLOCK + j,
                    mined_time=0,
                )
                self.minor_block_list.append(m_block)
                self.pending_minor_block_queue.append(m_block)

        self.root_block_to_mine = None
        self.minor_block_to_mine = None

    def get_minor_block_tip(self):
        return self.minor_block_list[-1]

    def get_root_block_tip(self):
        return self.root_block_list[-1]

    def get_genesis_root_block_number(self):
        return self.args.rprevious

    def get_genesis_minor_block_number(self):
        return self.args.minor_genesis_epoch * MINOR_BLOCK_PER_ROOT_BLOCK

    def get_next_minor_block_to_mine(self, ts):
        tip = self.get_minor_block_tip()

        # Produce block in this epoch if the epoch has space
        if tip.root_index < MINOR_BLOCK_PER_ROOT_BLOCK - 1:
            check(ts == tip.mined_time)
            return MinorBlock(
                root_block=tip.root_block,
                root_index=tip.root_index + 1,
                block_height=tip.block_height + 1,
                mined_time=ts + MINOR_BLOCK_INTERVAL_SEC,
            )

        # Produce block in next epoch if a root block is available
        root_height_to_confirm = tip.root_block.block_height + 1
        if root_height_to_confirm <= self.get_root_block_tip().block_height:
            root_block = self.root_block_list[root_height_to_confirm]
            check(ts == max(tip.mined_time, root_block.mined_time))
            return MinorBlock(
                root_block=root_block,
                root_index=0,
                block_height=tip.block_height + 1,
                mined_time=ts + MINOR_BLOCK_INTERVAL_SEC,
            )

        # Unable to find a root block to produce the minor block
        return None

    def mine_next_root_block(self, ts):
        check(self.root_block_to_mine is None)
        if (
            len(self.pending_minor_block_queue) == 0
            or self.pending_minor_block_queue[0].root_block.block_height
            + self.args.rprevious
            > self.get_root_block_tip().block_height + 1
        ):
            # The root block is not able to mine.  Will wait until a minor block is produced.
            # TODO: the root block may also mine an null minor block with reduced coinbase reward
            return

        self.root_block_to_mine = RootBlock(
            block_height=self.get_root_block_tip().block_height + 1,
            minor_block_list=[],  # to be fill once mined
            mining_time=ts,
            mined_time=None,
        )

        self.scheduler.schedule_after(
            get_next_interval(ROOT_BLOCK_INTERVAL_SEC),
            self.mine_root_block,
            self.root_block_to_mine,
        )

        if self.args.verbose >= 1:
            print(
                "%0.2f: root_block %d mining ..."
                % (ts, self.root_block_to_mine.block_height)
            )

    def mine_root_block(self, ts, root_block):
        check(root_block == self.root_block_to_mine)

        # Include minor blocks as much as possible
        confirmed_list = []
        while (
            len(self.pending_minor_block_queue) != 0
            and self.pending_minor_block_queue[0].root_block.block_height
            + self.args.rprevious
            <= root_block.block_height
        ):
            m_block = self.pending_minor_block_queue.popleft()
            confirmed_list.append(m_block)
            if len(confirmed_list) >= ROOT_BLOCK_MINOR_BLOCK_LIMIT:
                break

        check(len(confirmed_list) > 0)
        if self.args.verbose >= 1:
            print(
                "%0.2f: root_block %d mined with %d mblocks"
                % (ts, root_block.block_height, len(confirmed_list))
            )

        # Add root block to chain
        self.root_block_to_mine.minor_block_list = confirmed_list
        self.root_block_to_mine.mined_time = ts
        self.root_block_list.append(self.root_block_to_mine)
        self.root_block_to_mine = None

        if root_block.block_height >= self.args.rblocks:
            self.scheduler.stop()
            return

        # Mine minor block produced by the root block if no minor block is in progress.
        if self.minor_block_to_mine is None:
            self.mine_next_minor_block(ts)
            check(self.minor_block_to_mine is not None)
            check(self.minor_block_to_mine.root_block == self.get_root_block_tip())

        self.mine_next_root_block(ts)

    def mine_next_minor_block(self, ts):
        check(self.minor_block_to_mine is None)

        m_block = self.get_next_minor_block_to_mine(ts)
        if m_block is None:
            return

        self.minor_block_to_mine = m_block
        self.scheduler.schedule_after(
            m_block.mined_time - ts, self.mine_minor_block, self.minor_block_to_mine
        )

    def mine_minor_block(self, ts, minor_block):
        check(minor_block == self.minor_block_to_mine)

        if self.args.verbose >= 1:
            print("%0.2f: minor_block %d mined" % (ts, minor_block.block_height))

        # Add mined minor block to pending queue and chain
        self.pending_minor_block_queue.append(self.minor_block_to_mine)
        self.minor_block_list.append(self.minor_block_to_mine)
        self.minor_block_to_mine = None

        # Should restart root block mining, but actually it doesn't matter
        # because exponential distribution is memory-less
        if self.root_block_to_mine is None:
            self.mine_next_root_block(ts)

        self.mine_next_minor_block(ts)

    def run(self):
        self.mine_next_root_block(0)
        self.mine_next_minor_block(0)

        self.scheduler.loop_until_no_task()


def main():
    global ROOT_BLOCK_NUM
    global MINOR_BLOCK_ROOT_HASH_PREVIOUS
    global MINOR_BLOCK_GENESIS_EPOCH

    parser = argparse.ArgumentParser()
    parser.add_argument("--rblocks", default=ROOT_BLOCK_NUM, type=int)
    parser.add_argument("--rprevious", default=MINOR_BLOCK_ROOT_HASH_PREVIOUS, type=int)
    parser.add_argument("--verbose", default=0, type=int)
    parser.add_argument(
        "--minor_genesis_epoch", default=MINOR_BLOCK_GENESIS_EPOCH, type=int
    )
    parser.add_argument("--seed", default=int(time.time()), type=int)
    args = parser.parse_args()

    numpy.random.seed(args.seed)
    simulator = Simulator(args)
    simulator.run()

    m_block_freq = dict()
    for r_block in simulator.root_block_list[args.rprevious :]:
        m_block_freq[len(r_block.minor_block_list)] = (
            m_block_freq.get(len(r_block.minor_block_list), 0) + 1
        )
    for key in m_block_freq:
        print("%d: %d" % (key, m_block_freq[key]))

    idle_duration = 0
    for i in range(1, len(simulator.root_block_list)):
        r_block_prevous = simulator.root_block_list[i - 1]
        r_block = simulator.root_block_list[i]
        idle_duration += r_block.mining_time - r_block_prevous.mined_time
    print("Seed: %d" % (args.seed))
    print(
        "Duration: %0.2f, Root block mining idle duraiton: %0.2f, idle percentage: %0.2f%%"
        % (
            simulator.root_block_list[-1].mined_time,
            idle_duration,
            idle_duration / simulator.root_block_list[-1].mined_time * 100,
        )
    )
    print(
        "Minor block interval: %0.2f"
        % (
            simulator.minor_block_list[-1].mined_time
            / (
                simulator.minor_block_list[-1].block_height
                - simulator.get_genesis_minor_block_number()
            )
        )
    )


if __name__ == "__main__":
    main()
