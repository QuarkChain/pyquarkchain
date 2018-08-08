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

import copy
from quarkchain.experimental import diff
from quarkchain.experimental import proof_of_work
from quarkchain.experimental.event_driven_simulator import Scheduler
import random

NODE_SIZE = 18
NODE_FIX_MINER_SIZE = 6
NODE_POWERFUL_MINER_SIZE = 2
NODE_POWERFUL_MAJOR_MINER_SIZE = 0
NODE_DEFAULT_HASH_POWER = 100
NODE_POWERFUL_HASH_POWER = 10000
TOTAL_HASH_POWER = NODE_POWERFUL_HASH_POWER * NODE_POWERFUL_MINER_SIZE + \
    (NODE_SIZE - NODE_POWERFUL_MINER_SIZE) * NODE_DEFAULT_HASH_POWER

SHARD_SIZE = 8
MINOR_BLOCK_RATE_SEC = 10
MINOR_BLOCK_GENSIS_DIFF = 1 / TOTAL_HASH_POWER * \
    2 * SHARD_SIZE / MINOR_BLOCK_RATE_SEC
MINOR_BLOCK_REWARD = 100
MAJOR_BLOCK_RATE_SEC = 150
MAJOR_BLOCK_GENSIS_DIFF = 1 / TOTAL_HASH_POWER * 2 / MAJOR_BLOCK_RATE_SEC
MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS = 1

STATS_PRINTER_INTERVAL = 50


class MinorBlockHeader:

    def __init__(self,
                 hash,
                 nBranch,
                 height,
                 hashPrevMajorBlock=0,
                 hashPrevMinorBlock=0,
                 hashMerkleRoot=0,
                 nTime=0,
                 nBits=0,
                 nNonce=0):
        self.nBranch = nBranch
        self.hashPrevMajorBlock = hashPrevMajorBlock
        self.hashPrevMinorBlock = hashPrevMinorBlock
        self.hashMerkleRoot = hashMerkleRoot
        self.nTime = nTime
        self.nBits = nBits
        self.nNonce = nNonce
        self.hash = hash
        self.minedDiff = 1.0
        # TODO: Should be derive from nBits
        self.requiredDiff = 0.0
        self.blockReward = MINOR_BLOCK_REWARD
        self.height = height

    def calculateHash(self):
        return self.hash

    def meetDiff(self):
        return self.minedDiff <= self.requiredDiff

    def getMiningEco(self):
        return self.blockReward * self.requiredDiff


class MinorBlock:

    def __init__(self, header):
        self.header = header

    def get_shard_id(self):
        return self.header.nBranch

    def get_hash(self):
        return self.header.hash

    def getRequiredDiff(self):
        return self.header.requiredDiff

    def getCreateTimeSec(self):
        return self.header.nTime

    def getMiningEco(self):
        return self.header.getMiningEco()


class MajorBlockHeader:

    def __init__(self,
                 hash,
                 nShard,
                 height,
                 hashPrevBlock=0,
                 hashMerkleRoot=0,
                 hashCoinbase=0,
                 nTime=0,
                 nBits=0,
                 nNonce=0):
        self.hash = hash
        self.nShard = nShard
        self.hashPrevBlock = hashPrevBlock
        self.hashMerkleRoot = hashMerkleRoot
        self.hashCoinbase = hashCoinbase
        self.nTime = nTime
        self.nBits = nBits
        self.nNonce = nNonce
        self.diff = 1.0
        self.requiredDiff = 0.0
        self.blockReward = MINOR_BLOCK_REWARD   # TODO
        self.height = height

    def meetDiff(self):
        return self.minedDiff <= self.requiredDiff

    def getMiningEco(self):
        return self.blockReward * self.requiredDiff


class MajorBlock:

    def __init__(self, header, minorBlockMap={}):
        self.header = header
        self.minorBlockMap = minorBlockMap
        blockReward = 0
        for block in minorBlockMap.values():
            blockReward += block.header.blockReward
        self.header.blockReward = blockReward

    def get_hash(self):
        return self.header.hash

    def getRequiredDiff(self):
        return self.header.requiredDiff

    def getCreateTimeSec(self):
        return self.header.nTime

    def getMiningEco(self):
        return self.header.getMiningEco()

    def addMinorBlock(self, minorBlock):
        self.minorBlockMap[minorBlock.get_hash()] = minorBlock


# hash before 1024 are used for genesis block
hashCounter = 1024


def getGlobalHash():
    global hashCounter
    hashCounter = hashCounter + 1
    return hashCounter


def createGenesisMajorBlock(shardSize, hash):
    header = MajorBlockHeader(hash, shardSize, height=0)
    header.requiredDiff = MAJOR_BLOCK_GENSIS_DIFF
    header.nTime = 0
    return MajorBlock(header)


def createGenesisMinorBlock(shardSize, shardId, hash):
    header = MinorBlockHeader(hash=hash, nBranch=shardId, height=0)
    header.requiredDiff = MINOR_BLOCK_GENSIS_DIFF
    header.nTime = 0
    return MinorBlock(header)


class MinorBlockChain:

    def __init__(self, shardSize, shardId, hash):
        self.shardSize = shardSize
        self.shardId = shardId
        self.genesisBlock = createGenesisMinorBlock(shardSize, shardId, hash)
        # Map from block hash to block
        self.blockMap = {hash: self.genesisBlock}
        self.bestChain = [self.genesisBlock]
        self.diffCalc = diff.MADifficultyCalculator(
            maSamples=1440,
            bootstrapSamples=64,
            slideSize=1,
            targetIntervalSec=MINOR_BLOCK_RATE_SEC)

    def tryAppendBlock(self, minorBlock):
        if minorBlock.header.hashPrevMinorBlock != \
           self.bestChain[-1].header.hash:
            return False

        self.blockMap[minorBlock.header.hash] = minorBlock
        self.bestChain.append(minorBlock)
        return True

    def getBlockToMine(self):
        header = MinorBlockHeader(
            hash=getGlobalHash(),
            nBranch=self.shardId,
            height=self.bestChain[-1].header.height + 1,
            hashPrevMinorBlock=self.bestChain[-1].get_hash())
        header.requiredDiff = self.diffCalc.calculateDiff(self.bestChain)
        return MinorBlock(header)


class MajorBlockChain:

    def __init__(self, shardSize, hash):
        self.shardSize = shardSize
        self.genesisBlock = createGenesisMajorBlock(shardSize, hash)
        self.blockMap = {hash: self.genesisBlock}
        self.bestChain = [self.genesisBlock]
        self.diffCalc = diff.MADifficultyCalculator(
            maSamples=144,
            bootstrapSamples=64,
            slideSize=1,
            targetIntervalSec=MAJOR_BLOCK_RATE_SEC)
        self.pendingMinorBlockMap = {}

    def addMinorBlockToConfirm(self, minorBlock):
        self.pendingMinorBlockMap[minorBlock.header.hash] = minorBlock

    # Check if the major block can be appended to the best chain
    def tryAppendBlock(self, majorBlock):
        # TODO validate majorBlock
        if majorBlock.header.hashPrevBlock != self.bestChain[-1].header.hash:
            return False

        # May sure all hashs are unique
        hashSet = set()
        for blockHash in majorBlock.minorBlockMap:
            if blockHash in hashSet:
                return False
            hashSet.add(blockHash)

        # May sure local map contains all minor blocks (since we don't support
        # fork now)
        for blockHash in majorBlock.minorBlockMap:
            if blockHash not in self.pendingMinorBlockMap:
                return False

        for blockHash in majorBlock.minorBlockMap:
            self.pendingMinorBlockMap.pop(blockHash)

        self.blockMap[majorBlock.header.hash] = majorBlock
        self.bestChain.append(majorBlock)
        return True

    def getBlockToMine(self):
        header = MajorBlockHeader(
            hash=getGlobalHash(),
            nShard=self.shardSize,
            height=self.bestChain[-1].header.height + 1,
            hashPrevBlock=self.bestChain[-1].get_hash())
        header.requiredDiff = self.diffCalc.calculateDiff(self.bestChain)
        return MajorBlock(header, copy.copy(self.pendingMinorBlockMap))


class DynamicChainSelector:

    def select(majorChain, minorChainList):
        # Find the most economical chain
        bestBlock = majorChain.getBlockToMine()
        maxEco = bestBlock.getMiningEco()
        bestChainId = 0     # Major is 0
        maxDupCount = 1
        for chainId, chain in enumerate(minorChainList):
            block = chain.getBlockToMine()
            eco = block.getMiningEco()
            if eco > maxEco:
                maxEco = eco
                bestBlock = block
                bestChainId = chainId + 1
                maxDupCount = 1
            elif eco == maxEco:
                maxDupCount += 1
                # Random select if there are multiple max eco chains
                if random.random() < 1 / maxDupCount:
                    maxEco = eco
                    bestBlock = block
                    bestChainId = chainId + 1

        # Mine major block.  Examine whether the major block contains at least
        # MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS blocks in each shard.  If not, try
        # to mine the shard.
        if bestChainId == 0 and MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS != 0:
            blockCountInShard = {x: 0 for x in range(majorChain.shardSize)}
            for block in majorChain.pendingMinorBlockMap.values():
                blockCountInShard[block.get_shard_id()] += 1
            for i in range(majorChain.shardSize):
                if blockCountInShard[i] < MAJOR_BLOCK_INCLUDE_MINOR_BLOCKS:
                    block = minorChainList[i].getBlockToMine()
                    eco = block.getMiningEco()
                    return (i + 1, block, eco)

        return (bestChainId, bestBlock, maxEco)


class FixChainSelector:

    def __init__(self, minorChainId):
        self.minorChainId = minorChainId

    def select(self, majorChain, minorChainList):
        bestBlock = minorChainList[self.minorChainId].getBlockToMine()
        return (self.minorChainId + 1, bestBlock, bestBlock.getMiningEco())


class FixMajorChainSelector:

    def __init__(self):
        pass

    def select(self, majorChain, minorChainList):
        bestBlock = majorChain.getBlockToMine()
        maxEco = bestBlock.getMiningEco()
        bestChainId = 0     # Major is 0
        return (bestChainId, bestBlock, maxEco)


# A full node of the network
class Node:
    nodeId = 0

    @classmethod
    def getNextNodeId(cls):
        cls.nodeId = cls.nodeId + 1
        return cls.nodeId

    def __init__(
            self,
            scheduler,
            hashPower=NODE_DEFAULT_HASH_POWER,
            chainSelector=DynamicChainSelector):
        self.scheduler = scheduler
        self.nodeId = Node.getNextNodeId()
        self.peers = []
        self.majorChain = MajorBlockChain(SHARD_SIZE, 0)
        self.chainSelector = chainSelector
        self.mineTask = None

        # Create a list of minor blockchains
        minorChainList = []
        for i in range(SHARD_SIZE):
            minorChainList.append(MinorBlockChain(SHARD_SIZE, i, hash=i + 1))
        self.minorChainList = minorChainList
        print("node %d created" % self.nodeId)
        self.hashPower = hashPower
        self.pow = proof_of_work.PoW(self.hashPower)
        self.rewards = 0

    def mined(self, ts, data):
        bestBlock, bestChainId, mineTime = data
        bestBlock.header.nTime = ts
        if bestChainId == 0:
            if self.majorChain.tryAppendBlock(bestBlock):
                # print("Node %d mined major block height %d, used time %.2f" %
                #       (self.nodeId, bestBlock.header.height, mineTime))
                self.broadcastMajorBlock(bestBlock)
                self.rewards += bestBlock.header.blockReward
        else:
            if self.minorChainList[bestBlock.get_shard_id()].tryAppendBlock(bestBlock):
                # print("Node %d mined minor block height %d on minor chain %d, used time %.2f" %
                #       (self.nodeId, bestBlock.header.height, bestBlock.get_shard_id(), mineTime))
                self.majorChain.addMinorBlockToConfirm(bestBlock)
                self.broadcastMinorBlock(bestBlock)
                self.rewards += bestBlock.header.blockReward

        self.mineOneChain()

    def mineOneChain(self):
        bestChainId, bestBlock, maxEco = self.chainSelector.select(
            self.majorChain, self.minorChainList)

        mineTime = self.pow.mine(bestBlock.getRequiredDiff())
        # print("Node %d mining on chain %d with height %d with work %.2f and used time %.2f" %
        #       (self.nodeId, bestChainId, bestBlock.header.height, 1 / bestBlock.header.requiredDiff, mineTime))
        self.mineChainId = bestChainId
        self.mineEco = maxEco
        self.mineTask = self.scheduler.scheduleAfter(
            mineTime, self.mined, (bestBlock, bestChainId, mineTime))

    def cancelMiningAndReschedule(self):
        if self.mineTask is not None:
            self.mineTask.cancel()
        self.mineOneChain()

    def start(self):
        self.mineOneChain()

    def getPeers(self):
        return self.peers

    def rpcGetMajorBlock(self, majorBlock):
        assert(self.majorChain.tryAppendBlock(majorBlock))
        if self.mineChainId == 0:
            self.cancelMiningAndReschedule()

    def rpcGetMinorBlock(self, minorBlock):
        assert(self.minorChainList[
               minorBlock.get_shard_id()].tryAppendBlock(minorBlock))
        self.majorChain.addMinorBlockToConfirm(minorBlock)
        # TODO cancel mining on major if minor eco is smaller
        if self.mineChainId - 1 == minorBlock.get_shard_id() or self.mineChainId == 0:
            self.cancelMiningAndReschedule()

    def broadcastMajorBlock(self, majorBlock):
        for peer in self.peers:
            block = copy.deepcopy(majorBlock)
            peer.rpcGetMajorBlock(block)

    def broadcastMinorBlock(self, minorBlock):
        for peer in self.peers:
            block = copy.deepcopy(minorBlock)
            peer.rpcGetMinorBlock(block)

    def addPeer(self, peer):
        self.peers.append(peer)


class StatsPrinter:

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def printStats(self, ts, nodeList):
        usedTime = ts
        powerfulRewards = 0
        weakRewards = 0
        print("====================================")
        for i, node in enumerate(nodeList):
            if i < NODE_POWERFUL_MINER_SIZE:
                powerfulRewards += node.rewards
            else:
                weakRewards += node.rewards
            print("Node %d, rewards %d, mining %d" % (node.nodeId, node.rewards,
                                                      node.mineChainId if hasattr(node, "mineChainId") else -1))
        if weakRewards != 0:
            print("Powerful/weak rewards ratio: %.2f" %
                  (powerfulRewards / weakRewards))
        print("------------------------------------")
        print("Major chain height %d, reward %d, work %.2f, blocks interval %.2f" % (
            node.majorChain.bestChain[-1].header.height,
            node.majorChain.bestChain[-1].header.blockReward,
            1 / node.majorChain.getBlockToMine().header.requiredDiff,
            usedTime / node.majorChain.bestChain[-1].header.height
            if node.majorChain.bestChain[-1].header.height > 0 else 0))
        for cid, chain in enumerate(node.minorChainList):
            print("Minor chain %d, height %d, work %.2f, block interval %.2f" % (
                cid,
                chain.bestChain[-1].header.height,
                1 / chain.getBlockToMine().header.requiredDiff,
                usedTime / chain.bestChain[-1].header.height if chain.bestChain[-1].header.height > 0 else 0))
        print("====================================")
        self.scheduler.scheduleAfter(
            STATS_PRINTER_INTERVAL, self.printStats, nodeList)


def main():
    scheduler = Scheduler()
    nodeList = []
    for i in range(NODE_SIZE):
        if i < NODE_POWERFUL_MINER_SIZE:
            node = Node(scheduler, hashPower=NODE_POWERFUL_HASH_POWER)
        elif i < NODE_FIX_MINER_SIZE + NODE_POWERFUL_MINER_SIZE:
            node = Node(scheduler, chainSelector=FixChainSelector(i))
        else:
            node = Node(scheduler)
        for peer in nodeList:
            node.addPeer(peer)
            peer.addPeer(node)
        nodeList.append(node)
    # shardId = 0
    # for i in range(NODE_SIZE):
    #     if i < NODE_POWERFUL_MINER_SIZE:
    #         if i < NODE_POWERFUL_MAJOR_MINER_SIZE:
    #             node = Node(hashPower=NODE_POWERFUL_HASH_POWER, chainSelector=FixMajorChainSelector())
    #         else:
    #             node = Node(hashPower=NODE_POWERFUL_HASH_POWER)
    #     else:
    #         node = Node(hashPower=NODE_DEFAULT_HASH_POWER, chainSelector=FixChainSelector(shardId))
    #         shardId = (shardId + 1) % SHARD_SIZE

    #     for peer in nodeList:
    #         node.addPeer(peer)
    #         peer.addPeer(node)
    #     nodeList.append(node)

    for node in nodeList:
        node.start()
    statsPrinter = StatsPrinter(scheduler)
    scheduler.scheduleAfter(STATS_PRINTER_INTERVAL,
                            statsPrinter.printStats, nodeList)
    scheduler.loopUntilNoTask()


if __name__ == '__main__':
    main()
