import argparse
import logging
import random
import time

import jsonrpcclient

from quarkchain.cluster.core import MinorBlock, RootBlock
from quarkchain.cluster.jsonrpc import quantity_encoder

from quarkchain.config import DEFAULT_ENV
from quarkchain.utils import check, set_logging_level, Logger


class Endpoint:

    def __init__(self, port):
        self.port = port

    def __sendRequest(self, *args):
        return jsonrpcclient.request("http://localhost:{}".format(self.port), *args)

    def setArtificialTxCount(self, count):
        ''' Keep trying until success.
        It might take a while for the cluster to recover state.
        '''
        while True:
            try:
                return self.__sendRequest("setArtificialTxConfig", count, 10, 0)
            except Exception:
                pass
            time.sleep(1)

    def getNextBlockToMine(self, coinbaseAddressHex, shardMaskValue):
        resp = self.__sendRequest("getNextBlockToMine", coinbaseAddressHex, quantity_encoder(shardMaskValue))
        isRoot = resp["isRootBlock"]
        blockBytes = bytes.fromhex(resp["blockData"])
        blockClass = RootBlock if isRoot else MinorBlock
        block = blockClass.deserialize(blockBytes)
        return isRoot, block

    def addBlock(self, block):
        branch = 0 if isinstance(block, RootBlock) else block.header.branch.value
        resp = self.__sendRequest("addBlock", quantity_encoder(branch), block.serialize().hex())
        return resp


class Miner:

    def __init__(self, endpoint, coinbaseAddressHex, shardMaskValue, artificialTxCount):
        self.endpoint = endpoint
        self.coinbaseAddressHex = coinbaseAddressHex
        self.shardMaskValue = shardMaskValue
        self.artificialTxCount = artificialTxCount
        self.block = None
        self.isRoot = False

    def __simulatePowDelay(self, startTime):
        if self.isRoot:
            expectedBlockTime = DEFAULT_ENV.config.ROOT_BLOCK_INTERVAL_SEC
        else:
            expectedBlockTime = DEFAULT_ENV.config.MINOR_BLOCK_INTERVAL_SEC
        elapsed = time.time() - startTime
        delay = max(0, expectedBlockTime - elapsed + random.uniform(-1, 1))
        time.sleep(delay)

    def run(self):
        self.endpoint.setArtificialTxCount(self.artificialTxCount)
        while True:
            isRoot, block = self.endpoint.getNextBlockToMine(self.coinbaseAddressHex, self.shardMaskValue)
            check(block is not None)

            if self.block is None or self.block != block:
                self.block = block
                self.isRoot = isRoot

            if self.isRoot:
                Logger.info("Mining root block {} with {} minor headers".format(
                    block.header.height, len(block.minorBlockHeaderList)))
            else:
                Logger.info("Mining minor block {}-{} with {} transactions".format(
                    block.header.branch.getShardId(), block.header.height, len(self.block.txList)))
            start = time.time()
            for i in range(1000000):
                self.block.header.nonce += 1
                metric = int.from_bytes(self.block.header.getHash(), byteorder="big") * self.block.header.difficulty
                if metric < 2 ** 256:
                    self.__simulatePowDelay(start)
                    try:
                        self.endpoint.addBlock(self.block)
                    except Exception as e:
                        Logger.info("Failed to add block")
                    Logger.info("Successfully added block with nonce {}".format(self.block.header.nonce))
                    self.block = None
                    break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--jrpc_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--miner_address", default=DEFAULT_ENV.config.GENESIS_ACCOUNT.serialize().hex(), type=str)
    parser.add_argument(
        "--shard_mask", default=0, type=int)
    parser.add_argument(
        "--tx_count", default=100, type=int)
    parser.add_argument(
        "--log_jrpc", default=False, type=bool)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    if not args.log_jrpc:
        logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
        logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

    endpoint = Endpoint(args.jrpc_port)
    miner = Miner(endpoint, args.miner_address, args.shard_mask, args.tx_count)
    miner.run()


if __name__ == '__main__':
    main()
