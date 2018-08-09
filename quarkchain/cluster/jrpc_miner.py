import argparse
import logging
import time

import jsonrpcclient
import numpy

from quarkchain.cluster.jsonrpc import address_encoder, data_encoder, quantity_encoder
from quarkchain.config import DEFAULT_ENV, NetworkId
from quarkchain.core import MinorBlock, RootBlock
from quarkchain.utils import set_logging_level, Logger

NUM_MINERS = 2


class Endpoint:

    def __init__(self, port):
        self.port = port

    def __send_request(self, *args, **kwargs):
        return jsonrpcclient.request("http://localhost:{}".format(self.port), *args, **kwargs)

    def set_artificial_tx_count(self, count):
        ''' Keep trying until success.
        It might take a while for the cluster to recover state.
        '''
        while True:
            try:
                return self.__send_request("setArtificialTxConfig", count, 10, 0)
            except Exception:
                pass
            time.sleep(1)

    def get_next_block_to_mine(self, coinbaseAddressHex, shardMaskValue):
        resp = self.__send_request(
            "get_next_block_to_mine", coinbaseAddressHex, quantity_encoder(shardMaskValue), preferRoot=True)
        if not resp:
            return None, None
        isRoot = resp["isRootBlock"]
        blockBytes = bytes.fromhex(resp["blockData"][2:])
        blockClass = RootBlock if isRoot else MinorBlock
        block = blockClass.deserialize(blockBytes)
        return isRoot, block

    def add_block(self, block):
        branch = 0 if isinstance(block, RootBlock) else block.header.branch.value
        resp = self.__send_request("add_block", quantity_encoder(branch), data_encoder(block.serialize()))
        return resp


class Miner:

    def __init__(self, endpoint, coinbaseAddressHex, shardMaskValue, artificialTxCount):
        self.endpoint = endpoint
        self.coinbaseAddressHex = coinbaseAddressHex
        self.shardMaskValue = shardMaskValue
        self.artificialTxCount = artificialTxCount
        self.block = None
        self.isRoot = False

    def __simulate_pow_delay(self, startTime):
        if self.isRoot:
            expectedBlockTime = DEFAULT_ENV.config.ROOT_BLOCK_INTERVAL_SEC
        else:
            expectedBlockTime = DEFAULT_ENV.config.MINOR_BLOCK_INTERVAL_SEC

        blockTime = numpy.random.exponential(expectedBlockTime * NUM_MINERS)
        elapsed = time.time() - startTime
        delay = max(0, blockTime - elapsed)
        time.sleep(delay)

    def __check_metric(self, metric):
        # Testnet does not check difficulty
        if DEFAULT_ENV.config.NETWORK_ID != NetworkId.MAINNET:
            return True
        return metric < 2 ** 256

    def __logStatus(self, success):
        shard = "R" if self.isRoot else self.block.header.branch.get_shard_id()
        count = len(self.block.minorBlockHeaderList) if self.isRoot else len(self.block.txList)
        status = "success" if success else "fail"
        elapsed = time.time() - self.block.header.createTime

        Logger.info("[{}] {} [{}] ({} {:.2f})".format(
            shard, self.block.header.height, count, status, elapsed))

    def run(self):
        self.endpoint.set_artificial_tx_count(self.artificialTxCount)
        while True:
            isRoot, block = self.endpoint.get_next_block_to_mine(self.coinbaseAddressHex, self.shardMaskValue)
            if not block:
                time.sleep(1)
                continue

            if self.block is None or self.block != block:
                self.block = block
                self.isRoot = isRoot
            for i in range(1000000):
                self.block.header.nonce += 1
                metric = int.from_bytes(self.block.header.get_hash(), byteorder="big") * self.block.header.difficulty
                if self.__check_metric(metric):
                    self.__simulate_pow_delay(block.header.createTime)
                    try:
                        self.endpoint.add_block(self.block)
                        success = True
                    except Exception as e:
                        Logger.logException()
                        success = False
                    self.__logStatus(success)
                    self.block = None
                    break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--jrpc_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--miner_address", default=address_encoder(DEFAULT_ENV.config.GENESIS_ACCOUNT.serialize()), type=str)
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
