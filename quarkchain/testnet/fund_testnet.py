import argparse
import aiohttp
import asyncio
import logging
import pickle
import random
import rlp
from collections import defaultdict
from jsonrpcclient.aiohttp_client import aiohttpClient
from typing import Dict, List

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Identity
from quarkchain.evm.transactions import Transaction as EvmTransaction


class Endpoint:
    def __init__(self, url):
        self.url = url
        asyncio.get_event_loop().run_until_complete(self.__create_session())

    async def __create_session(self):
        self.session = aiohttp.ClientSession()

    async def __send_request(self, *args):
        client = aiohttpClient(self.session, self.url)
        # manual retry since the library has hard-coded timeouts
        while True:
            try:
                response = await client.request(*args)
                break
            except Exception as e:
                print("{} !timeout! retrying {}".format(self.url, e))
                await asyncio.sleep(1 + random.randint(0, 5))
        return response

    async def send_transaction(self, tx):
        txHex = "0x" + rlp.encode(tx, EvmTransaction).hex()
        resp = await self.__send_request("send_raw_transaction", txHex)
        return resp

    async def get_transaction_receipt(self, txId):
        """txId should be '0x.....' """
        resp = await self.__send_request("get_transaction_receipt", txId)
        return resp

    async def get_nonce(self, account):
        addressHex = "0x" + account.serialize().hex()
        resp = await self.__send_request("get_transaction_count", addressHex)
        return int(resp, 16)

    async def get_shard_size(self):
        resp = await self.__send_request("network_info")
        return int(resp["shardSize"], 16)

    async def get_network_id(self):
        resp = await self.__send_request("network_info")
        return int(resp["networkId"], 16)


def create_transaction(address, key, nonce, to, networkId, amount) -> EvmTransaction:
    evmTx = EvmTransaction(
        nonce=nonce,
        gasprice=1,
        startgas=1000000,
        to=to.recipient,
        value=int(amount) * (10 ** 18),
        data=b"",
        fromFullShardId=address.fullShardId,
        toFullShardId=to.fullShardId,
        networkId=networkId,
    )
    evmTx.sign(key)
    return evmTx


async def fund_shard(endpoint, genesisId, to, networkId, shard, amount):
    address = Address.create_from_identity(genesisId, shard)
    nonce = await endpoint.get_nonce(address)
    tx = create_transaction(address, genesisId.get_key(), nonce, to, networkId, amount)
    txId = await endpoint.send_transaction(tx)
    cnt = 0
    while True:
        addr = "0x" + to.recipient.hex() + hex(to.fullShardId)[2:]
        print("shard={} tx={} to={} block=(pending)".format(shard, txId, addr))
        await asyncio.sleep(5)
        resp = await endpoint.get_transaction_receipt(txId)
        if resp:
            break
        cnt += 1
        if cnt == 10:
            cnt = 0
            print("retry tx={}".format(txId))
            await endpoint.send_transaction(tx)

    height = int(resp["blockHeight"], 16)
    status = int(resp["status"], 16)
    print(
        "shard={} tx={} block={} status={} amount={}".format(
            shard, txId, height, status, amount
        )
    )
    return txId, height


async def fund(endpoint, genesisId, addrByAmount):
    networkId = await endpoint.get_network_id()
    shardSize = await endpoint.get_shard_size()
    for amount in addrByAmount:
        addrs = addrByAmount.get(amount, [])
        print(
            "======\nstart for amount {} with {} address\n======".format(
                amount, len(addrs)
            )
        )
        # shard -> [addr]
        byShard = defaultdict(list)
        for addr in addrs:
            shard = int(addr[-8:], 16) & (shardSize - 1)
            byShard[shard].append(addr)

        while True:
            toFund = []
            for addrs in byShard.values():
                if addrs:
                    toFund.append(addrs.pop())

            if not toFund:
                break

            futures = []
            for addr in toFund:
                shard = int(addr[-8:], 16) & (shardSize - 1)
                try:
                    # sorry but this is user input
                    to = Address.create_from(addr[2:])
                except:
                    print("addr format invalid {}".format(addr))
                    continue
                await asyncio.sleep(0.1)  # slight delay for each call
                futures.append(
                    fund_shard(endpoint, genesisId, to, networkId, shard, amount)
                )

            results = await asyncio.gather(*futures)
            print("\n\n")
            for idx, result in enumerate(results):
                txId, height = result
                print('[{}, "{}"],  // {}'.format(idx, height, txId))


def read_addr(filepath) -> Dict[int, List[str]]:
    """ Every line is '<addr> <tqkc amount>' """
    with open(filepath) as f:
        tqkcMap = dict([line.split() for line in f.readlines()])
    byAmount = defaultdict(list)
    for addr, amount in tqkcMap.items():
        byAmount[int(amount)].append(addr)
    return byAmount


def main():
    """ Fund the game addresses  """
    parser = argparse.ArgumentParser()
    parser.add_argument("--jrpc_endpoint", default="localhost:38391", type=str)
    parser.add_argument("--log_jrpc", default=False, type=bool)
    parser.add_argument("--tqkc_file", required=True, type=str)
    args = parser.parse_args()

    if not args.log_jrpc:
        logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
        logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

    genesisId = Identity.create_from_key(DEFAULT_ENV.config.GENESIS_KEY)

    endpoint = Endpoint("http://" + args.jrpc_endpoint)
    addrByAmount = read_addr(args.tqkc_file)
    asyncio.get_event_loop().run_until_complete(fund(endpoint, genesisId, addrByAmount))


if __name__ == "__main__":
    main()
