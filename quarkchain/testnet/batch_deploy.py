import argparse
import aiohttp
import asyncio
import logging
import rlp
from jsonrpcclient.aiohttp_client import aiohttpClient

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address,Identity
from quarkchain.evm.transactions import Transaction as EvmTransaction


class Endpoint:

    def __init__(self, url):
        self.url = url
        asyncio.get_event_loop().run_until_complete(self.__create_session())

    async def __create_session(self):
        self.session = aiohttp.ClientSession()

    async def __send_request(self, *args):
        client = aiohttpClient(self.session, self.url)
        response = await client.request(*args)
        return response

    async def send_transaction(self, tx):
        txHex = "0x" + rlp.encode(tx, EvmTransaction).hex()
        resp = await self.__send_request("send_raw_transaction", txHex)
        return resp

    async def get_contract_address(self, txId):
        """txId should be '0x.....' """
        resp = await self.__send_request("get_transaction_receipt", txId)
        if not resp:
            return None
        return resp["contractAddress"]

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


def create_transaction(address, key, nonce, data, networkId) -> EvmTransaction:
    evmTx = EvmTransaction(
        nonce=nonce,
        gasprice=1,
        startgas=1000000,
        to=b'',
        value=0,
        data=data,
        fromFullShardId=address.fullShardId,
        toFullShardId=address.fullShardId,
        networkId=networkId,
    )
    evmTx.sign(key)
    return evmTx


async def deploy_shard(endpoint, genesisId, data, networkId, shard):
    address = Address.create_from_identity(genesisId, shard)
    nonce = await endpoint.get_nonce(address)
    tx = create_transaction(address, genesisId.get_key(), nonce, data, networkId)
    txId = await endpoint.send_transaction(tx)
    while True:
        print("shard={} tx={} contract=(waiting for tx to be confirmed)".format(shard, txId))
        await asyncio.sleep(5)
        contractAddress = await endpoint.get_contract_address(txId)
        if contractAddress:
            break
    print("shard={} tx={} contract={}".format(shard, txId, contractAddress))
    return txId, contractAddress


async def deploy(endpoint, genesisId, data):
    networkId = await endpoint.get_network_id()
    shardSize = await endpoint.get_shard_size()
    futures = []
    for i in range(shardSize):
        futures.append(deploy_shard(endpoint, genesisId, data, networkId, i))

    results = await asyncio.gather(*futures)
    print("\n\n")
    for shard, result in enumerate(results):
        txId, contractAddress = result
        print("[{}, \"{}\"],  // {}".format(shard, contractAddress, txId))


def main():
    """ Deploy smart contract on all shards """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        default="608060405234801561001057600080fd5b5061014c806100206000396000f300608060405260043610610041576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063a2f09dfa14610114575b60008034141561005057610111565b60644233604051808381526020018273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166c01000000000000000000000000028152601401925050506040518091039020600190048115156100b857fe5b069050603281111515610110573373ffffffffffffffffffffffffffffffffffffffff166108fc346002029081150290604051600060405180830381858888f1935050505015801561010e573d6000803e3d6000fd5b505b5b50005b61011c61011e565b005b5600a165627a7a72305820dfb8255e8f0df762fae8168c8539831acd2852d55c2dc1827fd4348c7ff989d20029",
        type=str,
    )
    parser.add_argument(
        "--jrpc_endpoint", default="localhost:38391", type=str)
    parser.add_argument(
        "--log_jrpc", default=False, type=bool)
    args = parser.parse_args()

    if not args.log_jrpc:
        logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
        logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

    data = bytes.fromhex(args.data)
    genesisId = Identity.create_from_key(DEFAULT_ENV.config.GENESIS_KEY)

    endpoint = Endpoint("http://" + args.jrpc_endpoint)
    asyncio.get_event_loop().run_until_complete(deploy(endpoint, genesisId, data))


if __name__ == "__main__":
    main()
