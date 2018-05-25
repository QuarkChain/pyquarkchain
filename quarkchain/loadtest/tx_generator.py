import argparse
import aiohttp
import asyncio
import logging
import random
import time
from jsonrpcclient.aiohttp_client import aiohttpClient

from multiprocessing import Pool

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Branch, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.loadtest.accounts import LOADTEST_ACCOUNTS


ARGS = None


class Endpoint:

    def __init__(self, url):
        self.url = url
        asyncio.get_event_loop().run_until_complete(self.__createSession())

    async def __createSession(self):
        self.session = aiohttp.ClientSession()

    async def __sendRequest(self, *args):
        client = aiohttpClient(self.session, self.url)
        response = await client.request(*args)
        return response

    async def sendTransaction(self, tx):
        resp = await self.__sendRequest("sendRawTransaction", tx.serialize().hex())
        return resp

    async def getNonce(self, account, branch):
        addressHex = (account.recipient + branch.serialize()).hex()
        resp = await self.__sendRequest("getTransactionCount", addressHex)
        return int(resp["count"])


class Account:
    def __init__(self, recipient, key):
        self.recipient = recipient
        self.key = key


def create_transaction(account, nonce, branch: Branch):
    config = DEFAULT_ENV.config
    if random.randint(1, 100) <= ARGS.xshard_percent:
        # x-shard tx
        toShard = random.randint(0, config.SHARD_SIZE - 1)
        if toShard == branch.getShardId():
            toShard = (toShard + 1) % config.SHARD_SIZE
        withdrawTo = account.recipient + toShard.to_bytes(4, "big")
        evmTx = EvmTransaction(
            branchValue=branch.value,
            nonce=nonce,
            gasprice=1,
            startgas=500000,
            to=account.recipient,
            value=0,
            data=b'',
            withdrawSign=1,
            withdraw=random.randint(1, 100) * config.QUARKSH_TO_JIAOZI,
            withdrawTo=withdrawTo,
            networkId=config.NETWORK_ID)
    else:
        evmTx = EvmTransaction(
            branchValue=branch.value,
            nonce=nonce,
            gasprice=1,
            startgas=21000,
            to=account.recipient,
            value=random.randint(1, 100) * config.QUARKSH_TO_JIAOZI,
            data=b'',
            withdrawSign=1,
            withdraw=0,
            withdrawTo=b'',
            networkId=config.NETWORK_ID)
    evmTx.sign(account.key)
    return Transaction(code=Code.createEvmCode(evmTx))


async def run_account(account, branch, endpoint):
    nonce = await endpoint.getNonce(account, branch)
    tx = create_transaction(account, nonce, branch)
    await endpoint.sendTransaction(tx)
    # Not accurate but good enough for our use
    await asyncio.sleep(1.0 / ARGS.shard_tps)


def run_branch(branch):
    print("[{}] started!".format(branch.getShardId()))
    accounts = []
    for item in LOADTEST_ACCOUNTS:
        account = Account(bytes.fromhex(item["address"])[:20], bytes.fromhex(item["key"]))
        accounts.append(account)

    endpoint = Endpoint("http://{}:{}".format(ARGS.host, ARGS.port))
    start = time.time()
    count = 0
    while True:
        for account in accounts:
            try:
                asyncio.get_event_loop().run_until_complete(run_account(account, branch, endpoint))
            except Exception as e:
                print("Failed to issue new tx: {}".format(e))
                time.sleep(10)
                continue
            count += 1
            elapse = time.time() - start
            if elapse > 30:
                tps = count / elapse
                print("[{}] {:.2f} TPS".format(branch.getShardId(), tps))
                count = 0
                start = time.time()


def main():
    ''' Spawn multiple processes to generate the desired tps per shard '''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_processes", default=8, type=int)
    parser.add_argument(
        "--shard_tps", default=10, type=int)
    parser.add_argument(
        "--xshard_percent", default=30, type=int)
    parser.add_argument(
        "--host", default="localhost", type=str)
    parser.add_argument(
        "--port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--log_jrpc", default=False, type=bool)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    if not args.log_jrpc:
        logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
        logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)

    num_processes = args.num_processes
    global ARGS
    ARGS = args

    shardSize = DEFAULT_ENV.config.SHARD_SIZE
    branches = [Branch.create(shardSize, shard) for shard in range(shardSize)]

    with Pool(num_processes) as p:
        p.map(run_branch, branches)


if __name__ == "__main__":
    main()
