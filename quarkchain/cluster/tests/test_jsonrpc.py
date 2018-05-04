import aiohttp
import asyncio
import unittest

from contextlib import contextmanager
from jsonrpcclient.aiohttp_client import aiohttpClient

from quarkchain.cluster.jsonrpc import JSONRPCServer
from quarkchain.cluster.tests.test_utils import create_transfer_transaction, ClusterContext
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Identity
from quarkchain.utils import call_async


@contextmanager
def JSONRPCServerContext(master):
    server = JSONRPCServer(DEFAULT_ENV, master)
    server.start()
    yield server
    server.shutdown()


def sendRequest(*args):
    async def __sendRequest(*args):
        async with aiohttp.ClientSession(loop=asyncio.get_event_loop()) as session:
            client = aiohttpClient(session, "http://localhost:" + str(DEFAULT_ENV.config.LOCAL_SERVER_PORT))
            response = await client.request(*args)
            return response

    return call_async(__sendRequest(*args))


class TestJSONRPC(unittest.TestCase):

    def testGetTransactionCount(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.getTransactionCount(acc1)), (branch, 0))
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            response = sendRequest("getTransactionCount", "0x" + acc1.serialize().hex())
            self.assertEqual(response["branch"], "0x00000002")
            self.assertEqual(response["count"], "0x1")

            response = sendRequest("getTransactionCount", "0x" + acc2.serialize().hex())
            self.assertEqual(response["branch"], "0x00000003")
            self.assertEqual(response["count"], "0x0")
