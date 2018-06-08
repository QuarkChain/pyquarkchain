import aiohttp
import asyncio
import logging
import unittest

from contextlib import contextmanager
from jsonrpcclient.aiohttp_client import aiohttpClient

from quarkchain.cluster.core import MinorBlock, RootBlock
from quarkchain.cluster.jsonrpc import JSONRPCServer, quantity_encoder
from quarkchain.cluster.tests.test_utils import create_transfer_transaction, ClusterContext
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Identity, Transaction
from quarkchain.evm import opcodes
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async


# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


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
            self.assertEqual(call_async(master.getPrimaryAccountData(acc1)).transactionCount, 0)
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
            self.assertEqual(response["branch"], "0x2")
            self.assertEqual(response["count"], "0x1")

            response = sendRequest("getTransactionCount", "0x" + acc2.serialize().hex())
            self.assertEqual(response["branch"], "0x3")
            self.assertEqual(response["count"], "0x0")

    def testSendTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            evmTx = EvmTransaction(
                branchValue=branch.value,
                nonce=0,
                gasprice=6,
                startgas=7,
                to=acc1.recipient,
                value=15,
                data=b"",
                withdraw=10,
                withdrawSign=1,
                withdrawTo=bytes(acc2.serialize()),
                networkId=slaves[0].env.config.NETWORK_ID,
            )
            evmTx.sign(id1.getKey())
            request = dict(
                to="0x" + acc1.serialize().hex(),
                gasprice="0x6",
                startgas="0x7",
                value="0xf",  # 15
                v=quantity_encoder(evmTx.v),
                r=quantity_encoder(evmTx.r),
                s=quantity_encoder(evmTx.s),
                nonce="0x0",
                branch="0x2",
                withdraw="0xa",  # 10
                withdrawTo="0x" + acc2.serialize().hex(),
                networkId=hex(slaves[0].env.config.NETWORK_ID),
            )
            tx = Transaction(code=Code.createEvmCode(evmTx))
            response = sendRequest("sendTransaction", request)

            self.assertEqual(response, "0x" + tx.getHash().hex() + "02")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)
            self.assertEqual(slaves[0].shardStateMap[branch.value].txQueue.pop_transaction(), evmTx)

    def testSendTransactionWithBadSignature(self):
        ''' sendTransaction validates signature '''
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            request = dict(
                to="0x" + acc1.serialize().hex(),
                gasprice="0x6",
                startgas="0x7",
                value="0xf",
                v="0x1",
                r="0x2",
                s="0x3",
                nonce="0x0",
                branch="0x2",
                withdraw="0xa",
                withdrawTo="0x" + acc2.serialize().hex()
            )
            self.assertIsNone(sendRequest("sendTransaction", request))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0)

    def testSendTransactionWithBadWithdrawTo(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            evmTx = EvmTransaction(
                branchValue=branch.value,
                nonce=0,
                gasprice=6,
                startgas=7,
                to=acc1.recipient,
                value=15,
                data=b"",
                withdraw=10,
                withdrawSign=1,
                withdrawTo=b"ab",
            )
            evmTx.sign(id1.getKey(), DEFAULT_ENV.config.NETWORK_ID)
            request = dict(
                to="0x" + acc1.serialize().hex(),
                gasprice="0x6",
                startgas="0x7",
                value="0xf",
                v="0x1",
                r="0x2",
                s="0x3",
                nonce="0x0",
                branch="0x2",
                withdraw="0xa",
                withdrawTo="0xab",  # bad withdrawTo
            )

            self.assertIsNone(sendRequest("sendTransaction", request))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0)

    def testGetNextBlockToMineAndAddBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        acc3 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                fromId=id1,
                toAddress=acc2,
                amount=13,
                withdraw=14,
                withdrawTo=bytes(acc3.serialize()),
                startgas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].addTx(tx))

            # Expect to mine shard 0 since it has one tx
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b10)

            self.assertTrue(sendRequest("addBlock", "0x2", response["blockData"]))
            resp = sendRequest("getBalance", "0x" + acc2.serialize().hex())
            self.assertEqual(resp["branch"], "0x2")
            self.assertEqual(resp["balance"], "0xd")
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 due to proof-of-progress
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block2 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block2.header.branch.value, 0b11)

            self.assertTrue(sendRequest("addBlock", "0x3", response["blockData"]))

            # Expect to mine root
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertTrue(response["isRootBlock"])
            block = RootBlock.deserialize(bytes.fromhex(response["blockData"][2:]))

            self.assertEqual(block.header.height, 2)
            self.assertEqual(len(block.minorBlockHeaderList), 2)
            self.assertEqual(block.minorBlockHeaderList[0], block1.header)
            self.assertEqual(block.minorBlockHeaderList[1], block2.header)

            sendRequest("addBlock", "0x0", response["blockData"])
            self.assertEqual(slaves[1].shardStateMap[3].getBalance(acc3.recipient), 0)

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block3 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block3.header.branch.value, 0b11)

            self.assertTrue(sendRequest("addBlock", "0x3", response["blockData"]))
            # Expect withdrawTo is included in acc3's balance
            resp = sendRequest("getBalance", "0x" + acc3.serialize().hex())
            self.assertEqual(resp["branch"], "0x3")
            self.assertEqual(resp["balance"], "0xe")

    def testGetNextBlockToMineWithShardMask(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x2")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b10)

            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x3")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b11)

    def testGetMinorBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.getPrimaryAccountData(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            # By id
            resp = sendRequest("getMinorBlockById",
                               "0x" + block1.header.getHash().hex() + branch.serialize().hex(), False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.getHash().hex() + "02")
            resp = sendRequest("getMinorBlockById",
                               "0x" + block1.header.getHash().hex() + branch.serialize().hex(), True)
            self.assertEqual(resp["transactions"][0]["hash"], "0x" + tx.getHash().hex())

            resp = sendRequest("getMinorBlockById", "0x" + "ff" * 36, True)
            self.assertIsNone(resp)

            # By height
            resp = sendRequest("getMinorBlockByHeight", "0x0", "0x2", False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.getHash().hex() + "02")
            resp = sendRequest("getMinorBlockByHeight", "0x0", "0x2", True)
            self.assertEqual(resp["transactions"][0]["hash"], "0x" + tx.getHash().hex())

            resp = sendRequest("getMinorBlockByHeight", "0x1", "0x3", False)
            self.assertIsNone(resp)
            resp = sendRequest("getMinorBlockByHeight", "0x0", "0x5", False)
            self.assertIsNone(resp)

    def testGetTransactionById(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.getPrimaryAccountData(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                fromId=id1,
                toAddress=acc1,
                amount=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            resp = sendRequest("getTransactionById", "0x" + tx.getHash().hex() + branch.serialize().hex())
            self.assertEqual(resp["hash"], "0x" + tx.getHash().hex())
