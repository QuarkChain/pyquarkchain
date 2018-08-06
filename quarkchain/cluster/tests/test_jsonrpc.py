import aiohttp
import asyncio
import logging
import unittest

from contextlib import contextmanager

from jsonrpcclient.aiohttp_client import aiohttpClient
from quarkchain.core import MinorBlock, RootBlock
from quarkchain.cluster.jsonrpc import JSONRPCServer, quantity_encoder
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
    create_contract_creation_transaction,
)
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Identity, Transaction
from quarkchain.evm import opcodes
from quarkchain.evm.messages import mk_contract_address
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


@contextmanager
def JSONRPCServerContext(master):
    server = JSONRPCServer.startTestServer(DEFAULT_ENV, master)
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
                key=id1.getKey(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            response = sendRequest("getTransactionCount", "0x" + acc1.serialize().hex())
            self.assertEqual(response, "0x1")

            response = sendRequest("getTransactionCount", "0x" + acc2.serialize().hex())
            self.assertEqual(response, "0x0")

    def testSendTransaction(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            evmTx = EvmTransaction(
                nonce=0,
                gasprice=6,
                startgas=30000,
                to=acc2.recipient,
                value=15,
                data=b"",
                fromFullShardId=acc1.fullShardId,
                toFullShardId=acc2.fullShardId,
                networkId=slaves[0].env.config.NETWORK_ID,
            )
            evmTx.sign(id1.getKey())
            request = dict(
                to="0x" + acc2.recipient.hex(),
                gasPrice="0x6",
                gas=hex(30000),
                value="0xf",  # 15
                v=quantity_encoder(evmTx.v),
                r=quantity_encoder(evmTx.r),
                s=quantity_encoder(evmTx.s),
                nonce="0x0",
                fromFullShardId="0x00000000",
                toFullShardId="0x00000001",
                networkId=hex(slaves[0].env.config.NETWORK_ID),
            )
            tx = Transaction(code=Code.createEvmCode(evmTx))
            response = sendRequest("sendTransaction", request)

            self.assertEqual(response, "0x" + tx.getHash().hex() + "00000000")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)
            self.assertEqual(slaves[0].shardStateMap[branch.value].txQueue.pop_transaction(), evmTx)

    def testSendTransactionWithBadSignature(self):
        """ sendTransaction validates signature """
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            request = dict(
                to="0x" + acc2.recipient.hex(),
                gasPrice="0x6",
                gas=hex(30000),
                value="0xf",
                v="0x1",
                r="0x2",
                s="0x3",
                nonce="0x0",
                fromFullShardId="0x00000000",
                toFullShardId="0x00000001",
            )
            self.assertIsNone(sendRequest("sendTransaction", request))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0)

    def testSendTransactionMissingFromFullShardId(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            request = dict(
                to="0x" + acc1.recipient.hex(),
                gasPrice="0x6",
                gas=hex(30000),
                value="0xf",
                v="0x1",
                r="0x2",
                s="0x3",
                nonce="0x0",
            )

            with self.assertRaises(Exception):
                sendRequest("sendTransaction", request)

    def testGetNextBlockToMineAndAddBlock(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createRandomAccount(fullShardId=0)
        acc3 = Address.createRandomAccount(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                key=id1.getKey(),
                fromAddress=acc1,
                toAddress=acc3,
                value=14,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].addTx(tx))

            # Expect to mine shard 0 since it has one tx
            response = sendRequest("getNextBlockToMine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b10)

            self.assertTrue(sendRequest("addBlock", "0x2", response["blockData"]))
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
                key=id1.getKey(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            # By id
            resp = sendRequest("getMinorBlockById",
                               "0x" + block1.header.getHash().hex() + "0" * 8, False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.getHash().hex() + "0" * 8)
            resp = sendRequest("getMinorBlockById",
                               "0x" + block1.header.getHash().hex() + "0" * 8, True)
            self.assertEqual(resp["transactions"][0]["hash"], "0x" + tx.getHash().hex())

            resp = sendRequest("getMinorBlockById", "0x" + "ff" * 36, True)
            self.assertIsNone(resp)

            # By height
            resp = sendRequest("getMinorBlockByHeight", "0x0", "0x2", False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.getHash().hex() + "0" * 8)
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
                key=id1.getKey(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            resp = sendRequest(
                "getTransactionById", "0x" + tx.getHash().hex() + acc1.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["hash"], "0x" + tx.getHash().hex())

    def testCallSuccess(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            response = sendRequest("call", {"to": "0x" + acc1.serialize().hex()})

            self.assertEqual(response, "0x")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0, "should not affect tx queue")

    def testCallFailure(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            # insufficient gas
            response = sendRequest("call", {"to": "0x" + acc1.serialize().hex(), "gas": "0x1"})

            self.assertIsNone(response, "failed tx should return None")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0, "should not affect tx queue")

    def testGetTransactionReceiptNotExist(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            resp = sendRequest("getTransactionReceipt", "0x" + bytes(36).hex())
            self.assertIsNone(resp)

    def testGetTransactionReceiptOnTransfer(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.getKey(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            resp = sendRequest("getTransactionReceipt",
                               "0x" + tx.getHash().hex() + acc1.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.getHash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], "0x5208")
            self.assertIsNone(resp["contractAddress"])

    def testGetTransactionReceiptOnXShardTransfer(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)
        acc2 = Address.createFromIdentity(id1, fullShardId=1)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            s1, s2 = slaves[0].shardStateMap[2], slaves[1].shardStateMap[3]
            txGen = lambda s, f, t: create_transfer_transaction(
                shardState=s,
                key=id1.getKey(),
                fromAddress=f,
                toAddress=t,
                gas=21000 if f == t else 30000,
                value=12345,
            )
            self.assertTrue(slaves[0].addTx(txGen(s1, acc1, acc2)))
            _, b1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(b1)))
            _, b2 = call_async(master.getNextBlockToMine(address=acc2))
            self.assertTrue(call_async(slaves[1].addBlock(b2)))
            _, rB = call_async(master.getNextBlockToMine(address=acc1, preferRoot=True))

            call_async(master.addRootBlock(rB))

            tx = txGen(s2, acc2, acc2)
            self.assertTrue(slaves[1].addTx(tx))
            _, b3 = call_async(master.getNextBlockToMine(address=acc2))
            self.assertTrue(call_async(slaves[1].addBlock(b3)))

            # in-shard tx 21000 + receiving x-shard tx 9000
            self.assertEqual(s2.evmState.gas_used, 30000)
            self.assertEqual(s2.evmState.xshard_receive_gas_used, 9000)
            resp = sendRequest("getTransactionReceipt",
                               "0x" + tx.getHash().hex() + acc2.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.getHash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], hex(30000))
            self.assertEqual(resp["gasUsed"], hex(21000))
            self.assertIsNone(resp["contractAddress"])

    def testGetTransactionReceiptOnContractCreation(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            toFullShardId = acc1.fullShardId + 2
            tx = create_contract_creation_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.getKey(),
                fromAddress=acc1,
                toFullShardId=toFullShardId
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            resp = sendRequest("getTransactionReceipt", "0x" + tx.getHash().hex() + branch.serialize().hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.getHash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], "0x213eb")

            contractAddress = mk_contract_address(acc1.recipient, toFullShardId, 0)
            self.assertEqual(resp["contractAddress"],
                             "0x" + contractAddress.hex() + toFullShardId.to_bytes(4, "big").hex())

    def testGetTransactionReceiptOnContractCreationFailure(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, JSONRPCServerContext(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            toFullShardId = acc1.fullShardId + 1  # x-shard contract creation should fail
            tx = create_contract_creation_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.getKey(),
                fromAddress=acc1,
                toFullShardId=toFullShardId
            )
            self.assertTrue(slaves[0].addTx(tx))

            isRoot, block1 = call_async(master.getNextBlockToMine(address=acc1))
            self.assertTrue(call_async(slaves[0].addBlock(block1)))

            resp = sendRequest("getTransactionReceipt", "0x" + tx.getHash().hex() + branch.serialize().hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.getHash().hex())
            self.assertEqual(resp["status"], "0x0")
            self.assertEqual(resp["cumulativeGasUsed"], "0x13d6c")
            self.assertIsNone(resp["contractAddress"])
