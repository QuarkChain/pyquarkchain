import asyncio
import logging
import unittest
from contextlib import contextmanager

import aiohttp
from jsonrpcclient.aiohttp_client import aiohttpClient

from quarkchain.cluster.jsonrpc import JSONRPCServer, quantity_encoder
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
    create_contract_creation_transaction,
)
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Identity, Transaction
from quarkchain.core import MinorBlock, RootBlock
from quarkchain.evm import opcodes
from quarkchain.evm.messages import mk_contract_address
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


@contextmanager
def _j_s_o_n_r_p_c_server_context(master):
    server = JSONRPCServer.start_test_server(DEFAULT_ENV, master)
    yield server
    server.shutdown()


def send_request(*args):
    async def __send_request(*args):
        async with aiohttp.ClientSession(loop=asyncio.get_event_loop()) as session:
            client = aiohttpClient(session, "http://localhost:" + str(DEFAULT_ENV.config.LOCAL_SERVER_PORT))
            response = await client.request(*args)
            return response

    return call_async(__send_request(*args))


class TestJSONRPC(unittest.TestCase):

    def test_get_transaction_count(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.get_primary_account_data(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            response = send_request("get_transaction_count", "0x" + acc1.serialize().hex())
            self.assertEqual(response, "0x1")

            response = send_request("get_transaction_count", "0x" + acc2.serialize().hex())
            self.assertEqual(response, "0x0")

    def test_send_transaction(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
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
            evmTx.sign(id1.get_key())
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
            tx = Transaction(code=Code.create_evm_code(evmTx))
            response = send_request("send_transaction", request)

            self.assertEqual(response, "0x" + tx.get_hash().hex() + "00000000")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 1)
            self.assertEqual(slaves[0].shardStateMap[branch.value].txQueue.pop_transaction(), evmTx)

    def test_send_transaction_with_bad_signature(self):
        """ send_transaction validates signature """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
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
            self.assertIsNone(send_request("send_transaction", request))
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0)

    def test_send_transaction_missing_from_full_shard_id(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
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
                send_request("send_transaction", request)

    def test_get_next_block_to_mine_and_add_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_random_account(fullShardId=0)
        acc3 = Address.create_random_account(fullShardId=1)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            slaves = clusters[0].slaveList

            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[2 | 0],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc3,
                value=14,
                gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            # Expect to mine shard 0 since it has one tx
            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b10)

            self.assertTrue(send_request("add_block", "0x2", response["blockData"]))
            self.assertEqual(slaves[1].shardStateMap[3].get_balance(acc3.recipient), 0)

            # Expect to mine shard 1 due to proof-of-progress
            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block2 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block2.header.branch.value, 0b11)

            self.assertTrue(send_request("add_block", "0x3", response["blockData"]))

            # Expect to mine root
            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertTrue(response["isRootBlock"])
            block = RootBlock.deserialize(bytes.fromhex(response["blockData"][2:]))

            self.assertEqual(block.header.height, 2)
            self.assertEqual(len(block.minorBlockHeaderList), 2)
            self.assertEqual(block.minorBlockHeaderList[0], block1.header)
            self.assertEqual(block.minorBlockHeaderList[1], block2.header)

            send_request("add_block", "0x0", response["blockData"])
            self.assertEqual(slaves[1].shardStateMap[3].get_balance(acc3.recipient), 0)

            # Expect to mine shard 1 for the gas on xshard tx to acc3
            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x0")
            self.assertFalse(response["isRootBlock"])
            block3 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block3.header.branch.value, 0b11)

            self.assertTrue(send_request("add_block", "0x3", response["blockData"]))
            # Expect withdrawTo is included in acc3's balance
            resp = send_request("get_balance", "0x" + acc3.serialize().hex())
            self.assertEqual(resp["branch"], "0x3")
            self.assertEqual(resp["balance"], "0xe")

    def test_get_next_block_to_mine_with_shard_mask(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x2")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b10)

            response = send_request("get_next_block_to_mine", "0x" + acc1.serialize().hex(), "0x3")
            self.assertFalse(response["isRootBlock"])
            block1 = MinorBlock.deserialize(bytes.fromhex(response["blockData"][2:]))
            self.assertEqual(block1.header.branch.value, 0b11)

    def test_get_minor_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.get_primary_account_data(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            # By id
            resp = send_request("get_minor_block_by_id",
                               "0x" + block1.header.get_hash().hex() + "0" * 8, False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.get_hash().hex() + "0" * 8)
            resp = send_request("get_minor_block_by_id",
                               "0x" + block1.header.get_hash().hex() + "0" * 8, True)
            self.assertEqual(resp["transactions"][0]["hash"], "0x" + tx.get_hash().hex())

            resp = send_request("get_minor_block_by_id", "0x" + "ff" * 36, True)
            self.assertIsNone(resp)

            # By height
            resp = send_request("get_minor_block_by_height", "0x0", "0x2", False)
            self.assertEqual(resp["transactions"][0], "0x" + tx.get_hash().hex() + "0" * 8)
            resp = send_request("get_minor_block_by_height", "0x0", "0x2", True)
            self.assertEqual(resp["transactions"][0]["hash"], "0x" + tx.get_hash().hex())

            resp = send_request("get_minor_block_by_height", "0x1", "0x3", False)
            self.assertIsNone(resp)
            resp = send_request("get_minor_block_by_height", "0x0", "0x5", False)
            self.assertIsNone(resp)

    def test_get_transaction_by_id(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            self.assertEqual(call_async(master.get_primary_account_data(acc1)).transactionCount, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            resp = send_request(
                "get_transaction_by_id", "0x" + tx.get_hash().hex() + acc1.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["hash"], "0x" + tx.get_hash().hex())

    def test_call_success(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            response = send_request("call", {"to": "0x" + acc1.serialize().hex()})

            self.assertEqual(response, "0x")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0, "should not affect tx queue")

    def test_call_failure(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            # insufficient gas
            response = send_request("call", {"to": "0x" + acc1.serialize().hex(), "gas": "0x1"})

            self.assertIsNone(response, "failed tx should return None")
            self.assertEqual(len(slaves[0].shardStateMap[branch.value].txQueue), 0, "should not affect tx queue")

    def test_get_transaction_receipt_not_exist(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            resp = send_request("get_transaction_receipt", "0x" + bytes(36).hex())
            self.assertIsNone(resp)

    def test_get_transaction_receipt_on_transfer(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            tx = create_transfer_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toAddress=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            resp = send_request("get_transaction_receipt",
                               "0x" + tx.get_hash().hex() + acc1.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], "0x5208")
            self.assertIsNone(resp["contractAddress"])

    def test_get_transaction_receipt_on_x_shard_transfer(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)
        acc2 = Address.create_from_identity(id1, fullShardId=1)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            s1, s2 = slaves[0].shardStateMap[2], slaves[1].shardStateMap[3]
            txGen = lambda s, f, t: create_transfer_transaction(
                shardState=s,
                key=id1.get_key(),
                fromAddress=f,
                toAddress=t,
                gas=21000 if f == t else 30000,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(txGen(s1, acc1, acc2)))
            _, b1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(b1)))
            _, b2 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertTrue(call_async(slaves[1].add_block(b2)))
            _, rB = call_async(master.get_next_block_to_mine(address=acc1, preferRoot=True))

            call_async(master.add_root_block(rB))

            tx = txGen(s2, acc2, acc2)
            self.assertTrue(slaves[1].add_tx(tx))
            _, b3 = call_async(master.get_next_block_to_mine(address=acc2))
            self.assertTrue(call_async(slaves[1].add_block(b3)))

            # in-shard tx 21000 + receiving x-shard tx 9000
            self.assertEqual(s2.evmState.gas_used, 30000)
            self.assertEqual(s2.evmState.xshard_receive_gas_used, 9000)
            resp = send_request("get_transaction_receipt",
                               "0x" + tx.get_hash().hex() + acc2.fullShardId.to_bytes(4, "big").hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], hex(30000))
            self.assertEqual(resp["gasUsed"], hex(21000))
            self.assertIsNone(resp["contractAddress"])

    def test_get_transaction_receipt_on_contract_creation(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            toFullShardId = acc1.fullShardId + 2
            tx = create_contract_creation_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toFullShardId=toFullShardId
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            resp = send_request("get_transaction_receipt", "0x" + tx.get_hash().hex() + branch.serialize().hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["cumulativeGasUsed"], "0x213eb")

            contractAddress = mk_contract_address(acc1.recipient, toFullShardId, 0)
            self.assertEqual(resp["contractAddress"],
                             "0x" + contractAddress.hex() + toFullShardId.to_bytes(4, "big").hex())

    def test_get_transaction_receipt_on_contract_creation_failure(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, fullShardId=0)

        with ClusterContext(1, acc1) as clusters, _j_s_o_n_r_p_c_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slaveList

            branch = Branch.create(2, 0)
            toFullShardId = acc1.fullShardId + 1  # x-shard contract creation should fail
            tx = create_contract_creation_transaction(
                shardState=slaves[0].shardStateMap[branch.value],
                key=id1.get_key(),
                fromAddress=acc1,
                toFullShardId=toFullShardId
            )
            self.assertTrue(slaves[0].add_tx(tx))

            isRoot, block1 = call_async(master.get_next_block_to_mine(address=acc1))
            self.assertTrue(call_async(slaves[0].add_block(block1)))

            resp = send_request("get_transaction_receipt", "0x" + tx.get_hash().hex() + branch.serialize().hex())
            self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
            self.assertEqual(resp["status"], "0x0")
            self.assertEqual(resp["cumulativeGasUsed"], "0x13d6c")
            self.assertIsNone(resp["contractAddress"])
