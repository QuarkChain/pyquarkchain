import asyncio
import logging
import unittest
from contextlib import contextmanager

import aiohttp
from jsonrpcclient.aiohttp_client import aiohttpClient

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.jsonrpc import EMPTY_TX_ID, JSONRPCServer, quantity_encoder
from quarkchain.cluster.miner import DoubleSHA256, MiningWork
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
    create_contract_creation_transaction,
    create_contract_creation_with_event_transaction,
    create_contract_with_storage_transaction,
)
from quarkchain.core import (
    Address,
    Identity,
    SerializedEvmTransaction,
    TypedTransaction,
)
from quarkchain.env import DEFAULT_ENV
from quarkchain.evm.messages import mk_contract_address
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import call_async, sha3_256, token_id_encode

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


@contextmanager
def jrpc_server_context(master):
    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig()
    env.cluster_config.JSON_RPC_PORT = 38391
    # to pass the circleCi
    env.cluster_config.JSON_RPC_HOST = "127.0.0.1"
    server = JSONRPCServer.start_test_server(env, master)
    try:
        yield server
    finally:
        server.shutdown()


def send_request(*args):
    async def __send_request(*args):
        async with aiohttp.ClientSession(loop=asyncio.get_event_loop()) as session:
            client = aiohttpClient(session, "http://localhost:38391")
            response = await client.request(*args)
            return response

    return call_async(__send_request(*args))


class TestJSONRPC(unittest.TestCase):
    def test_getTransactionCount(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )
            for i in range(3):
                tx = create_transfer_transaction(
                    shard_state=clusters[0].get_shard_state(2 | 0),
                    key=id1.get_key(),
                    from_address=acc1,
                    to_address=acc1,
                    value=12345,
                )
                self.assertTrue(slaves[0].add_tx(tx))

                block = call_async(
                    master.get_next_block_to_mine(address=acc1, branch_value=0b10)
                )
                self.assertEqual(i + 1, block.header.height)
                self.assertTrue(
                    call_async(clusters[0].get_shard(2 | 0).add_block(block))
                )

            response = send_request(
                "getTransactionCount", ["0x" + acc2.serialize().hex()]
            )
            self.assertEqual(response, "0x0")

            response = send_request(
                "getTransactionCount", ["0x" + acc1.serialize().hex()]
            )
            self.assertEqual(response, "0x3")
            response = send_request(
                "getTransactionCount", ["0x" + acc1.serialize().hex(), "latest"]
            )
            self.assertEqual(response, "0x3")

            for i in range(3):
                response = send_request(
                    "getTransactionCount", ["0x" + acc1.serialize().hex(), hex(i + 1)]
                )
                self.assertEqual(response, hex(i + 1))

    def test_sendTransaction(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            slaves = clusters[0].slave_list
            master = clusters[0].master

            block = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=None)
            )
            call_async(master.add_root_block(block))

            evm_tx = EvmTransaction(
                nonce=0,
                gasprice=6,
                startgas=30000,
                to=acc2.recipient,
                value=15,
                data=b"",
                from_full_shard_key=acc1.full_shard_key,
                to_full_shard_key=acc2.full_shard_key,
                network_id=slaves[0].env.quark_chain_config.NETWORK_ID,
                gas_token_id=master.env.quark_chain_config.genesis_token,
                transfer_token_id=master.env.quark_chain_config.genesis_token,
            )
            evm_tx.sign(id1.get_key())
            request = dict(
                to="0x" + acc2.recipient.hex(),
                gasPrice="0x6",
                gas=hex(30000),
                value="0xf",  # 15
                v=quantity_encoder(evm_tx.v),
                r=quantity_encoder(evm_tx.r),
                s=quantity_encoder(evm_tx.s),
                nonce="0x0",
                fromFullShardKey="0x00000000",
                toFullShardKey="0x00000001",
                network_id=hex(slaves[0].env.quark_chain_config.NETWORK_ID),
            )
            tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
            response = send_request("sendTransaction", [request])

            self.assertEqual(response, "0x" + tx.get_hash().hex() + "00000000")
            state = clusters[0].get_shard_state(2 | 0)
            self.assertEqual(len(state.tx_queue), 1)
            self.assertEqual(
                state.tx_queue.pop_transaction(state.get_transaction_count), evm_tx
            )

    def test_sendTransaction_with_bad_signature(self):
        """ sendTransaction validates signature """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master

            block = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=None)
            )
            call_async(master.add_root_block(block))

            request = dict(
                to="0x" + acc2.recipient.hex(),
                gasPrice="0x6",
                gas=hex(30000),
                value="0xf",
                v="0x1",
                r="0x2",
                s="0x3",
                nonce="0x0",
                fromFullShardKey="0x00000000",
                toFullShardKey="0x00000001",
            )
            self.assertEqual(send_request("sendTransaction", [request]), EMPTY_TX_ID)
            self.assertEqual(len(clusters[0].get_shard_state(2 | 0).tx_queue), 0)

    def test_sendTransaction_missing_from_full_shard_key(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

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
                send_request("sendTransaction", [request])

    def test_getMinorBlock(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )
            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            # By id
            for need_extra_info in [True, False]:
                resp = send_request(
                    "getMinorBlockById",
                    [
                        "0x" + block1.header.get_hash().hex() + "0" * 8,
                        False,
                        need_extra_info,
                    ],
                )
                self.assertEqual(
                    resp["transactions"][0], "0x" + tx.get_hash().hex() + "00000002"
                )

            resp = send_request(
                "getMinorBlockById",
                ["0x" + block1.header.get_hash().hex() + "0" * 8, True],
            )
            self.assertEqual(
                resp["transactions"][0]["hash"], "0x" + tx.get_hash().hex()
            )

            resp = send_request("getMinorBlockById", ["0x" + "ff" * 36, True])
            self.assertIsNone(resp)

            # By height
            for need_extra_info in [True, False]:
                resp = send_request(
                    "getMinorBlockByHeight", ["0x0", "0x1", False, need_extra_info]
                )
                self.assertEqual(
                    resp["transactions"][0], "0x" + tx.get_hash().hex() + "00000002"
                )

            resp = send_request("getMinorBlockByHeight", ["0x0", "0x1", True])
            self.assertEqual(
                resp["transactions"][0]["hash"], "0x" + tx.get_hash().hex()
            )

            resp = send_request("getMinorBlockByHeight", ["0x1", "0x2", False])
            self.assertIsNone(resp)
            resp = send_request("getMinorBlockByHeight", ["0x0", "0x4", False])
            self.assertIsNone(resp)

    def test_getRootblockConfirmationIdAndCount(self):
        # TODO test root chain forks
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            tx_id = (
                "0x"
                + tx.get_hash().hex()
                + acc1.full_shard_key.to_bytes(4, "big").hex()
            )
            resp = send_request("getTransactionById", [tx_id])
            self.assertEqual(resp["hash"], "0x" + tx.get_hash().hex())
            self.assertEqual(
                resp["blockId"],
                "0x"
                + block1.header.get_hash().hex()
                + block1.header.branch.get_full_shard_id()
                .to_bytes(4, byteorder="big")
                .hex(),
            )
            minor_hash = resp["blockId"]

            # zero root block confirmation
            resp_hash = send_request(
                "getRootHashConfirmingMinorBlockById", [minor_hash]
            )
            self.assertIsNone(
                resp_hash, "should return None for unconfirmed minor blocks"
            )
            resp_count = send_request(
                "getTransactionConfirmedByNumberRootBlocks", [tx_id]
            )
            self.assertEqual(resp_count, "0x0")

            # 1 root block confirmation
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))
            resp_hash = send_request(
                "getRootHashConfirmingMinorBlockById", [minor_hash]
            )
            self.assertIsNotNone(resp_hash, "confirmed by root block")
            self.assertEqual(resp_hash, "0x" + block.header.get_hash().hex())
            resp_count = send_request(
                "getTransactionConfirmedByNumberRootBlocks", [tx_id]
            )
            self.assertEqual(resp_count, "0x1")

            # 2 root block confirmation
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))
            resp_hash = send_request(
                "getRootHashConfirmingMinorBlockById", [minor_hash]
            )
            self.assertIsNotNone(resp_hash, "confirmed by root block")
            self.assertNotEqual(resp_hash, "0x" + block.header.get_hash().hex())
            resp_count = send_request(
                "getTransactionConfirmedByNumberRootBlocks", [tx_id]
            )
            self.assertEqual(resp_count, "0x2")

    def test_getTransactionById(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            self.assertEqual(
                call_async(master.get_primary_account_data(acc1)).transaction_count, 0
            )
            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            resp = send_request(
                "getTransactionById",
                [
                    "0x"
                    + tx.get_hash().hex()
                    + acc1.full_shard_key.to_bytes(4, "big").hex()
                ],
            )
            self.assertEqual(resp["hash"], "0x" + tx.get_hash().hex())

    def test_call_success(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            slaves = clusters[0].slave_list

            response = send_request(
                "call", [{"to": "0x" + acc1.serialize().hex(), "gas": hex(21000)}]
            )

            self.assertEqual(response, "0x")
            self.assertEqual(
                len(clusters[0].get_shard_state(2 | 0).tx_queue),
                0,
                "should not affect tx queue",
            )

    def test_call_success_default_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            slaves = clusters[0].slave_list

            # gas is not specified in the request
            response = send_request(
                "call", [{"to": "0x" + acc1.serialize().hex()}, "latest"]
            )

            self.assertEqual(response, "0x")
            self.assertEqual(
                len(clusters[0].get_shard_state(2 | 0).tx_queue),
                0,
                "should not affect tx queue",
            )

    def test_call_failure(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            slaves = clusters[0].slave_list

            # insufficient gas
            response = send_request(
                "call", [{"to": "0x" + acc1.serialize().hex(), "gas": "0x1"}, None]
            )

            self.assertIsNone(response, "failed tx should return None")
            self.assertEqual(
                len(clusters[0].get_shard_state(2 | 0).tx_queue),
                0,
                "should not affect tx queue",
            )

    def test_getTransactionReceipt_not_exist(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(endpoint, ["0x" + bytes(36).hex()])
                self.assertIsNone(resp)

    def test_getTransactionReceipt_on_transfer(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(
                    endpoint,
                    [
                        "0x"
                        + tx.get_hash().hex()
                        + acc1.full_shard_key.to_bytes(4, "big").hex()
                    ],
                )
                self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], "0x5208")
                self.assertIsNone(resp["contractAddress"])

    def test_getTransactionReceipt_on_x_shard_transfer(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            block = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=None)
            )
            call_async(master.add_root_block(block))

            s1, s2 = (
                clusters[0].get_shard_state(2 | 0),
                clusters[0].get_shard_state(2 | 1),
            )
            tx_gen = lambda s, f, t: create_transfer_transaction(
                shard_state=s,
                key=id1.get_key(),
                from_address=f,
                to_address=t,
                gas=21000 if f == t else 30000,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx_gen(s1, acc1, acc2)))
            b1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(b1)))

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )

            call_async(master.add_root_block(root_block))

            tx = tx_gen(s2, acc2, acc2)
            self.assertTrue(slaves[0].add_tx(tx))
            b3 = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=0b11)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 1).add_block(b3)))

            # in-shard tx 21000 + receiving x-shard tx 9000
            self.assertEqual(s2.evm_state.gas_used, 30000)
            self.assertEqual(s2.evm_state.xshard_receive_gas_used, 9000)

            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(
                    endpoint,
                    [
                        "0x"
                        + tx.get_hash().hex()
                        + acc2.full_shard_key.to_bytes(4, "big").hex()
                    ],
                )
                self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], hex(30000))
                self.assertEqual(resp["gasUsed"], hex(21000))
                self.assertIsNone(resp["contractAddress"])

    def test_getTransactionReceipt_on_contract_creation(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            to_full_shard_key = acc1.full_shard_key + 2
            tx = create_contract_creation_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=to_full_shard_key,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(endpoint, ["0x" + tx.get_hash().hex() + "00000002"])
                self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], "0x213eb")

                contract_address = mk_contract_address(
                    acc1.recipient, 0, to_full_shard_key
                )
                self.assertEqual(
                    resp["contractAddress"],
                    "0x"
                    + contract_address.hex()
                    + to_full_shard_key.to_bytes(4, "big").hex(),
                )

    def test_getTransactionReceipt_on_xshard_contract_creation(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block to update block gas limit for xshard tx throttling
            # so that the following tx can be processed
            root_block = call_async(
                master.get_next_block_to_mine(acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            to_full_shard_key = (
                acc1.full_shard_key + 1
            )  # x-shard contract creation should fail
            tx = create_contract_creation_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=to_full_shard_key,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block1)))

            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(endpoint, ["0x" + tx.get_hash().hex() + "00000002"])
                self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], "0x13d6c")
                self.assertIsNone(resp["contractAddress"])

    def test_getLogs(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        expected_log_parts = {
            "logIndex": "0x0",
            "transactionIndex": "0x0",
            "blockNumber": "0x1",
            "blockHeight": "0x1",
            "data": "0x",
        }

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            tx = create_contract_creation_with_event_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))

            for using_eth_endpoint in (True, False):
                shard_id = hex(acc1.full_shard_key)
                if using_eth_endpoint:
                    req = lambda o: send_request("eth_getLogs", [o, shard_id])
                else:
                    # `None` needed to bypass some request modification
                    req = lambda o: send_request("getLogs", [o, shard_id])

                # no filter object as wild cards
                resp = req({})
                self.assertEqual(1, len(resp))
                self.assertDictContainsSubset(expected_log_parts, resp[0])

                # filter by contract address
                contract_addr = mk_contract_address(
                    acc1.recipient, 0, acc1.full_shard_key
                )
                filter_obj = {
                    "address": "0x"
                    + contract_addr.hex()
                    + (
                        ""
                        if using_eth_endpoint
                        else hex(acc1.full_shard_key)[2:].zfill(8)
                    )
                }
                resp = req(filter_obj)
                self.assertEqual(1, len(resp))

                # filter by topics
                filter_obj = {
                    "topics": [
                        "0xa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa"
                    ]
                }
                filter_obj_nested = {
                    "topics": [
                        [
                            "0xa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa"
                        ]
                    ]
                }
                for f in (filter_obj, filter_obj_nested):
                    resp = req(f)
                    self.assertEqual(1, len(resp))
                    self.assertDictContainsSubset(expected_log_parts, resp[0])
                    self.assertEqual(
                        "0xa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa",
                        resp[0]["topics"][0],
                    )

    def test_estimateGas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            response = send_request(
                "estimateGas", [{"to": "0x" + acc1.serialize().hex()}]
            )
            self.assertEqual(response, "0x5208")  # 21000

    def test_getStorageAt(self):
        key = bytes.fromhex(
            "c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3"
        )
        id1 = Identity.create_from_key(key)
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        created_addr = "0x8531eb33bba796115f56ffa1b7df1ea3acdd8cdd00000000"

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            tx = create_contract_with_storage_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))

            for using_eth_endpoint in (True, False):
                if using_eth_endpoint:
                    req = lambda k: send_request(
                        "eth_getStorageAt", [created_addr[:-8], k, "0x0"]
                    )
                else:
                    req = lambda k: send_request("getStorageAt", [created_addr, k])

                # first storage
                response = req("0x0")
                # equals 1234
                self.assertEqual(
                    response,
                    "0x00000000000000000000000000000000000000000000000000000000000004d2",
                )

                # mapping storage
                k = sha3_256(
                    bytes.fromhex(acc1.recipient.hex().zfill(64) + "1".zfill(64))
                )
                response = req("0x" + k.hex())
                self.assertEqual(
                    response,
                    "0x000000000000000000000000000000000000000000000000000000000000162e",
                )

                # doesn't exist
                response = req("0x3")
                self.assertEqual(
                    response,
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                )

    def test_getCode(self):
        key = bytes.fromhex(
            "c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3"
        )
        id1 = Identity.create_from_key(key)
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        created_addr = "0x8531eb33bba796115f56ffa1b7df1ea3acdd8cdd00000000"

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            tx = create_contract_with_storage_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))

            for using_eth_endpoint in (True, False):
                if using_eth_endpoint:
                    resp = send_request("eth_getCode", [created_addr[:-8], "0x0"])
                else:
                    resp = send_request("getCode", [created_addr])

                self.assertEqual(
                    resp,
                    "0x6080604052600080fd00a165627a7a72305820a6ef942c101f06333ac35072a8ff40332c71d0e11cd0e6d86de8cae7b42696550029",
                )

    def test_gasPrice(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # run for multiple times
            for _ in range(3):
                tx = create_transfer_transaction(
                    shard_state=clusters[0].get_shard_state(2 | 0),
                    key=id1.get_key(),
                    from_address=acc1,
                    to_address=acc1,
                    value=0,
                    gas_price=12,
                )
                self.assertTrue(slaves[0].add_tx(tx))

                block = call_async(
                    master.get_next_block_to_mine(address=acc1, branch_value=0b10)
                )
                self.assertTrue(
                    call_async(clusters[0].get_shard(2 | 0).add_block(block))
                )

            for using_eth_endpoint in (True, False):
                if using_eth_endpoint:
                    resp = send_request("eth_gasPrice", ["0x0"])
                else:
                    resp = send_request(
                        "gasPrice", ["0x0", quantity_encoder(token_id_encode("QKC"))]
                    )

                self.assertEqual(resp, "0xc")

    def test_getWork_and_submitWork(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, remote_mining=True, shard_size=1, small_coinbase=True
        ) as clusters, jrpc_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(1 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc1,
                value=0,
                gas_price=12,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            for shard_id in ["0x0", None]:  # shard, then root
                resp = send_request("getWork", [shard_id])
                self.assertEqual(resp[1:], ["0x1", "0xa"])  # height and diff

                header_hash_hex = resp[0]
                if shard_id is not None:  # shard 0
                    miner_address = Address.create_from(
                        master.env.quark_chain_config.shards[1].COINBASE_ADDRESS
                    )
                else:  # root
                    miner_address = Address.create_from(
                        master.env.quark_chain_config.ROOT.COINBASE_ADDRESS
                    )
                block = call_async(
                    master.get_next_block_to_mine(
                        address=miner_address, branch_value=shard_id and 0b01
                    )
                )
                # solve it and submit
                work = MiningWork(bytes.fromhex(header_hash_hex[2:]), 1, 10)
                solver = DoubleSHA256(work)
                nonce = solver.mine(0, 10000).nonce
                mixhash = "0x" + sha3_256(b"").hex()
                resp = send_request(
                    "submitWork",
                    [
                        shard_id,
                        header_hash_hex,
                        hex(nonce),
                        mixhash,
                        "0x" + bytes(65).hex(),
                    ],
                )
                self.assertTrue(resp)

            # show progress on shard 0
            self.assertEqual(
                clusters[0].get_shard_state(1 | 0).get_tip().header.height, 1
            )

    def test_createTransactions(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        loadtest_accounts = [
            {
                "address": "b067ac9ebeeecb10bbcd1088317959d58d1e38f6b0ee10d5",
                "key": "ca0143c9aa51c3013f08e83f3b6368a4f3ba5b52c4841c6e0c22c300f7ee6827",
            },
            {
                "address": "9f2b984937ff8e3f20d2a2592f342f47257870909fffa247",
                "key": "40efdb8528de149c35fb43a572fc821d8fbdf2469dcc7fe1a9e847ef29e3c941",
            },
        ]

        with ClusterContext(
            1, acc1, small_coinbase=True, loadtest_accounts=loadtest_accounts
        ) as clusters, jrpc_server_context(clusters[0].master):
            slaves = clusters[0].slave_list
            master = clusters[0].master

            block = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=None)
            )
            call_async(master.add_root_block(block))

            send_request("createTransactions", {"numTxPerShard": 1, "xShardPercent": 0})
