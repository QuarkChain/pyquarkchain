import asyncio
import json
import logging
import unittest
from contextlib import contextmanager

import aiohttp
from jsonrpcclient.aiohttp_client import aiohttpClient
from jsonrpcclient.exceptions import ReceivedErrorResponse
import websockets

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.jsonrpc import (
    EMPTY_TX_ID,
    JSONRPCHttpServer,
    JSONRPCWebsocketServer,
    quantity_encoder,
    data_encoder,
)
from quarkchain.cluster.miner import DoubleSHA256, MiningWork
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    ClusterContext,
    create_contract_creation_transaction,
    create_contract_creation_with_event_transaction,
    create_contract_with_storage_transaction,
)
from quarkchain.config import ConsensusType
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
def jrpc_http_server_context(master):
    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig()
    env.cluster_config.JSON_RPC_PORT = 38391
    # to pass the circleCi
    env.cluster_config.JSON_RPC_HOST = "127.0.0.1"
    server = JSONRPCHttpServer.start_test_server(env, master)
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


class TestJSONRPCHttp(unittest.TestCase):
    def test_getTransactionCount(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            stats = call_async(master.get_stats())
            self.assertTrue("posw" in json.dumps(stats))

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

    def test_getBalance(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            response = send_request("getBalances", ["0x" + acc1.serialize().hex()])
            self.assertListEqual(
                response["balances"],
                [{"tokenId": "0x8bb0", "tokenStr": "QKC", "balance": "0xf4240"}],
            )

            response = send_request("eth_getBalance", ["0x" + acc1.recipient.hex()])
            self.assertEqual(response, "0xf4240")

    def test_sendTransaction(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
                state.tx_queue.pop_transaction(state.get_transaction_count).tx.evm_tx,
                evm_tx,
            )

    def test_sendTransaction_with_bad_signature(self):
        """ sendTransaction validates signature """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(endpoint, ["0x" + bytes(36).hex()])
                self.assertIsNone(resp)

    def test_getTransactionReceipt_on_transfer(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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

    def test_getTransactionReceipt_on_xshard_transfer_before_enabling_EVM(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            # disable EVM to have fake xshard receipts
            master.env.quark_chain_config.ENABLE_EVM_TIMESTAMP = 2 ** 64 - 1

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
            tx1 = tx_gen(s1, acc1, acc2)
            self.assertTrue(slaves[0].add_tx(tx1))
            b1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(b1)))

            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )

            call_async(master.add_root_block(root_block))

            tx2 = tx_gen(s2, acc2, acc2)
            self.assertTrue(slaves[0].add_tx(tx2))
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
                        + tx2.get_hash().hex()
                        + acc2.full_shard_key.to_bytes(4, "big").hex()
                    ],
                )
                self.assertEqual(resp["transactionHash"], "0x" + tx2.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], hex(30000))
                self.assertEqual(resp["gasUsed"], hex(21000))
                self.assertIsNone(resp["contractAddress"])

            # query xshard tx receipt on the target shard
            resp = send_request(
                endpoint,
                [
                    "0x"
                    + tx1.get_hash().hex()
                    + acc2.full_shard_key.to_bytes(4, "big").hex()
                ],
            )
            self.assertEqual(resp["status"], "0x1")
            # other fields are fake
            self.assertEqual(resp["cumulativeGasUsed"], hex(0))
            self.assertEqual(resp["gasUsed"], hex(0))

    def test_getTransactionReceipt_on_xshard_transfer_after_enabling_EVM(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
            tx = create_transfer_transaction(
                shard_state=s1,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                gas=30000,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))
            # source shard
            b1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(b1)))
            # root chain
            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))
            # target shard
            b3 = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=0b11)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 1).add_block(b3)))

            # query xshard tx receipt on the target shard
            resp = send_request(
                "getTransactionReceipt",
                [
                    "0x"
                    + tx.get_hash().hex()
                    + acc2.full_shard_key.to_bytes(4, "big").hex()
                ],
            )
            self.assertEqual(resp["status"], "0x1")
            self.assertEqual(resp["transactionIndex"], "0x3")
            self.assertEqual(resp["cumulativeGasUsed"], hex(9000))
            self.assertEqual(resp["gasUsed"], hex(9000))

    def test_getTransactionReceipt_on_contract_creation(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block to update block gas limit for xshard tx throttling
            # so that the following tx can be processed
            root_block = call_async(
                master.get_next_block_to_mine(acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            to_full_shard_key = acc1.full_shard_key + 1
            tx = create_contract_creation_with_event_transaction(
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
                self.assertEqual(resp["cumulativeGasUsed"], "0x11374")
                self.assertIsNone(resp["contractAddress"])

            # x-shard contract creation should succeed. check target shard
            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )  # root chain
            call_async(master.add_root_block(root_block))
            block2 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b11)
            )  # target shard
            self.assertTrue(call_async(clusters[0].get_shard(2 | 1).add_block(block2)))
            for endpoint in ("getTransactionReceipt", "eth_getTransactionReceipt"):
                resp = send_request(endpoint, ["0x" + tx.get_hash().hex() + "00000003"])
                self.assertEqual(resp["transactionHash"], "0x" + tx.get_hash().hex())
                self.assertEqual(resp["status"], "0x1")
                self.assertEqual(resp["cumulativeGasUsed"], "0xc515")
                self.assertIsNotNone(resp["contractAddress"])

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
            1, acc1, small_coinbase=True, genesis_minor_quarkash=10000000
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list

            # Add a root block to update block gas limit for xshard tx throttling
            # so that the following tx can be processed
            root_block = call_async(
                master.get_next_block_to_mine(acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            tx = create_contract_creation_with_event_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            expected_log_parts["transactionHash"] = "0x" + tx.get_hash().hex()
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

            # xshard creation and check logs: shard 0 -> shard 1
            tx = create_contract_creation_with_event_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key + 1,
            )
            self.assertTrue(slaves[0].add_tx(tx))
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )  # source shard
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))
            root_block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )  # root chain
            call_async(master.add_root_block(root_block))
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b11)
            )  # target shard
            self.assertTrue(call_async(clusters[0].get_shard(2 | 1).add_block(block)))

            req = lambda o: send_request("getLogs", [o, hex(0b11)])
            # no filter object as wild cards
            resp = req({})
            self.assertEqual(1, len(resp))
            expected_log_parts["transactionIndex"] = "0x3"  # after root block coinbase
            expected_log_parts["transactionHash"] = "0x" + tx.get_hash().hex()
            expected_log_parts["blockHash"] = "0x" + block.header.get_hash().hex()
            self.assertDictContainsSubset(expected_log_parts, resp[0])
            self.assertEqual(2, len(resp[0]["topics"]))
            # missing shard ID should fail
            for endpoint in ("getLogs", "eth_getLogs"):
                with self.assertRaises(ReceivedErrorResponse):
                    send_request(endpoint, [{}])
                with self.assertRaises(ReceivedErrorResponse):
                    send_request(endpoint, [{}, None])

    def test_estimateGas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
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

    def test_getWork_with_optional_diff_divider(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, remote_mining=True, shard_size=1, small_coinbase=True
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            shard = next(iter(slaves[0].shards.values()))
            qkc_config = master.env.quark_chain_config
            qkc_config.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE

            # add a root block first to init shard chains
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

            qkc_config.ROOT.POSW_CONFIG.ENABLED = True
            qkc_config.ROOT.POSW_CONFIG.ENABLE_TIMESTAMP = 0
            qkc_config.ROOT.POSW_CONFIG.WINDOW_SIZE = 2

            shard.state.get_root_chain_stakes = lambda _1, _2: (
                qkc_config.ROOT.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK,
                acc1.recipient,
            )

            resp = send_request("getWork", [None])
            # height and diff, and returns the diff divider since it's PoSW mineable
            self.assertEqual(resp[1:], ["0x2", "0xa", hex(1000)])

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
        ) as clusters, jrpc_http_server_context(clusters[0].master):
            slaves = clusters[0].slave_list
            master = clusters[0].master

            block = call_async(
                master.get_next_block_to_mine(address=acc2, branch_value=None)
            )
            call_async(master.add_root_block(block))

            send_request("createTransactions", {"numTxPerShard": 1, "xShardPercent": 0})


# ------------------------------- Test for JSONRPCWebsocketServer -------------------------------
@contextmanager
def jrpc_websocket_server_context(slave_server, port=38590):
    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig()
    env.cluster_config.JSON_RPC_PORT = 38391
    env.cluster_config.JSON_RPC_HOST = "127.0.0.1"

    env.slave_config = env.cluster_config.get_slave_config("S0")
    env.slave_config.HOST = "0.0.0.0"
    env.slave_config.WEBSOCKET_JSON_RPC_PORT = port
    server = JSONRPCWebsocketServer.start_websocket_server(env, slave_server)
    try:
        yield server
    finally:
        server.shutdown()


def send_websocket_request(request, num_response=1, port=38590):
    responses = []

    async def __send_request(request, port):
        uri = "ws://0.0.0.0:" + str(port)
        async with websockets.connect(uri) as websocket:
            await websocket.send(request)
            while True:
                response = await websocket.recv()
                responses.append(response)
                if len(responses) == num_response:
                    return responses

    return call_async(__send_request(request, port))


async def get_websocket(port=38590):
    uri = "ws://0.0.0.0:" + str(port)
    return await websockets.connect(uri)


class TestJSONRPCWebsocket(unittest.TestCase):
    def test_new_heads(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(clusters[0].slave_list[0]):
            # clusters[0].slave_list[0] has two shards with full_shard_id 2 and 3
            master = clusters[0].master

            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newHeads", "0x00000002"],
                "id": 3,
            }
            websocket = call_async(get_websocket())
            call_async(websocket.send(json.dumps(request)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["id"], 3)

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))
            block_hash = block.header.get_hash()
            block_height = block.header.height

            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(
                response["params"]["result"]["hash"], data_encoder(block_hash)
            )
            self.assertEqual(
                response["params"]["result"]["height"], quantity_encoder(block_height)
            )

    def test_new_heads_with_chain_reorg(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True, genesis_minor_quarkash=10000000
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38591
        ):
            websocket = call_async(get_websocket(port=38591))

            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newHeads", "0x00000002"],
                "id": 3,
            }
            call_async(websocket.send(json.dumps(request)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["id"], 3)

            state = clusters[0].get_shard_state(2 | 0)
            tip = state.get_tip()

            # no chain reorg at this point
            b0 = state.create_block_to_mine(address=acc1)
            state.finalize_and_add_block(b0)
            self.assertEqual(state.header_tip, b0.header)
            response = call_async(websocket.recv())
            d = json.loads(response)
            self.assertEqual(
                d["params"]["result"]["hash"], data_encoder(b0.header.get_hash())
            )

            # fork happens
            b1 = tip.create_block_to_append(address=acc1)
            state.finalize_and_add_block(b1)
            b2 = b1.create_block_to_append(address=acc1)
            state.finalize_and_add_block(b2)
            self.assertEqual(state.header_tip, b2.header)

            # new heads b1, b2 emitted from new chain
            blocks = [b1, b2]
            for b in blocks:
                response = call_async(websocket.recv())
                d = json.loads(response)
                self.assertEqual(
                    d["params"]["result"]["hash"], data_encoder(b.header.get_hash())
                )

    def test_new_pending_xshard_tx_sender(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0x0)
        acc2 = Address.create_from_identity(id1, full_shard_key=0x10001)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38592
        ):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newPendingTransactions", "0x00000002"],
                "id": 6,
            }

            websocket = call_async(get_websocket(38592))
            call_async(websocket.send(json.dumps(request)))

            sub_response = json.loads(call_async(websocket.recv()))
            self.assertEqual(sub_response["id"], 6)
            self.assertEqual(len(sub_response["result"]), 34)

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                gas=30000,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            tx_response = json.loads(call_async(websocket.recv()))
            self.assertEqual(
                tx_response["params"]["subscription"], sub_response["result"]
            )
            self.assertTrue(tx_response["params"]["result"], tx.get_hash())

            b1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(b1)))

    def test_new_pending_xshard_tx_target(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0x10001)
        acc2 = Address.create_from_identity(id1, full_shard_key=0x0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38593
        ):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newPendingTransactions", "0x00000002"],
                "id": 6,
            }
            websocket = call_async(get_websocket(38593))
            call_async(websocket.send(json.dumps(request)))

            sub_response = json.loads(call_async(websocket.recv()))
            self.assertEqual(sub_response["id"], 6)
            self.assertEqual(len(sub_response["result"]), 34)

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(0x10003),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                gas=30000,
                value=12345,
            )
            self.assertTrue(slaves[1].add_tx(tx))

            b1 = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0x10003)
            )
            self.assertTrue(call_async(clusters[0].get_shard(0x10003).add_block(b1)))

            tx_response = json.loads(call_async(websocket.recv()))
            self.assertEqual(
                tx_response["params"]["subscription"], sub_response["result"]
            )
            self.assertTrue(tx_response["params"]["result"], tx.get_hash())

    def test_new_pending_tx_same_acc_multi_subscriptions(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0x0)
        acc2 = Address.create_from_identity(id1, full_shard_key=0x10001)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38594
        ):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=None)
            )
            call_async(master.add_root_block(block))

            requests = []
            REQ_NUM = 5
            for i in range(REQ_NUM):
                req = {
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": ["newPendingTransactions", "0x00000002"],
                    "id": i,
                }
                requests.append(req)

            websocket = call_async(get_websocket(38594))
            [call_async(websocket.send(json.dumps(req))) for req in requests]
            sub_responses = [json.loads(call_async(websocket.recv())) for _ in requests]

            for i, resp in enumerate(sub_responses):
                self.assertEqual(resp["id"], i)
                self.assertEqual(len(resp["result"]), 34)

            tx = create_transfer_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                gas=30000,
                value=12345,
            )
            self.assertTrue(slaves[0].add_tx(tx))

            tx_responses = [json.loads(call_async(websocket.recv())) for _ in requests]
            for i, resp in enumerate(tx_responses):
                self.assertEqual(
                    resp["params"]["subscription"], sub_responses[i]["result"]
                )
                self.assertTrue(resp["params"]["result"], tx.get_hash())

    def test_new_pending_tx_with_reorg(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True, genesis_minor_quarkash=10000000
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38595
        ):
            websocket = call_async(get_websocket(port=38595))
            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newPendingTransactions", "0x00000002"],
                "id": 3,
            }
            call_async(websocket.send(json.dumps(request)))

            sub_response = json.loads(call_async(websocket.recv()))
            self.assertEqual(sub_response["id"], 3)
            self.assertEqual(len(sub_response["result"]), 34)

            state = clusters[0].get_shard_state(2 | 0)
            tip = state.get_tip()

            tx = create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                gas=30000,
                value=12345,
            )
            self.assertTrue(state.add_tx(tx))
            tx_response1 = json.loads(call_async(websocket.recv()))
            self.assertEqual(
                tx_response1["params"]["subscription"], sub_response["result"]
            )
            self.assertTrue(tx_response1["params"]["result"], tx.get_hash())

            b0 = state.create_block_to_mine()
            state.finalize_and_add_block(b0)
            b1 = tip.create_block_to_append()
            state.finalize_and_add_block(b1)
            b2 = b1.create_block_to_append()
            state.finalize_and_add_block(b2)  # fork should happen, b0-b2 is picked up

            tx_response2 = json.loads(call_async(websocket.recv()))
            self.assertEqual(state.header_tip, b2.header)
            self.assertEqual(tx_response2, tx_response1)

    def test_logs(self):
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
            1, acc1, small_coinbase=True, genesis_minor_quarkash=10000000
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38596
        ):
            master = clusters[0].master
            slaves = clusters[0].slave_list
            websocket = call_async(get_websocket(port=38596))

            # filter by contract address
            contract_addr = mk_contract_address(acc1.recipient, 0, acc1.full_shard_key)
            filter_req = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": [
                    "logs",
                    "0x00000002",
                    {
                        "address": "0x"
                        + contract_addr.hex()
                        + hex(acc1.full_shard_key)[2:].zfill(8)
                    },
                ],
                "id": 4,
            }
            call_async(websocket.send(json.dumps(filter_req)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["id"], 4)

            # filter by topics
            filter_req = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": [
                    "logs",
                    "0x00000002",
                    {
                        "topics": [
                            "0xa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa"
                        ]
                    },
                ],
                "id": 5,
            }
            call_async(websocket.send(json.dumps(filter_req)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["id"], 5)

            tx = create_contract_creation_with_event_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),  # full_shard_id = 2
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            expected_log_parts["transactionHash"] = "0x" + tx.get_hash().hex()
            self.assertTrue(slaves[0].add_tx(tx))

            block = call_async(
                master.get_next_block_to_mine(
                    address=acc1, branch_value=0b10
                )  # branch_value = 2
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))

            count = 0
            while count < 2:
                response = call_async(websocket.recv())
                count += 1
                d = json.loads(response)
                self.assertDictContainsSubset(expected_log_parts, d["params"]["result"])
                self.assertEqual(
                    "0xa9378d5bd800fae4d5b8d4c6712b2b64e8ecc86fdc831cb51944000fc7c8ecfa",
                    d["params"]["result"]["topics"][0],
                )
            self.assertEqual(count, 2)

    def test_log_removed_flag_with_chain_reorg(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True, genesis_minor_quarkash=10000000
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38597
        ):
            websocket = call_async(get_websocket(port=38597))

            # a log subscriber with no-filter request
            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["logs", "0x00000002", {}],
                "id": 3,
            }
            call_async(websocket.send(json.dumps(request)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["id"], 3)

            state = clusters[0].get_shard_state(2 | 0)
            tip = state.get_tip()
            b0 = state.create_block_to_mine(address=acc1)
            tx = create_contract_creation_with_event_transaction(
                shard_state=clusters[0].get_shard_state(2 | 0),  # full_shard_id = 2
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=acc1.full_shard_key,
            )
            b0.add_tx(tx)
            state.finalize_and_add_block(b0)
            self.assertEqual(state.header_tip, b0.header)
            tx_hash = tx.get_hash()

            response = call_async(websocket.recv())
            d = json.loads(response)
            self.assertEqual(
                d["params"]["result"]["transactionHash"], data_encoder(tx_hash)
            )
            self.assertEqual(d["params"]["result"]["removed"], False)

            # fork happens
            b1 = tip.create_block_to_append(address=acc1)
            b1.add_tx(tx)
            state.finalize_and_add_block(b1)
            b2 = b1.create_block_to_append(address=acc1)
            state.finalize_and_add_block(b2)
            self.assertEqual(state.header_tip, b2.header)

            # log emitted from old chain, flag is set to True
            response = call_async(websocket.recv())
            d = json.loads(response)
            self.assertEqual(
                d["params"]["result"]["transactionHash"], data_encoder(tx_hash)
            )
            self.assertEqual(d["params"]["result"]["removed"], True)

            # log emitted from new chain
            response = call_async(websocket.recv())
            d = json.loads(response)
            self.assertEqual(
                d["params"]["result"]["transactionHash"], data_encoder(tx_hash)
            )
            self.assertEqual(d["params"]["result"]["removed"], False)

    def test_invalid_subscription(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38598
        ):
            # Invalid subscription type
            request1 = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newBlocks", "0x00000002"],
                "id": 3,
            }
            # Invalid full shard id
            request2 = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newHeads", "0x00040002"],
                "id": 3,
            }

            websocket = call_async(get_websocket(port=38598))
            [
                call_async(websocket.send(json.dumps(req)))
                for req in [request1, request2]
            ]
            responses = [json.loads(call_async(websocket.recv())) for _ in range(2)]
            [self.assertTrue(resp["error"]) for resp in responses]  # emit error message

    def test_multi_subs_with_some_unsubs_in_one_ws_conn(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38599
        ):
            # clusters[0].slave_list[0] has two shards with full_shard_id 2 and 3
            master = clusters[0].master
            websocket = call_async(get_websocket(port=38599))

            # make 3 subscriptions on new heads
            ids = [3, 4, 5]
            sub_ids = []
            for id in ids:
                request = {
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": ["newHeads", "0x00000002"],
                    "id": id,
                }
                call_async(websocket.send(json.dumps(request)))
                response = call_async(websocket.recv())
                response = json.loads(response)
                sub_ids.append(response["result"])
                self.assertEqual(response["id"], id)

            # cancel the first subscription
            request = {
                "jsonrpc": "2.0",
                "method": "unsubscribe",
                "params": [sub_ids[0]],
                "id": 3,
            }
            call_async(websocket.send(json.dumps(request)))
            response = call_async(websocket.recv())
            response = json.loads(response)
            self.assertEqual(response["result"], True)  # unsubscribed successfully

            # add a new block, should expect only 2 responses
            root_block = call_async(
                master.get_next_block_to_mine(acc1, branch_value=None)
            )
            call_async(master.add_root_block(root_block))

            block = call_async(
                master.get_next_block_to_mine(address=acc1, branch_value=0b10)
            )
            self.assertTrue(call_async(clusters[0].get_shard(2 | 0).add_block(block)))

            for sub_id in sub_ids[1:]:
                response = call_async(websocket.recv())
                response = json.loads(response)
                self.assertEqual(response["params"]["subscription"], sub_id)

    def test_unsubscribe(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        with ClusterContext(
            1, acc1, small_coinbase=True
        ) as clusters, jrpc_websocket_server_context(
            clusters[0].slave_list[0], port=38600
        ):
            request = {
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": ["newPendingTransactions", "0x00000002"],
                "id": 6,
            }
            websocket = call_async(get_websocket(port=38600))
            call_async(websocket.send(json.dumps(request)))
            sub_response = json.loads(call_async(websocket.recv()))

            # Check subscription response
            self.assertEqual(sub_response["id"], 6)
            self.assertEqual(len(sub_response["result"]), 34)

            unsubscribe = {
                "jsonrpc": "2.0",
                "method": "unsubscribe",
                "params": [sub_response["result"]],
                "id": 3,
            }

            # Unsubscribe successfully
            call_async(websocket.send(json.dumps(unsubscribe)))
            response = json.loads(call_async(websocket.recv()))
            self.assertTrue(response["result"])
            self.assertEqual(response["id"], 3)

            # Invalid unsubscription if sub_id does not exist
            call_async(websocket.send(json.dumps(unsubscribe)))
            response = json.loads(call_async(websocket.recv()))
            self.assertTrue(response["error"])
