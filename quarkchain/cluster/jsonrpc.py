import asyncio
import inspect
import json
from typing import Callable, Dict, List, Optional

import aiohttp_cors
import websockets
import rlp
from aiohttp import web
from async_armor import armor
from decorator import decorator
from jsonrpcserver import config
from jsonrpcserver.async_methods import AsyncMethods
from jsonrpcserver.exceptions import InvalidParams, InvalidRequest, ServerError

from quarkchain.cluster.master import MasterServer
from quarkchain.cluster.rpc import AccountBranchData
from quarkchain.cluster.slave import SlaveServer
from quarkchain.core import (
    Address,
    Branch,
    Log,
    MinorBlock,
    RootBlock,
    SerializedEvmTransaction,
    TokenBalanceMap,
    TransactionReceipt,
    TypedTransaction,
    Constant,
    MinorBlockHeader,
    PoSWInfo,
)
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.utils import denoms, is_numeric
from quarkchain.p2p.p2p_manager import P2PManager
from quarkchain.utils import Logger, token_id_decode, token_id_encode
from cachetools import LRUCache
import uuid
from quarkchain.cluster.log_filter import LogFilter
from quarkchain.cluster.subscription import SUB_LOGS

# defaults
DEFAULT_STARTGAS = 100 * 1000
DEFAULT_GASPRICE = 10 * denoms.gwei

# Allow 16 MB request for submitting big blocks
# TODO: revisit this parameter
JSON_RPC_CLIENT_REQUEST_MAX_SIZE = 16 * 1024 * 1024

# Disable jsonrpcserver logging
config.log_requests = False
config.log_responses = False

EMPTY_TX_ID = "0x" + "0" * Constant.TX_ID_HEX_LENGTH


def quantity_decoder(hex_str, allow_optional=False):
    """Decode `hexStr` representing a quantity."""
    if allow_optional and hex_str is None:
        return None
    # must start with "0x"
    if not hex_str.startswith("0x") or len(hex_str) < 3:
        raise InvalidParams("Invalid quantity encoding")

    try:
        return int(hex_str, 16)
    except ValueError:
        raise InvalidParams("Invalid quantity encoding")


def quantity_encoder(i):
    """Encode integer quantity `data`."""
    assert is_numeric(i)
    return hex(i)


def data_decoder(hex_str, allow_optional=False):
    """Decode `hexStr` representing unformatted hex_str."""
    if allow_optional and hex_str is None:
        return None
    if not hex_str.startswith("0x"):
        raise InvalidParams("Invalid hex_str encoding")
    try:
        return bytes.fromhex(hex_str[2:])
    except Exception:
        raise InvalidParams("Invalid hex_str hex encoding")


def data_encoder(data_bytes):
    """Encode unformatted binary `dataBytes`."""
    return "0x" + data_bytes.hex()


def address_decoder(hex_str):
    """Decode an address from hex with 0x prefix to 24 bytes."""
    addr_bytes = data_decoder(hex_str)
    if len(addr_bytes) not in (24, 0):
        raise InvalidParams("Addresses must be 24 or 0 bytes long")
    return addr_bytes


def address_encoder(addr_bytes):
    assert len(addr_bytes) == 24
    return data_encoder(addr_bytes)


def recipient_decoder(hex_str, allow_optional=False):
    """Decode an recipient from hex with 0x prefix to 20 bytes."""
    if allow_optional and hex_str is None:
        return None
    recipient_bytes = data_decoder(hex_str)
    if len(recipient_bytes) not in (20, 0):
        raise InvalidParams("Addresses must be 20 or 0 bytes long")
    return recipient_bytes


def recipient_encoder(recipient_bytes):
    assert len(recipient_bytes) == 20
    return data_encoder(recipient_bytes)


def full_shard_key_decoder(hex_str):
    b = data_decoder(hex_str)
    if len(b) != 4:
        raise InvalidParams("Full shard id must be 4 bytes")
    return int.from_bytes(b, byteorder="big")


def full_shard_key_encoder(full_shard_key):
    return data_encoder(full_shard_key.to_bytes(4, byteorder="big"))


def id_encoder(hash_bytes, full_shard_key):
    """Encode hash and full_shard_key into hex"""
    return data_encoder(hash_bytes + full_shard_key.to_bytes(4, byteorder="big"))


def id_decoder(hex_str):
    """Decode an id to (hash, full_shard_key)"""
    data_bytes = data_decoder(hex_str)
    if len(data_bytes) != 36:
        raise InvalidParams("Invalid id encoding")
    return data_bytes[:32], int.from_bytes(data_bytes[32:], byteorder="big")


def hash_decoder(hex_str):
    """Decode a block hash."""
    decoded = data_decoder(hex_str)
    if len(decoded) != 32:
        raise InvalidParams("Hashes must be 32 bytes long")
    return decoded


def signature_decoder(hex_str):
    """Decode a block signature."""
    if not hex_str:
        return None
    decoded = data_decoder(hex_str)
    if len(decoded) != 65:
        raise InvalidParams("Signature must be 65 bytes long")
    return decoded


def bool_decoder(data):
    if not isinstance(data, bool):
        raise InvalidParams("Parameter must be boolean")
    return data


def _add_posw_info_to_resp(d: Dict, diff: int, posw_info: PoSWInfo):
    d["effectiveDifficulty"] = quantity_encoder(posw_info.effective_difficulty)
    d["poswMineableBlocks"] = quantity_encoder(posw_info.posw_mineable_blocks)
    d["poswMinedBlocks"] = quantity_encoder(posw_info.posw_mined_blocks)
    d["stakingApplied"] = posw_info.effective_difficulty < diff


def root_block_encoder(block, extra_info):
    header = block.header

    d = {
        "id": data_encoder(header.get_hash()),
        "height": quantity_encoder(header.height),
        "hash": data_encoder(header.get_hash()),
        "sealHash": data_encoder(header.get_hash_for_mining()),
        "hashPrevBlock": data_encoder(header.hash_prev_block),
        "idPrevBlock": data_encoder(header.hash_prev_block),
        "nonce": quantity_encoder(header.nonce),
        "hashMerkleRoot": data_encoder(header.hash_merkle_root),
        "miner": address_encoder(header.coinbase_address.serialize()),
        "coinbase": balances_encoder(header.coinbase_amount_map),
        "difficulty": quantity_encoder(header.difficulty),
        "timestamp": quantity_encoder(header.create_time),
        "size": quantity_encoder(len(block.serialize())),
        "minorBlockHeaders": [],
        "signature": data_encoder(header.signature),
    }
    if extra_info:
        _add_posw_info_to_resp(d, header.difficulty, extra_info)

    for header in block.minor_block_header_list:
        h = minor_block_header_encoder(header)
        d["minorBlockHeaders"].append(h)
    return d


def minor_block_encoder(block, include_transactions=False, extra_info=None):
    """Encode a block as JSON object.

    :param block: a :class:`ethereum.block.Block`
    :param include_transactions: if true transaction details are included, otherwise
                                 only their hashes
    :param extra_info: MinorBlockExtraInfo
    :returns: a json encodable dictionary
    """
    header = block.header
    meta = block.meta

    header_info = minor_block_header_encoder(header)
    d = {
        **header_info,
        "hashMerkleRoot": data_encoder(meta.hash_merkle_root),
        "hashEvmStateRoot": data_encoder(meta.hash_evm_state_root),
        "gasUsed": quantity_encoder(meta.evm_gas_used),
        "size": quantity_encoder(len(block.serialize())),
    }
    if include_transactions:
        d["transactions"] = []
        for i, _ in enumerate(block.tx_list):
            d["transactions"].append(tx_encoder(block, i))
    else:
        d["transactions"] = [
            id_encoder(tx.get_hash(), block.header.branch.get_full_shard_id())
            for tx in block.tx_list
        ]
    if extra_info:
        _add_posw_info_to_resp(d, header.difficulty, extra_info)
    return d


def minor_block_header_encoder(header: MinorBlockHeader) -> Dict:
    d = {
        "id": id_encoder(header.get_hash(), header.branch.get_full_shard_id()),
        "height": quantity_encoder(header.height),
        "hash": data_encoder(header.get_hash()),
        "fullShardId": quantity_encoder(header.branch.get_full_shard_id()),
        "chainId": quantity_encoder(header.branch.get_chain_id()),
        "shardId": quantity_encoder(header.branch.get_shard_id()),
        "hashPrevMinorBlock": data_encoder(header.hash_prev_minor_block),
        "idPrevMinorBlock": id_encoder(
            header.hash_prev_minor_block, header.branch.get_full_shard_id()
        ),
        "hashPrevRootBlock": data_encoder(header.hash_prev_root_block),
        "nonce": quantity_encoder(header.nonce),
        "miner": address_encoder(header.coinbase_address.serialize()),
        "coinbase": balances_encoder(header.coinbase_amount_map),
        "difficulty": quantity_encoder(header.difficulty),
        "extraData": data_encoder(header.extra_data),
        "gasLimit": quantity_encoder(header.evm_gas_limit),
        "timestamp": quantity_encoder(header.create_time),
    }
    return d


def tx_encoder(block, i):
    """Encode a transaction as JSON object.

    `transaction` is the `i`th transaction in `block`.
    """
    tx = block.tx_list[i]
    evm_tx = tx.tx.to_evm_tx()
    branch = block.header.branch
    return {
        "id": id_encoder(tx.get_hash(), evm_tx.from_full_shard_key),
        "hash": data_encoder(tx.get_hash()),
        "nonce": quantity_encoder(evm_tx.nonce),
        "timestamp": quantity_encoder(block.header.create_time),
        "fullShardId": quantity_encoder(branch.get_full_shard_id()),
        "chainId": quantity_encoder(branch.get_chain_id()),
        "shardId": quantity_encoder(branch.get_shard_id()),
        "blockId": id_encoder(block.header.get_hash(), branch.get_full_shard_id()),
        "blockHeight": quantity_encoder(block.header.height),
        "transactionIndex": quantity_encoder(i),
        "from": data_encoder(evm_tx.sender),
        "to": data_encoder(evm_tx.to),
        "fromFullShardKey": full_shard_key_encoder(evm_tx.from_full_shard_key),
        "toFullShardKey": full_shard_key_encoder(evm_tx.to_full_shard_key),
        "value": quantity_encoder(evm_tx.value),
        "gasPrice": quantity_encoder(evm_tx.gasprice),
        "gas": quantity_encoder(evm_tx.startgas),
        "data": data_encoder(evm_tx.data),
        "networkId": quantity_encoder(evm_tx.network_id),
        "transferTokenId": quantity_encoder(evm_tx.transfer_token_id),
        "gasTokenId": quantity_encoder(evm_tx.gas_token_id),
        "transferTokenStr": token_id_decode(evm_tx.transfer_token_id),
        "gasTokenStr": token_id_decode(evm_tx.gas_token_id),
        "r": quantity_encoder(evm_tx.r),
        "s": quantity_encoder(evm_tx.s),
        "v": quantity_encoder(evm_tx.v),
    }


def tx_detail_encoder(tx):
    """Encode a transaction detail object as JSON object. Used for indexing server."""
    return {
        "txId": id_encoder(tx.tx_hash, tx.from_address.full_shard_key),
        "fromAddress": address_encoder(tx.from_address.serialize()),
        "toAddress": address_encoder(tx.to_address.serialize())
        if tx.to_address
        else "0x",
        "value": quantity_encoder(tx.value),
        "transferTokenId": quantity_encoder(tx.transfer_token_id),
        "transferTokenStr": token_id_decode(tx.transfer_token_id),
        "gasTokenId": quantity_encoder(tx.gas_token_id),
        "gasTokenStr": token_id_decode(tx.gas_token_id),
        "blockHeight": quantity_encoder(tx.block_height),
        "timestamp": quantity_encoder(tx.timestamp),
        "success": tx.success,
        "isFromRootChain": tx.is_from_root_chain,
        "nonce": quantity_encoder(tx.nonce),
    }


def loglist_encoder(loglist: List[Log], is_removed: bool = False):
    """Encode a list of log"""
    result = []
    for l in loglist:
        result.append(
            {
                "logIndex": quantity_encoder(l.log_idx),
                "transactionIndex": quantity_encoder(l.tx_idx),
                "transactionHash": data_encoder(l.tx_hash),
                "blockHash": data_encoder(l.block_hash),
                "blockNumber": quantity_encoder(l.block_number),
                "blockHeight": quantity_encoder(l.block_number),
                "address": data_encoder(l.recipient),
                "recipient": data_encoder(l.recipient),
                "data": data_encoder(l.data),
                "topics": [data_encoder(topic) for topic in l.topics],
                "removed": is_removed,
            }
        )
    return result


def receipt_encoder(block: MinorBlock, i: int, receipt: TransactionReceipt):
    tx_id, tx_hash = None, None  # if empty, will be populated at call site
    if i < len(block.tx_list):
        tx = block.tx_list[i]
        evm_tx = tx.tx.to_evm_tx()
        tx_id = id_encoder(tx.get_hash(), evm_tx.from_full_shard_key)
        tx_hash = data_encoder(tx.get_hash())
    resp = {
        "transactionId": tx_id,
        "transactionHash": tx_hash,
        "transactionIndex": quantity_encoder(i),
        "blockId": id_encoder(
            block.header.get_hash(), block.header.branch.get_full_shard_id()
        ),
        "blockHash": data_encoder(block.header.get_hash()),
        "blockHeight": quantity_encoder(block.header.height),
        "blockNumber": quantity_encoder(block.header.height),
        "cumulativeGasUsed": quantity_encoder(receipt.gas_used),
        "gasUsed": quantity_encoder(receipt.gas_used - receipt.prev_gas_used),
        "status": quantity_encoder(1 if receipt.success == b"\x01" else 0),
        "contractAddress": (
            address_encoder(receipt.contract_address.serialize())
            if not receipt.contract_address.is_empty()
            else None
        ),
        "logs": loglist_encoder(receipt.logs),
        "timestamp": quantity_encoder(block.header.create_time),
    }

    return resp


def balances_encoder(balances: TokenBalanceMap) -> List[Dict]:
    balance_list = []
    for k, v in balances.balance_map.items():
        balance_list.append(
            {
                "tokenId": quantity_encoder(k),
                "tokenStr": token_id_decode(k),
                "balance": quantity_encoder(v),
            }
        )
    return balance_list


def decode_arg(name, decoder, allow_optional=False):
    """Create a decorator that applies `decoder` to argument `name`."""

    @decorator
    def new_f(f, *args, **kwargs):
        call_args = inspect.getcallargs(f, *args, **kwargs)
        call_args[name] = (
            decoder(call_args[name], allow_optional=True)
            if allow_optional
            else decoder(call_args[name])
        )
        return f(**call_args)

    return new_f


def encode_res(encoder):
    """Create a decorator that applies `encoder` to the return value of the
    decorated function.
    """

    @decorator
    async def new_f(f, *args, **kwargs):
        res = await f(*args, **kwargs)
        return encoder(res)

    return new_f


def block_height_decoder(data):
    """Decode block height string, which can either be None, 'latest', 'earliest' or a hex number
    of minor block height"""
    if data is None or data == "latest":
        return None
    if data == "earliest":
        return 0
    # TODO: support pending
    return quantity_decoder(data)


def shard_id_decoder(data):
    try:
        return quantity_decoder(data)
    except Exception:
        return None


def eth_address_to_quarkchain_address_decoder(hex_str):
    eth_hex = hex_str[2:]
    if len(eth_hex) != 40:
        raise InvalidParams("Addresses must be 40 or 0 bytes long")
    return address_decoder("0x" + eth_hex + "00000001")


def _parse_log_request(
    params: Dict, addr_decoder: Callable[[str], bytes]
) -> (bytes, bytes):
    """Returns addresses and topics from a EVM log request."""
    addresses, topics = [], []
    if "address" in params:
        if isinstance(params["address"], str):
            addresses = [Address.deserialize(addr_decoder(params["address"]))]
        elif isinstance(params["address"], list):
            addresses = [
                Address.deserialize(addr_decoder(a)) for a in params["address"]
            ]
    if "topics" in params:
        for topic_item in params["topics"]:
            if isinstance(topic_item, str):
                topics.append([data_decoder(topic_item)])
            elif isinstance(topic_item, list):
                topics.append([data_decoder(tp) for tp in topic_item])
    return addresses, topics


public_methods = AsyncMethods()
private_methods = AsyncMethods()


# noinspection PyPep8Naming
class JSONRPCHttpServer:
    @classmethod
    def start_public_server(cls, env, master_server):
        server = cls(
            env,
            master_server,
            env.cluster_config.JSON_RPC_PORT,
            env.cluster_config.JSON_RPC_HOST,
            public_methods,
        )
        server.start()
        return server

    @classmethod
    def start_private_server(cls, env, master_server):
        server = cls(
            env,
            master_server,
            env.cluster_config.PRIVATE_JSON_RPC_PORT,
            env.cluster_config.PRIVATE_JSON_RPC_HOST,
            private_methods,
        )
        server.start()
        return server

    @classmethod
    def start_test_server(cls, env, master_server):
        methods = AsyncMethods()
        for method in public_methods.values():
            methods.add(method)
        for method in private_methods.values():
            methods.add(method)
        server = cls(
            env,
            master_server,
            env.cluster_config.JSON_RPC_PORT,
            env.cluster_config.JSON_RPC_HOST,
            methods,
        )
        server.start()
        return server

    def __init__(
        self, env, master_server: MasterServer, port, host, methods: AsyncMethods
    ):
        self.loop = asyncio.get_event_loop()
        self.port = port
        self.host = host
        self.env = env
        self.master = master_server
        self.counters = dict()

        # Bind RPC handler functions to this instance
        self.handlers = AsyncMethods()
        for rpc_name in methods:
            func = methods[rpc_name]
            self.handlers[rpc_name] = func.__get__(self, self.__class__)

    async def __handle(self, request):
        request = await request.text()
        Logger.info(request)

        d = dict()
        try:
            d = json.loads(request)
        except Exception:
            pass
        method = d.get("method", "null")
        if method in self.counters:
            self.counters[method] += 1
        else:
            self.counters[method] = 1
        # Use armor to prevent the handler from being cancelled when
        # aiohttp server loses connection to client
        response = await armor(self.handlers.dispatch(request))
        if "error" in response:
            Logger.error(response)
        if response.is_notification:
            return web.Response()
        return web.json_response(response, status=response.http_status)

    def start(self):
        app = web.Application(client_max_size=JSON_RPC_CLIENT_REQUEST_MAX_SIZE)
        cors = aiohttp_cors.setup(app)
        route = app.router.add_post("/", self.__handle)
        cors.add(
            route,
            {
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers=("X-Custom-Server-Header",),
                    allow_methods=["POST", "PUT"],
                    allow_headers=("X-Requested-With", "Content-Type"),
                )
            },
        )
        self.runner = web.AppRunner(app, access_log=None)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, self.host, self.port)
        self.loop.run_until_complete(site.start())

    def shutdown(self):
        self.loop.run_until_complete(self.runner.cleanup())

    # JSON RPC handlers
    @public_methods.add
    @decode_arg("quantity", quantity_decoder)
    @encode_res(quantity_encoder)
    async def echoQuantity(self, quantity):
        return quantity

    @public_methods.add
    @decode_arg("data", data_decoder)
    @encode_res(data_encoder)
    async def echoData(self, data):
        return data

    @public_methods.add
    async def networkInfo(self):
        return {
            "networkId": quantity_encoder(
                self.master.env.quark_chain_config.NETWORK_ID
            ),
            "chainSize": quantity_encoder(
                self.master.env.quark_chain_config.CHAIN_SIZE
            ),
            "shardSizes": [
                quantity_encoder(c.SHARD_SIZE)
                for c in self.master.env.quark_chain_config.CHAINS
            ],
            "syncing": self.master.is_syncing(),
            "mining": self.master.is_mining(),
            "shardServerCount": len(self.master.slave_pool),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("block_height", block_height_decoder)
    @encode_res(quantity_encoder)
    async def getTransactionCount(self, address, block_height=None):
        account_branch_data = await self.master.get_primary_account_data(
            Address.deserialize(address), block_height
        )
        return account_branch_data.transaction_count

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("block_height", block_height_decoder)
    async def getBalances(self, address, block_height=None):
        account_branch_data = await self.master.get_primary_account_data(
            Address.deserialize(address), block_height
        )
        branch = account_branch_data.branch
        balances = account_branch_data.token_balances
        return {
            "branch": quantity_encoder(branch.value),
            "fullShardId": quantity_encoder(branch.get_full_shard_id()),
            "shardId": quantity_encoder(branch.get_shard_id()),
            "chainId": quantity_encoder(branch.get_chain_id()),
            "balances": balances_encoder(balances),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("block_height", block_height_decoder)
    async def getAccountData(self, address, block_height=None, include_shards=False):
        # do not allow specify height if client wants info on all shards
        if include_shards and block_height is not None:
            return None

        primary = None
        address = Address.deserialize(address)
        if not include_shards:
            account_branch_data = await self.master.get_primary_account_data(
                address, block_height
            )  # type: AccountBranchData
            branch = account_branch_data.branch
            count = account_branch_data.transaction_count

            balances = account_branch_data.token_balances
            primary = {
                "fullShardId": quantity_encoder(branch.get_full_shard_id()),
                "shardId": quantity_encoder(branch.get_shard_id()),
                "chainId": quantity_encoder(branch.get_chain_id()),
                "balances": balances_encoder(balances),
                "transactionCount": quantity_encoder(count),
                "isContract": account_branch_data.is_contract,
                "minedBlocks": quantity_encoder(account_branch_data.mined_blocks),
                "poswMineableBlocks": quantity_encoder(
                    account_branch_data.posw_mineable_blocks
                ),
            }
            return {"primary": primary}

        branch_to_account_branch_data = await self.master.get_account_data(address)

        shards = []
        for branch, account_branch_data in branch_to_account_branch_data.items():
            balances = account_branch_data.token_balances
            data = {
                "fullShardId": quantity_encoder(branch.get_full_shard_id()),
                "shardId": quantity_encoder(branch.get_shard_id()),
                "chainId": quantity_encoder(branch.get_chain_id()),
                "balances": balances_encoder(balances),
                "transactionCount": quantity_encoder(
                    account_branch_data.transaction_count
                ),
                "isContract": account_branch_data.is_contract,
            }
            shards.append(data)

            if branch.get_full_shard_id() == self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            ):
                primary = data.copy()
                primary["minedBlocks"] = quantity_encoder(
                    account_branch_data.mined_blocks
                )
                primary["poswMineableBlocks"] = quantity_encoder(
                    account_branch_data.posw_mineable_blocks
                )

        return {"primary": primary, "shards": shards}

    @public_methods.add
    async def sendTransaction(self, data):
        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        to = get_data_default("to", recipient_decoder, b"")
        startgas = get_data_default("gas", quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default("gasPrice", quantity_decoder, DEFAULT_GASPRICE)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")
        v = get_data_default("v", quantity_decoder, 0)
        r = get_data_default("r", quantity_decoder, 0)
        s = get_data_default("s", quantity_decoder, 0)
        nonce = get_data_default("nonce", quantity_decoder, None)

        to_full_shard_key = get_data_default(
            "toFullShardKey", full_shard_key_decoder, None
        )
        from_full_shard_key = get_data_default(
            "fromFullShardKey", full_shard_key_decoder, None
        )
        network_id = get_data_default(
            "networkId", quantity_decoder, self.master.env.quark_chain_config.NETWORK_ID
        )

        gas_token_id = get_data_default(
            "gasTokenId", quantity_decoder, self.env.quark_chain_config.genesis_token
        )
        transfer_token_id = get_data_default(
            "transferTokenId",
            quantity_decoder,
            self.env.quark_chain_config.genesis_token,
        )

        if nonce is None:
            raise InvalidParams("Missing nonce")
        if not (v and r and s):
            raise InvalidParams("Missing v, r, s")
        if from_full_shard_key is None:
            raise InvalidParams("Missing fromFullShardKey")

        if to_full_shard_key is None:
            to_full_shard_key = from_full_shard_key

        evm_tx = EvmTransaction(
            nonce,
            gasprice,
            startgas,
            to,
            value,
            data_,
            v=v,
            r=r,
            s=s,
            from_full_shard_key=from_full_shard_key,
            to_full_shard_key=to_full_shard_key,
            network_id=network_id,
            gas_token_id=gas_token_id,
            transfer_token_id=transfer_token_id,
        )
        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
        success = await self.master.add_transaction(tx)
        if not success:
            return EMPTY_TX_ID
        return id_encoder(tx.get_hash(), from_full_shard_key)

    @public_methods.add
    @decode_arg("tx_data", data_decoder)
    async def sendRawTransaction(self, tx_data):
        evm_tx = rlp.decode(tx_data, EvmTransaction)
        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
        success = await self.master.add_transaction(tx)
        if not success:
            return EMPTY_TX_ID
        return id_encoder(tx.get_hash(), evm_tx.from_full_shard_key)

    @public_methods.add
    @decode_arg("block_id", data_decoder)
    @decode_arg("need_extra_info", bool_decoder)
    async def getRootBlockById(self, block_id, need_extra_info=True):
        block, extra_info = await self.master.get_root_block_by_height_or_hash(
            None, block_id, need_extra_info
        )
        if not block:
            return None
        return root_block_encoder(block, extra_info)

    @public_methods.add
    @decode_arg("need_extra_info", bool_decoder)
    async def getRootBlockByHeight(self, height=None, need_extra_info=True):
        if height is not None:
            height = quantity_decoder(height)
        block, extra_info = await self.master.get_root_block_by_height_or_hash(
            height, None, need_extra_info
        )
        if not block:
            return None
        return root_block_encoder(block, extra_info)

    @public_methods.add
    @decode_arg("block_id", id_decoder)
    @decode_arg("include_transactions", bool_decoder)
    @decode_arg("need_extra_info", bool_decoder)
    async def getMinorBlockById(
        self, block_id, include_transactions=False, need_extra_info=True
    ):
        block_hash, full_shard_key = block_id
        try:
            branch = Branch(
                self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                    full_shard_key
                )
            )
        except Exception:
            return None
        block, extra_info = await self.master.get_minor_block_by_hash(
            block_hash, branch, need_extra_info
        )
        if not block:
            return None
        return minor_block_encoder(block, include_transactions, extra_info)

    @public_methods.add
    @decode_arg("full_shard_key", quantity_decoder)
    @decode_arg("include_transactions", bool_decoder)
    @decode_arg("need_extra_info", bool_decoder)
    async def getMinorBlockByHeight(
        self,
        full_shard_key: int,
        height=None,
        include_transactions=False,
        need_extra_info=True,
    ):
        if height is not None:
            height = quantity_decoder(height)
        try:
            branch = Branch(
                self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                    full_shard_key
                )
            )
        except Exception:
            return None
        block, extra_info = await self.master.get_minor_block_by_height(
            height, branch, need_extra_info
        )
        if not block:
            return None
        return minor_block_encoder(block, include_transactions, extra_info)

    @public_methods.add
    @decode_arg("tx_id", id_decoder)
    async def getTransactionById(self, tx_id):
        tx_hash, full_shard_key = tx_id
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        minor_block, i = await self.master.get_transaction_by_hash(tx_hash, branch)
        if not minor_block:
            return None
        if len(minor_block.tx_list) <= i:
            return None
        return tx_encoder(minor_block, i)

    @public_methods.add
    @decode_arg("block_height", block_height_decoder)
    async def call(self, data, block_height=None):
        return await self._call_or_estimate_gas(
            is_call=True, block_height=block_height, **data
        )

    @public_methods.add
    async def estimateGas(self, data):
        return await self._call_or_estimate_gas(is_call=False, **data)

    @public_methods.add
    async def getTransactionReceipt(self, tx_id):
        id_bytes = data_decoder(tx_id)
        if len(id_bytes) != 36:
            raise InvalidParams("Invalid id encoding")
        tx_hash, full_shard_key = (
            id_bytes[:32],
            int.from_bytes(id_bytes[32:], byteorder="big"),
        )
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        resp = await self.master.get_transaction_receipt(tx_hash, branch)
        if not resp:
            return None
        minor_block, i, receipt = resp

        ret = receipt_encoder(minor_block, i, receipt)
        if ret["transactionId"] is None:
            ret["transactionId"] = tx_id
            ret["transactionHash"] = data_encoder(tx_hash)
        return ret

    @public_methods.add
    @decode_arg("full_shard_key", shard_id_decoder)
    async def getLogs(self, data, full_shard_key):
        return await self._get_logs(data, full_shard_key, decoder=address_decoder)

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("key", quantity_decoder)
    @decode_arg("block_height", block_height_decoder)
    # TODO: add block number
    async def getStorageAt(self, address, key, block_height=None):
        res = await self.master.get_storage_at(
            Address.deserialize(address), key, block_height
        )
        return data_encoder(res) if res is not None else None

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("block_height", block_height_decoder)
    async def getCode(self, address, block_height=None):
        res = await self.master.get_code(Address.deserialize(address), block_height)
        return data_encoder(res) if res is not None else None

    @public_methods.add
    @decode_arg("full_shard_key", shard_id_decoder)
    @decode_arg("start", data_decoder)
    @decode_arg("limit", quantity_decoder)
    async def getAllTransactions(self, full_shard_key, start="0x", limit="0xa"):
        """ "start" should be the "next" in the response for fetching next page.
        "start" can also be "0x" to fetch from the beginning (i.e., latest).
        """
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        if limit > 20:
            limit = 20
        result = await self.master.get_all_transactions(branch, start, limit)
        if not result:
            return None
        tx_list, next = result
        return {
            "txList": [tx_detail_encoder(tx) for tx in tx_list],
            "next": data_encoder(next),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("start", data_decoder)
    @decode_arg("limit", quantity_decoder)
    @decode_arg("transfer_token_id", quantity_decoder, allow_optional=True)
    async def getTransactionsByAddress(
        self, address, start="0x", limit="0xa", transfer_token_id=None
    ):
        """ "start" should be the "next" in the response for fetching next page.
        "start" can also be "0x" to fetch from the beginning (i.e., latest).
        "start" can be "0x00" to fetch the pending outgoing transactions.
        """
        address = Address.create_from(address)
        if limit > 20:
            limit = 20
        result = await self.master.get_transactions_by_address(
            address, transfer_token_id, start, limit
        )
        if not result:
            return None
        tx_list, next = result
        return {
            "txList": [tx_detail_encoder(tx) for tx in tx_list],
            "next": data_encoder(next),
        }

    @public_methods.add
    async def getJrpcCalls(self):
        return self.counters

    @public_methods.add
    async def gasPrice(self, full_shard_key: str, token_id: Optional[str] = None):
        full_shard_key = shard_id_decoder(full_shard_key)
        if full_shard_key is None:
            return None
        parsed_token_id = (
            quantity_decoder(token_id) if token_id else token_id_encode("QKC")
        )
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        ret = await self.master.gas_price(branch, parsed_token_id)
        if ret is None:
            return None
        return quantity_encoder(ret)

    @public_methods.add
    @decode_arg("full_shard_key", shard_id_decoder)
    @decode_arg("header_hash", hash_decoder)
    @decode_arg("nonce", quantity_decoder)
    @decode_arg("mixhash", hash_decoder)
    @decode_arg("signature", signature_decoder)
    async def submitWork(
        self, full_shard_key, header_hash, nonce, mixhash, signature=None
    ):
        branch = None  # `None` means getting work from root chain
        if full_shard_key is not None:
            branch = Branch(
                self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                    full_shard_key
                )
            )
        return await self.master.submit_work(
            branch, header_hash, nonce, mixhash, signature
        )

    @public_methods.add
    @decode_arg("full_shard_key", shard_id_decoder)
    @decode_arg("coinbase_addr", recipient_decoder, allow_optional=True)
    async def getWork(self, full_shard_key, coinbase_addr=None):
        branch = None  # `None` means getting work from root chain
        if full_shard_key is not None:
            branch = Branch(
                self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                    full_shard_key
                )
            )
        work, optional_divider = await self.master.get_work(branch, coinbase_addr)
        if work is None:
            return None
        ret = [
            data_encoder(work.hash),
            quantity_encoder(work.height),
            quantity_encoder(work.difficulty),
        ]
        if optional_divider is not None:
            ret.append(quantity_encoder(optional_divider))
        return ret

    @public_methods.add
    @decode_arg("block_id", data_decoder)
    async def getRootHashConfirmingMinorBlockById(self, block_id):
        retv = self.master.root_state.db.get_root_block_confirming_minor_block(block_id)
        return data_encoder(retv) if retv else None

    @public_methods.add
    @decode_arg("tx_id", id_decoder)
    async def getTransactionConfirmedByNumberRootBlocks(self, tx_id):
        tx_hash, full_shard_key = tx_id
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        minor_block, i = await self.master.get_transaction_by_hash(tx_hash, branch)
        if not minor_block:
            return None
        confirming_hash = (
            self.master.root_state.db.get_root_block_confirming_minor_block(
                minor_block.header.get_hash()
                + minor_block.header.branch.get_full_shard_id().to_bytes(
                    4, byteorder="big"
                )
            )
        )
        if confirming_hash is None:
            return quantity_encoder(0)
        confirming_header = self.master.root_state.db.get_root_block_header_by_hash(
            confirming_hash
        )
        canonical_hash = self.master.root_state.db.get_root_block_hash_by_height(
            confirming_header.height
        )
        if canonical_hash != confirming_hash:
            return quantity_encoder(0)
        tip = self.master.root_state.tip
        return quantity_encoder(tip.height - confirming_header.height + 1)

    ######################## Ethereum JSON RPC ########################

    @public_methods.add
    async def net_version(self):
        return quantity_encoder(self.master.env.quark_chain_config.NETWORK_ID)

    @public_methods.add
    async def eth_gasPrice(self, shard):
        return await self.gasPrice(shard, quantity_encoder(token_id_encode("QKC")))

    @public_methods.add
    @decode_arg("block_height", block_height_decoder)
    @decode_arg("include_transactions", bool_decoder)
    async def eth_getBlockByNumber(self, block_height, include_transactions):
        """
        NOTE: only support block_id "latest" or hex
        """

        def block_transcoder(block):
            """
            QuarkChain Block => ETH Block
            """
            return {
                **block,
                "number": block["height"],
                "parentHash": block["hashPrevMinorBlock"],
                "sha3Uncles": "",
                "logsBloom": "",
                "transactionsRoot": block["hashMerkleRoot"],  # ?
                "stateRoot": block["hashEvmStateRoot"],  # ?
            }

        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(0)
        )
        block, _ = await self.master.get_minor_block_by_height(
            block_height, branch, need_extra_info=False
        )
        if block is None:
            return None
        return block_transcoder(minor_block_encoder(block))

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("shard", shard_id_decoder)
    @encode_res(quantity_encoder)
    async def eth_getBalance(self, address, shard=None):
        address = Address.deserialize(address)
        if shard is not None:
            address = Address(address.recipient, shard)
        account_branch_data = await self.master.get_primary_account_data(address)
        balance = account_branch_data.token_balances.balance_map.get(
            token_id_encode("QKC"), 0
        )
        return balance

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("shard", shard_id_decoder)
    @encode_res(quantity_encoder)
    async def eth_getTransactionCount(self, address, shard=None):
        address = Address.deserialize(address)
        if shard is not None:
            address = Address(address.recipient, shard)
        account_branch_data = await self.master.get_primary_account_data(address)
        return account_branch_data.transaction_count

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("shard", shard_id_decoder)
    async def eth_getCode(self, address, shard=None):
        addr = Address.deserialize(address)
        if shard is not None:
            addr = Address(addr.recipient, shard)
        res = await self.master.get_code(addr, None)
        return data_encoder(res) if res is not None else None

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def eth_call(self, data, shard=None):
        """Returns the result of the transaction application without putting in block chain"""
        data = self._convert_eth_call_data(data, shard)
        return await self.call(data)

    @public_methods.add
    async def eth_sendRawTransaction(self, tx_data):
        return await self.sendRawTransaction(tx_data)

    @public_methods.add
    async def eth_getTransactionReceipt(self, tx_id):
        return await self.getTransactionReceipt(tx_id)

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def eth_estimateGas(self, data, shard):
        data = self._convert_eth_call_data(data, shard)
        return await self.estimateGas(**data)

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def eth_getLogs(self, data, shard):
        return await self._get_logs(
            data, shard, decoder=eth_address_to_quarkchain_address_decoder
        )

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("key", quantity_decoder)
    @decode_arg("shard", shard_id_decoder)
    async def eth_getStorageAt(self, address, key, shard=None):
        addr = Address.deserialize(address)
        if shard is not None:
            addr = Address(addr.recipient, shard)
        res = await self.master.get_storage_at(addr, key, None)
        return data_encoder(res) if res is not None else None

    ######################## Private Methods ########################

    @private_methods.add
    @decode_arg("branch", quantity_decoder)
    @decode_arg("block_data", data_decoder)
    async def addBlock(self, branch, block_data):
        if branch == 0:
            block = RootBlock.deserialize(block_data)
            return await self.master.add_root_block_from_miner(block)
        return await self.master.add_raw_minor_block(Branch(branch), block_data)

    @private_methods.add
    async def getPeers(self):
        peer_list = []
        for peer_id, peer in self.master.network.active_peer_pool.items():
            peer_list.append(
                {
                    "id": data_encoder(peer_id),
                    "ip": quantity_encoder(int(peer.ip)),
                    "port": quantity_encoder(peer.port),
                }
            )
        return {"peers": peer_list}

    @private_methods.add
    async def getSyncStats(self):
        return self.master.synchronizer.get_stats()

    @private_methods.add
    async def getStats(self):
        # This JRPC doesn't follow the standard encoding
        return await self.master.get_stats()

    @private_methods.add
    async def getBlockCount(self):
        # This JRPC doesn't follow the standard encoding
        return self.master.get_block_count()

    @private_methods.add
    async def createTransactions(self, **load_test_data):
        """Create transactions for load testing"""

        def get_data_default(key, decoder, default=None):
            if key in load_test_data:
                return decoder(load_test_data[key])
            return default

        num_tx_per_shard = load_test_data["numTxPerShard"]
        x_shard_percent = load_test_data["xShardPercent"]
        to = get_data_default("to", recipient_decoder, b"")
        startgas = get_data_default("gas", quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default(
            "gasPrice", quantity_decoder, int(DEFAULT_GASPRICE / 10)
        )
        value = get_data_default("value", quantity_decoder, 0)
        data = get_data_default("data", data_decoder, b"")
        # FIXME: can't support specifying full shard ID to 0. currently is regarded as not set
        from_full_shard_key = get_data_default(
            "fromFullShardKey", full_shard_key_decoder, 0
        )
        gas_token_id = get_data_default(
            "gas_token_id", quantity_decoder, self.env.quark_chain_config.genesis_token
        )
        transfer_token_id = get_data_default(
            "transfer_token_id",
            quantity_decoder,
            self.env.quark_chain_config.genesis_token,
        )
        # build sample tx
        evm_tx_sample = EvmTransaction(
            0,
            gasprice,
            startgas,
            to,
            value,
            data,
            from_full_shard_key=from_full_shard_key,
            gas_token_id=gas_token_id,
            transfer_token_id=transfer_token_id,
        )
        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx_sample))
        return await self.master.create_transactions(
            num_tx_per_shard, x_shard_percent, tx
        )

    @private_methods.add
    async def setTargetBlockTime(self, root_block_time=0, minor_block_time=0):
        """0 will not update existing value"""
        return await self.master.set_target_block_time(
            root_block_time, minor_block_time
        )

    @public_methods.add
    @decode_arg("block_id", id_decoder)
    @decode_arg("root_block_id", data_decoder, allow_optional=True)
    @decode_arg("token_id", quantity_decoder)  # default: QKC
    @decode_arg("start", data_decoder, allow_optional=True)
    @decode_arg("limit", quantity_decoder)
    async def getTotalBalance(
        self, block_id, root_block_id=None, token_id="0x8bb0", start=None, limit="0x64"
    ):
        if limit > 10000:
            limit = 10000
        block_hash, full_shard_key = block_id
        full_shard_id = (
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        try:
            result = await self.master.get_total_balance(
                Branch(full_shard_id), block_hash, root_block_id, token_id, start, limit
            )
        except:
            raise ServerError
        if not result:
            raise InvalidRequest
        total_balance, next_start = result
        return {
            "totalBalance": quantity_encoder(total_balance),
            "next": data_encoder(next_start),
        }

    @private_methods.add
    async def setMining(self, mining):
        """Turn on / off mining"""
        return await self.master.set_mining(mining)

    @private_methods.add
    async def getJrpcCalls(self):
        return self.counters

    @private_methods.add
    async def getKadRoutingTableSize(self):
        """Returns number of nodes in the p2p discovery routing table"""
        if not isinstance(self.master.network, P2PManager):
            raise InvalidRequest("network is not P2P")
        return len(self.master.network.server.discovery.proto.routing)

    @private_methods.add
    async def getKadRoutingTable(self):
        """returns a list of nodes in the p2p discovery routing table, in the enode format
        eg. "enode://PUBKEY@IP:PORT"
        """
        if not isinstance(self.master.network, P2PManager):
            raise InvalidRequest("network is not P2P")
        return [n.to_uri() for n in self.master.network.server.discovery.proto.routing]

    @public_methods.add
    async def getTotalSupply(self):
        total_supply = self.master.get_total_supply()
        return quantity_encoder(total_supply) if total_supply else None

    @staticmethod
    def _convert_eth_call_data(data, shard):
        to_address = Address.create_from(
            eth_address_to_quarkchain_address_decoder(data["to"])
        )
        if shard:
            to_address = Address(to_address.recipient, shard)
        data["to"] = "0x" + to_address.serialize().hex()
        if "from" in data:
            from_address = Address.create_from(
                eth_address_to_quarkchain_address_decoder(data["from"])
            )
            if shard:
                from_address = Address(from_address.recipient, shard)
            data["from"] = "0x" + from_address.serialize().hex()
        return data

    async def _get_logs(self, data, full_shard_key, decoder: Callable[[str], bytes]):
        start_block = block_height_decoder(data.get("fromBlock", "latest"))
        end_block = block_height_decoder(data.get("toBlock", "latest"))
        addresses, topics = _parse_log_request(data, decoder)
        if full_shard_key is None:
            raise InvalidParams("Full shard key is required to get logs")
        addresses = [Address(a.recipient, full_shard_key) for a in addresses]
        branch = Branch(
            self.master.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                full_shard_key
            )
        )
        logs = await self.master.get_logs(
            addresses, topics, start_block, end_block, branch
        )
        if logs is None:
            return None
        return loglist_encoder(logs)

    async def _call_or_estimate_gas(self, is_call: bool, **data):
        """Returns the result of the transaction application without putting in block chain"""
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        gas = get_data_default("gas", quantity_decoder, 0)
        gas_price = get_data_default("gasPrice", quantity_decoder, 0)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")
        sender = get_data_default("from", address_decoder, b"\x00" * 20 + to[20:])
        sender_address = Address.create_from(sender)
        from_full_shard_key = sender_address.full_shard_key
        to = get_data_default("to", address_decoder, None)
        if to is None:
            to_full_shard_key = 0
            to = b""
        else:
            to_full_shard_key = int.from_bytes(to[20:], "big")
            to = to[:20]
        gas_token_id = get_data_default(
            "gas_token_id", quantity_decoder, self.env.quark_chain_config.genesis_token
        )
        transfer_token_id = get_data_default(
            "transfer_token_id",
            quantity_decoder,
            self.env.quark_chain_config.genesis_token,
        )

        network_id = self.master.env.quark_chain_config.NETWORK_ID

        nonce = 0  # slave will fill in the real nonce
        evm_tx = EvmTransaction(
            nonce,
            gas_price,
            gas,
            to,
            value,
            data_,
            from_full_shard_key=from_full_shard_key,
            to_full_shard_key=to_full_shard_key,
            network_id=network_id,
            gas_token_id=gas_token_id,
            transfer_token_id=transfer_token_id,
        )

        tx = TypedTransaction(SerializedEvmTransaction.from_evm_tx(evm_tx))
        if is_call:
            # xshard not supported for now
            is_same_shard = self.master.env.quark_chain_config.is_same_full_shard(
                to_full_shard_key, from_full_shard_key
            )
            if not is_same_shard:
                raise InvalidParams("Call cross-shard tx not supported yet")
            res = await self.master.execute_transaction(
                tx, sender_address, data["block_height"]
            )
            return data_encoder(res) if res is not None else None
        else:  # estimate gas
            res = await self.master.estimate_gas(tx, sender_address)
            return quantity_encoder(res) if res is not None else None


class JSONRPCWebsocketServer:
    @classmethod
    def start_websocket_server(cls, env, slave_server):
        server = cls(
            env,
            slave_server,
            env.slave_config.WEBSOCKET_JSON_RPC_PORT,
            env.slave_config.HOST,
            public_methods,
        )
        server.start()
        return server

    def __init__(
        self, env, slave_server: SlaveServer, port, host, methods: AsyncMethods
    ):
        self.loop = asyncio.get_event_loop()
        self.port = port
        self.host = host
        self.env = env
        self.slave = slave_server
        self.counters = dict()
        self.pending_tx_cache = LRUCache(maxsize=1024)

        # Bind RPC handler functions to this instance
        self.handlers = AsyncMethods()
        for rpc_name in methods:
            func = methods[rpc_name]
            self.handlers[rpc_name] = func.__get__(self, self.__class__)

        self.shard_subscription_managers = self.slave.shard_subscription_managers

    async def __handle(self, websocket, path):
        sub_ids = dict()  # per-websocket var, Dict[sub_id, full_shard_id]
        try:
            async for message in websocket:
                Logger.info(message)

                d = dict()
                try:
                    d = json.loads(message)
                except Exception:
                    raise InvalidParams("Cannot parse message as JSON")
                method = d.get("method", "null")
                if method in self.counters:
                    self.counters[method] += 1
                else:
                    self.counters[method] = 1
                msg_id = d.get("id", 0)

                response = await self.handlers.dispatch(
                    message,
                    context={
                        "websocket": websocket,
                        "msg_id": msg_id,
                        "sub_ids": sub_ids,
                    },
                )

                if "error" in response:
                    Logger.error(response)
                else:
                    if method == "subscribe":
                        sub_id = response["result"]
                        full_shard_id = shard_id_decoder(d.get("params")[1])
                        sub_ids[sub_id] = full_shard_id
                    elif method == "unsubscribe":
                        sub_id = d.get("params")[0]
                        del sub_ids[sub_id]
                if not response.is_notification:
                    await websocket.send(json.dumps(response))
        finally:  # current websocket connection terminates, remove subscribers in this connection
            for sub_id, full_shard_id in sub_ids.items():
                try:
                    shard_subscription_manager = self.shard_subscription_managers[
                        full_shard_id
                    ]
                    shard_subscription_manager.remove_subscriber(sub_id)
                except:
                    pass

    def start(self):
        start_server = websockets.serve(self.__handle, self.host, self.port)
        self.loop.run_until_complete(start_server)

    def shutdown(self):
        pass  # TODO

    @staticmethod
    def response_transcoder(sub_id, result):
        return {
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {"subscription": sub_id, "result": result},
        }

    @public_methods.add
    async def subscribe(self, sub_type, full_shard_id, params=None, context=None):
        assert context is not None
        full_shard_id = shard_id_decoder(full_shard_id)
        if full_shard_id is None:
            raise InvalidParams("Invalid full shard ID")
        branch = Branch(full_shard_id)
        shard = self.slave.shards.get(branch, None)
        if not shard:
            raise InvalidParams("Full shard ID not found")

        websocket = context["websocket"]
        sub_id = "0x" + uuid.uuid4().hex
        shard_subscription_manager = self.shard_subscription_managers[full_shard_id]

        extra = None
        if sub_type == SUB_LOGS:
            addresses, topics = _parse_log_request(params, address_decoder)
            addresses = [Address(a.recipient, full_shard_id) for a in addresses]
            extra = lambda candidate_blocks: LogFilter.create_from_block_candidates(
                shard.state.db, addresses, topics, candidate_blocks
            )

        shard_subscription_manager.add_subscriber(sub_type, sub_id, websocket, extra)
        return sub_id

    @public_methods.add
    async def unsubscribe(self, sub_id, context=None):
        sub_ids = context["sub_ids"]
        assert context is not None
        if sub_id not in sub_ids:
            raise InvalidParams("Subscription ID not found")

        full_shard_id = sub_ids[sub_id]
        shard_subscription_manager = self.shard_subscription_managers[full_shard_id]
        shard_subscription_manager.remove_subscriber(sub_id)

        return True
