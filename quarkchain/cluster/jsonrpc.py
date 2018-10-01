import asyncio
import inspect
import json
from typing import List, Callable

import aiohttp_cors
import rlp
from aiohttp import web
from async_armor import armor
from decorator import decorator
from jsonrpcserver import config
from jsonrpcserver.async_methods import AsyncMethods
from jsonrpcserver.exceptions import InvalidParams

from quarkchain.cluster.master import MasterServer
from quarkchain.core import Address, Branch, Code, Transaction, Log
from quarkchain.core import RootBlock, TransactionReceipt, MinorBlock
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.evm.utils import is_numeric, denoms
from quarkchain.utils import Logger

# defaults
DEFAULT_STARTGAS = 100 * 1000
DEFAULT_GASPRICE = 10 * denoms.gwei


# Allow 16 MB request for submitting big blocks
# TODO: revisit this parameter
JSON_RPC_CLIENT_REQUEST_MAX_SIZE = 16 * 1024 * 1024


# Disable jsonrpcserver logging
config.log_requests = False
config.log_responses = False


def quantity_decoder(hex_str):
    """Decode `hexStr` representing a quantity."""
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


def data_decoder(hex_str):
    """Decode `hexStr` representing unformatted hex_str."""
    if not hex_str.startswith("0x"):
        raise InvalidParams("Invalid hex_str encoding")
    try:
        return bytes.fromhex(hex_str[2:])
    except Exception:
        raise InvalidParams("Invalid hex_str hex encoding")


def data_encoder(data_bytes):
    """Encode unformatted binary `dataBytes`.
    """
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


def recipient_decoder(hex_str):
    """Decode an recipient from hex with 0x prefix to 20 bytes."""
    recipient_bytes = data_decoder(hex_str)
    if len(recipient_bytes) not in (20, 0):
        raise InvalidParams("Addresses must be 20 or 0 bytes long")
    return recipient_bytes


def recipient_encoder(recipient_bytes):
    assert len(recipient_bytes) == 20
    return data_encoder(recipient_bytes)


def full_shard_id_decoder(hex_str):
    b = data_decoder(hex_str)
    if len(b) != 4:
        raise InvalidParams("Full shard id must be 4 bytes")
    return int.from_bytes(b, byteorder="big")


def full_shard_id_encoder(full_shard_id):
    return data_encoder(full_shard_id.to_bytes(4, byteorder="big"))


def id_encoder(hash_bytes, full_shard_id):
    """ Encode hash and full_shard_id into hex """
    return data_encoder(hash_bytes + full_shard_id.to_bytes(4, byteorder="big"))


def id_decoder(hex_str):
    """ Decode an id to (hash, full_shard_id) """
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


def bool_decoder(data):
    if not isinstance(data, bool):
        raise InvalidParams("Parameter must be boolean")
    return data


def root_block_encoder(block):
    header = block.header

    d = {
        "id": data_encoder(header.get_hash()),
        "height": quantity_encoder(header.height),
        "hash": data_encoder(header.get_hash()),
        "hashPrevBlock": data_encoder(header.hash_prev_block),
        "idPrevBlock": data_encoder(header.hash_prev_block),
        "nonce": quantity_encoder(header.nonce),
        "hashMerkleRoot": data_encoder(header.hash_merkle_root),
        "miner": address_encoder(header.coinbase_address.serialize()),
        "difficulty": quantity_encoder(header.difficulty),
        "timestamp": quantity_encoder(header.create_time),
        "size": quantity_encoder(len(block.serialize())),
        "minorBlockHeaders": [],
    }

    for header in block.minor_block_header_list:
        h = {
            "id": id_encoder(header.get_hash(), header.branch.get_shard_id()),
            "height": quantity_encoder(header.height),
            "hash": data_encoder(header.get_hash()),
            "branch": quantity_encoder(header.branch.value),
            "shard": quantity_encoder(header.branch.get_shard_id()),
            "hashPrevMinorBlock": data_encoder(header.hash_prev_minor_block),
            "idPrevMinorBlock": id_encoder(
                header.hash_prev_minor_block, header.branch.get_shard_id()
            ),
            "hashPrevRootBlock": data_encoder(header.hash_prev_root_block),
            "nonce": quantity_encoder(header.nonce),
            "difficulty": quantity_encoder(header.difficulty),
            "timestamp": quantity_encoder(header.create_time),
        }
        d["minorBlockHeaders"].append(h)
    return d


def minor_block_encoder(block, include_transactions=False):
    """Encode a block as JSON object.

    :param block: a :class:`ethereum.block.Block`
    :param include_transactions: if true transactions are included, otherwise
                                 only their hashes
    :returns: a json encodable dictionary
    """
    header = block.header
    meta = block.meta

    d = {
        "id": id_encoder(header.get_hash(), header.branch.get_shard_id()),
        "height": quantity_encoder(header.height),
        "hash": data_encoder(header.get_hash()),
        "branch": quantity_encoder(header.branch.value),
        "shard": quantity_encoder(header.branch.get_shard_id()),
        "hashPrevMinorBlock": data_encoder(header.hash_prev_minor_block),
        "idPrevMinorBlock": id_encoder(
            header.hash_prev_minor_block, header.branch.get_shard_id()
        ),
        "hashPrevRootBlock": data_encoder(header.hash_prev_root_block),
        "nonce": quantity_encoder(header.nonce),
        "hashMerkleRoot": data_encoder(meta.hash_merkle_root),
        "hashEvmStateRoot": data_encoder(meta.hash_evm_state_root),
        "miner": address_encoder(meta.coinbase_address.serialize()),
        "difficulty": quantity_encoder(header.difficulty),
        "extraData": data_encoder(meta.extra_data),
        "gasLimit": quantity_encoder(header.evm_gas_limit),
        "gasUsed": quantity_encoder(meta.evm_gas_used),
        "timestamp": quantity_encoder(header.create_time),
        "size": quantity_encoder(len(block.serialize())),
    }
    if include_transactions:
        d["transactions"] = []
        for i, _ in enumerate(block.tx_list):
            d["transactions"].append(tx_encoder(block, i))
    else:
        d["transactions"] = [
            id_encoder(tx.get_hash(), block.header.branch.get_shard_id())
            for tx in block.tx_list
        ]
    return d


def tx_encoder(block, i):
    """Encode a transaction as JSON object.

    `transaction` is the `i`th transaction in `block`.
    """
    tx = block.tx_list[i]
    evm_tx = tx.code.get_evm_transaction()
    # TODO: shard_mask is wrong when the tx is pending and block is fake
    shard_mask = block.header.branch.get_shard_size() - 1
    return {
        "id": id_encoder(tx.get_hash(), evm_tx.from_full_shard_id),
        "hash": data_encoder(tx.get_hash()),
        "nonce": quantity_encoder(evm_tx.nonce),
        "timestamp": quantity_encoder(block.header.create_time),
        "shard": quantity_encoder(block.header.branch.get_shard_id()),
        "blockId": id_encoder(
            block.header.get_hash(), block.header.branch.get_shard_id()
        ),
        "blockHeight": quantity_encoder(block.header.height),
        "transactionIndex": quantity_encoder(i),
        "from": data_encoder(evm_tx.sender),
        "to": data_encoder(evm_tx.to),
        "fromFullShardId": full_shard_id_encoder(evm_tx.from_full_shard_id),
        "toFullShardId": full_shard_id_encoder(evm_tx.to_full_shard_id),
        "fromShardId": quantity_encoder(evm_tx.from_full_shard_id & shard_mask),
        "toShardId": quantity_encoder(evm_tx.to_full_shard_id & shard_mask),
        "value": quantity_encoder(evm_tx.value),
        "gasPrice": quantity_encoder(evm_tx.gasprice),
        "gas": quantity_encoder(evm_tx.startgas),
        "data": data_encoder(evm_tx.data),
        "network_id": quantity_encoder(evm_tx.network_id),
        "r": quantity_encoder(evm_tx.r),
        "s": quantity_encoder(evm_tx.s),
        "v": quantity_encoder(evm_tx.v),
    }


def loglist_encoder(loglist: List[Log]):
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
            }
        )
    return result


def receipt_encoder(block: MinorBlock, i: int, receipt: TransactionReceipt):
    tx = block.tx_list[i]
    evm_tx = tx.code.get_evm_transaction()
    resp = {
        "transactionId": id_encoder(tx.get_hash(), evm_tx.from_full_shard_id),
        "transactionHash": data_encoder(tx.get_hash()),
        "transactionIndex": quantity_encoder(i),
        "blockId": id_encoder(
            block.header.get_hash(), block.header.branch.get_shard_id()
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
    }

    return resp


def decode_arg(name, decoder):
    """Create a decorator that applies `decoder` to argument `name`."""

    @decorator
    def new_f(f, *args, **kwargs):
        call_args = inspect.getcallargs(f, *args, **kwargs)
        call_args[name] = decoder(call_args[name])
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
    full_shard_id_hex = ""
    for i in range(4):
        index = i * 10
        full_shard_id_hex += eth_hex[index : index + 2]
    return address_decoder("0x" + eth_hex + full_shard_id_hex)


public_methods = AsyncMethods()
private_methods = AsyncMethods()


# noinspection PyPep8Naming
class JSONRPCServer:
    @classmethod
    def start_public_server(cls, env, master_server):
        server = cls(
            env, master_server, env.cluster_config.JSON_RPC_PORT, public_methods
        )
        server.start()
        return server

    @classmethod
    def start_private_server(cls, env, master_server):
        server = cls(
            env,
            master_server,
            env.cluster_config.PRIVATE_JSON_RPC_PORT,
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
        server = cls(env, master_server, env.cluster_config.JSON_RPC_PORT, methods)
        server.start()
        return server

    def __init__(self, env, master_server: MasterServer, port, methods: AsyncMethods):
        self.loop = asyncio.get_event_loop()
        self.port = port
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
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
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
            "shardSize": quantity_encoder(self.master.get_shard_size()),
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
    async def getBalance(self, address, block_height=None):
        account_branch_data = await self.master.get_primary_account_data(
            Address.deserialize(address), block_height
        )
        branch = account_branch_data.branch
        balance = account_branch_data.balance
        return {
            "branch": quantity_encoder(branch.value),
            "shard": quantity_encoder(branch.get_shard_id()),
            "balance": quantity_encoder(balance),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("block_height", block_height_decoder)
    async def getAccountData(self, address, block_height=None, include_shards=False):
        # do not allow specify height if client wants info on all shards
        if include_shards and block_height is not None:
            return None

        address = Address.deserialize(address)
        if not include_shards:
            account_branch_data = await self.master.get_primary_account_data(
                address, block_height
            )
            branch = account_branch_data.branch
            balance = account_branch_data.balance
            count = account_branch_data.transaction_count
            primary = {
                "branch": quantity_encoder(branch.value),
                "shard": quantity_encoder(branch.get_shard_id()),
                "balance": quantity_encoder(balance),
                "transactionCount": quantity_encoder(count),
                "isContract": account_branch_data.is_contract,
            }
            return {"primary": primary}

        branch_to_account_branch_data = await self.master.get_account_data(address)
        shard_size = self.master.get_shard_size()

        shards = []
        for shard in range(shard_size):
            branch = Branch.create(shard_size, shard)
            account_branch_data = branch_to_account_branch_data[branch]
            data = {
                "branch": quantity_encoder(account_branch_data.branch.value),
                "shard": quantity_encoder(account_branch_data.branch.get_shard_id()),
                "balance": quantity_encoder(account_branch_data.balance),
                "transactionCount": quantity_encoder(
                    account_branch_data.transaction_count
                ),
                "isContract": account_branch_data.is_contract,
            }
            shards.append(data)

            if shard == address.get_shard_id(shard_size):
                primary = data

        return {"primary": primary, "shards": shards}

    @public_methods.add
    async def sendUnsigedTransaction(self, **data):
        """ Returns the unsigned hash of the evm transaction """
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        nonce = get_data_default("nonce", quantity_decoder, None)
        to = get_data_default("to", recipient_decoder, b"")
        startgas = get_data_default("gas", quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default("gasPrice", quantity_decoder, DEFAULT_GASPRICE)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")

        from_full_shard_id = get_data_default(
            "fromFullShardId", full_shard_id_decoder, None
        )
        to_full_shard_id = get_data_default(
            "toFullShardId", full_shard_id_decoder, None
        )

        if nonce is None:
            raise InvalidParams("nonce is missing")
        if from_full_shard_id is None:
            raise InvalidParams("fromFullShardId is missing")

        if to_full_shard_id is None:
            to_full_shard_id = from_full_shard_id

        evm_tx = EvmTransaction(
            nonce,
            gasprice,
            startgas,
            to,
            value,
            data_,
            from_full_shard_id=from_full_shard_id,
            to_full_shard_id=to_full_shard_id,
            network_id=self.master.env.quark_chain_config.NETWORK_ID,
        )

        return {
            "txHashUnsigned": data_encoder(evm_tx.hash_unsigned),
            "nonce": quantity_encoder(evm_tx.nonce),
            "to": data_encoder(evm_tx.to),
            "fromFullShardId": full_shard_id_encoder(evm_tx.from_full_shard_id),
            "toFullShardId": full_shard_id_encoder(evm_tx.to_full_shard_id),
            "value": quantity_encoder(evm_tx.value),
            "gasPrice": quantity_encoder(evm_tx.gasprice),
            "gas": quantity_encoder(evm_tx.startgas),
            "data": data_encoder(evm_tx.data),
            "networkId": quantity_encoder(evm_tx.network_id),
        }

    @public_methods.add
    async def sendTransaction(self, **data):
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

        to_full_shard_id = get_data_default(
            "toFullShardId", full_shard_id_decoder, None
        )
        from_full_shard_id = get_data_default(
            "fromFullShardId", full_shard_id_decoder, None
        )
        network_id = get_data_default(
            "networkId", quantity_decoder, self.master.env.quark_chain_config.NETWORK_ID
        )

        if nonce is None:
            raise InvalidParams("Missing nonce")
        if not (v and r and s):
            raise InvalidParams("Missing v, r, s")
        if from_full_shard_id is None:
            raise InvalidParams("Missing fromFullShardId")

        if to_full_shard_id is None:
            to_full_shard_id = from_full_shard_id

        evm_tx = EvmTransaction(
            nonce,
            gasprice,
            startgas,
            to,
            value,
            data_,
            v,
            r,
            s,
            from_full_shard_id=from_full_shard_id,
            to_full_shard_id=to_full_shard_id,
            network_id=network_id,
        )
        tx = Transaction(code=Code.create_evm_code(evm_tx))
        success = await self.master.add_transaction(tx)
        if not success:
            return None

        return id_encoder(tx.get_hash(), from_full_shard_id)

    @public_methods.add
    @decode_arg("tx_data", data_decoder)
    async def sendRawTransaction(self, tx_data):
        evm_tx = rlp.decode(tx_data, EvmTransaction)
        tx = Transaction(code=Code.create_evm_code(evm_tx))
        success = await self.master.add_transaction(tx)
        if not success:
            return "0x" + bytes(32 + 4).hex()
        return id_encoder(tx.get_hash(), evm_tx.from_full_shard_id)

    @public_methods.add
    @decode_arg("block_id", data_decoder)
    async def getRootBlockById(self, block_id):
        try:
            block = self.master.root_state.db.get_root_block_by_hash(block_id, False)
            return root_block_encoder(block)
        except Exception:
            return None

    @public_methods.add
    @decode_arg("height", quantity_decoder)
    async def getRootBlockByHeight(self, height):
        block = self.master.root_state.get_root_block_by_height(height)
        if not block:
            return None
        return root_block_encoder(block)

    @public_methods.add
    @decode_arg("block_id", id_decoder)
    @decode_arg("include_transactions", bool_decoder)
    async def getMinorBlockById(self, block_id, include_transactions=False):
        block_hash, full_shard_id = block_id
        shard_size = self.master.get_shard_size()
        branch = Branch.create(shard_size, (shard_size - 1) & full_shard_id)
        block = await self.master.get_minor_block_by_hash(block_hash, branch)
        if not block:
            return None
        return minor_block_encoder(block, include_transactions)

    @public_methods.add
    @decode_arg("shard", quantity_decoder)
    @decode_arg("include_transactions", bool_decoder)
    async def getMinorBlockByHeight(
        self, shard: int, height=None, include_transactions=False
    ):
        shard_size = self.master.get_shard_size()
        if height is not None:
            height = quantity_decoder(height)
        if shard >= shard_size:
            raise InvalidParams(
                "shard is larger than shard size {} > {}".format(shard, shard_size)
            )
        branch = Branch.create(shard_size, shard)
        block = await self.master.get_minor_block_by_height(height, branch)
        if not block:
            return None
        return minor_block_encoder(block, include_transactions)

    @public_methods.add
    @decode_arg("tx_id", id_decoder)
    async def getTransactionById(self, tx_id):
        tx_hash, full_shard_id = tx_id
        shard_size = self.master.get_shard_size()
        branch = Branch.create(shard_size, (shard_size - 1) & full_shard_id)
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
    async def estimateGas(self, **data):
        return await self._call_or_estimate_gas(is_call=False, **data)

    @public_methods.add
    @decode_arg("tx_id", id_decoder)
    async def getTransactionReceipt(self, tx_id):
        tx_hash, full_shard_id = tx_id
        shard_size = self.master.get_shard_size()
        branch = Branch.create(shard_size, (shard_size - 1) & full_shard_id)
        resp = await self.master.get_transaction_receipt(tx_hash, branch)
        if not resp:
            return None
        minor_block, i, receipt = resp

        return receipt_encoder(minor_block, i, receipt)

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def getLogs(self, data, shard):
        return await self._get_logs(data, shard, decoder=address_decoder)

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
    @decode_arg("address", address_decoder)
    @decode_arg("start", data_decoder)
    @decode_arg("limit", quantity_decoder)
    async def getTransactionsByAddress(self, address, start="0x", limit="0xa"):
        """ "start" should be the "next" in the response for fetching next page.
            "start" can also be "0x" to fetch from the beginning (i.e., latest).
            "start" can be "0x00" to fetch the pending outgoing transactions.
        """
        address = Address.create_from(address)
        if limit > 20:
            limit = 20
        result = await self.master.get_transactions_by_address(address, start, limit)
        if not result:
            return None
        tx_list, next = result
        txs = []
        for tx in tx_list:
            txs.append(
                {
                    "txId": id_encoder(tx.tx_hash, tx.from_address.full_shard_id),
                    "fromAddress": address_encoder(tx.from_address.serialize()),
                    "toAddress": address_encoder(tx.to_address.serialize())
                    if tx.to_address
                    else "0x",
                    "value": quantity_encoder(tx.value),
                    "blockHeight": quantity_encoder(tx.block_height),
                    "timestamp": quantity_encoder(tx.timestamp),
                    "success": tx.success,
                }
            )
        return {"txList": txs, "next": data_encoder(next)}

    @public_methods.add
    async def getJrpcCalls(self):
        return self.counters

    @public_methods.add
    async def gasPrice(self, shard):
        shard = shard_id_decoder(shard)
        if shard is None:
            return None
        branch = Branch.create(self.master.get_shard_size(), shard)
        ret = await self.master.gas_price(branch)
        if ret is None:
            return None
        return quantity_encoder(ret)

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    @decode_arg("header_hash", hash_decoder)
    @decode_arg("nonce", quantity_decoder)
    @decode_arg("mixhash", hash_decoder)
    async def submitWork(self, shard, header_hash, nonce, mixhash):
        # TODO: to be implemented
        return False

    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def getWork(self, shard):
        # TODO: to be implemented
        return None

    @decode_arg("from_address", address_decoder)
    @decode_arg("to_address", address_decoder)
    async def donate(self, from_address, to_address):
        """Faucet function to send 1 token from from_address to to_address.
        from_address must be one of the addresses in genesis_data/alloc.json.
        Only allow one pending tx at a time.
        Return tx id if success else None
        """
        key = self.master.env.quark_chain_config.alloc_accounts.get(
            from_address.hex(), None
        )
        if not key:
            return None

        from_address = Address.deserialize(from_address)
        to_address = Address.deserialize(to_address)

        # Do nothing if there is already a pending tx
        result = await self.master.get_transactions_by_address(
            from_address, bytes(1), 1
        )
        if result:
            tx_list, next_token = result
            if tx_list:
                return None

        account_branch_data = await self.master.get_primary_account_data(from_address)
        nonce = account_branch_data.transaction_count
        network_id = self.master.env.quark_chain_config.NETWORK_ID
        evm_tx = EvmTransaction(
            nonce,
            10 ** 9,
            30000,
            to_address.recipient,
            1 * (10 ** 18),
            b"",
            from_full_shard_id=from_address.full_shard_id,
            to_full_shard_id=to_address.full_shard_id,
            network_id=network_id,
        )
        evm_tx.sign(key)
        tx = Transaction(code=Code.create_evm_code(evm_tx))
        success = await self.master.add_transaction(tx)
        if not success:
            return None
        return id_encoder(tx.get_hash(), from_address.full_shard_id)

    ######################## Ethereum JSON RPC ########################

    @public_methods.add
    async def net_version(self):
        return quantity_encoder(self.master.env.quark_chain_config.NETWORK_ID)

    @public_methods.add
    async def eth_gasPrice(self, shard):
        return await self.gasPrice(shard)

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

        block = await self.master.get_minor_block_by_height(
            block_height, Branch.create(self.master.get_shard_size(), 0)
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
        balance = account_branch_data.balance
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
        """ Returns the result of the transaction application without putting in block chain """
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
    @decode_arg("coinbase_address", address_decoder)
    @decode_arg("shard_mask_value", quantity_decoder)
    async def getNextBlockToMine(
        self, coinbase_address, shard_mask_value, prefer_root=False
    ):
        address = Address.deserialize(coinbase_address)
        is_root_block, block = await self.master.get_next_block_to_mine(
            address, shard_mask_value, prefer_root=prefer_root
        )
        if not block:
            return None
        return {
            "isRootBlock": is_root_block,
            "blockData": data_encoder(block.serialize()),
        }

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
    async def getStats(self):
        # This JRPC doesn't follow the standard encoding
        return await self.master.get_stats()

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
        from_full_shard_id = get_data_default(
            "fromFullShardId", full_shard_id_decoder, 0
        )
        # build sample tx
        evm_tx_sample = EvmTransaction(
            0,
            gasprice,
            startgas,
            to,
            value,
            data,
            from_full_shard_id=from_full_shard_id,
        )
        tx = Transaction(code=Code.create_evm_code(evm_tx_sample))
        return await self.master.create_transactions(
            num_tx_per_shard, x_shard_percent, tx
        )

    @private_methods.add
    async def setTargetBlockTime(self, root_block_time=0, minor_block_time=0):
        """0 will not update existing value"""
        return await self.master.set_target_block_time(
            root_block_time, minor_block_time
        )

    @private_methods.add
    async def setMining(self, mining):
        """Turn on / off mining"""
        return await self.master.set_mining(mining)

    @private_methods.add
    async def getJrpcCalls(self):
        return self.counters

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

    async def _get_logs(self, data, shard, decoder: Callable[[str], bytes]):
        start_block = data.get("fromBlock", "latest")
        end_block = data.get("toBlock", "latest")
        # TODO: not supported yet for "earliest" or "pending" block
        if (isinstance(start_block, str) and start_block != "latest") or (
            isinstance(end_block, str) and end_block != "latest"
        ):
            return None
        # parse addresses / topics
        addresses, topics = [], []
        if "address" in data:
            if isinstance(data["address"], str):
                addresses = [Address.deserialize(decoder(data["address"]))]
            elif isinstance(data["address"], list):
                addresses = [Address.deserialize(decoder(a)) for a in data["address"]]
        if shard is not None:
            addresses = [Address(a.recipient, shard) for a in addresses]
        if "topics" in data:
            for topic_item in data["topics"]:
                if isinstance(topic_item, str):
                    topics.append([data_decoder(topic_item)])
                elif isinstance(topic_item, list):
                    topics.append([data_decoder(tp) for tp in topic_item])
        branch = Branch.create(self.master.get_shard_size(), shard)
        logs = await self.master.get_logs(
            addresses, topics, start_block, end_block, branch
        )
        if logs is None:
            return None
        return loglist_encoder(logs)

    async def _call_or_estimate_gas(self, is_call: bool, **data):
        """ Returns the result of the transaction application without putting in block chain """
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        to = get_data_default("to", address_decoder, None)
        if to is None:
            raise InvalidParams("Missing to")

        to_full_shard_id = int.from_bytes(to[20:], "big")

        gas = get_data_default("gas", quantity_decoder, 0)
        gas_price = get_data_default("gasPrice", quantity_decoder, 0)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")
        sender = get_data_default("from", address_decoder, b"\x00" * 20 + to[20:])
        sender_address = Address.create_from(sender)

        network_id = self.master.env.quark_chain_config.NETWORK_ID

        nonce = 0  # slave will fill in the real nonce
        evm_tx = EvmTransaction(
            nonce,
            gas_price,
            gas,
            to[:20],
            value,
            data_,
            from_full_shard_id=sender_address.full_shard_id,
            to_full_shard_id=to_full_shard_id,
            network_id=network_id,
        )

        tx = Transaction(code=Code.create_evm_code(evm_tx))
        if is_call:
            res = await self.master.execute_transaction(
                tx, sender_address, data["block_height"]
            )
            return data_encoder(res) if res is not None else None
        else:  # estimate gas
            res = await self.master.estimate_gas(tx, sender_address)
            return quantity_encoder(res) if res is not None else None
