import asyncio
import inspect
import json

import aiohttp_cors
import rlp
from aiohttp import web
from async_armor import armor
from decorator import decorator
from jsonrpcserver import config
from jsonrpcserver.async_methods import AsyncMethods
from jsonrpcserver.exceptions import InvalidParams

from quarkchain.evm.utils import is_numeric, denoms
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Transaction
from quarkchain.core import RootBlock, TransactionReceipt, MinorBlock
from quarkchain.evm.transactions import Transaction as EvmTransaction
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


def quantity_decoder(hexStr):
    """Decode `hexStr` representing a quantity."""
    # must start with "0x"
    if not hexStr.startswith("0x") or len(hexStr) < 3:
        raise InvalidParams("Invalid quantity encoding")

    try:
        return int(hexStr, 16)
    except ValueError:
        raise InvalidParams('Invalid quantity encoding')


def quantity_encoder(i):
    """Encode integer quantity `data`."""
    assert is_numeric(i)
    return hex(i)


def data_decoder(hexStr):
    """Decode `hexStr` representing unformatted hexStr."""
    if not hexStr.startswith("0x"):
        raise InvalidParams("Invalid hexStr encoding")
    try:
        return bytes.fromhex(hexStr[2:])
    except Exception:
        raise InvalidParams("Invalid hexStr hex encoding")


def data_encoder(dataBytes):
    """Encode unformatted binary `dataBytes`.
    """
    return "0x" + dataBytes.hex()


def address_decoder(hexStr):
    """Decode an address from hex with 0x prefix to 24 bytes."""
    addrBytes = data_decoder(hexStr)
    if len(addrBytes) not in (24, 0):
        raise InvalidParams('Addresses must be 24 or 0 bytes long')
    return addrBytes


def address_encoder(addrBytes):
    assert len(addrBytes) == 24
    return data_encoder(addrBytes)


def recipient_decoder(hexStr):
    """Decode an recipient from hex with 0x prefix to 20 bytes."""
    recipientBytes = data_decoder(hexStr)
    if len(recipientBytes) not in (20, 0):
        raise InvalidParams('Addresses must be 20 or 0 bytes long')
    return recipientBytes


def recipient_encoder(recipientBytes):
    assert len(recipientBytes) == 20
    return data_encoder(recipientBytes)


def full_shard_id_decoder(hexStr):
    b = data_decoder(hexStr)
    if len(b) != 4:
        raise InvalidParams("Full shard id must be 4 bytes")
    return int.from_bytes(b, byteorder="big")


def full_shard_id_encoder(fullShardId):
    return data_encoder(fullShardId.to_bytes(4, byteorder="big"))


def id_encoder(hashBytes, fullShardId):
    """ Encode hash and fullShardId into hex """
    return data_encoder(hashBytes + fullShardId.to_bytes(4, byteorder="big"))


def id_decoder(hexStr):
    """ Decode an id to (hash, fullShardId) """
    dataBytes = data_decoder(hexStr)
    if len(dataBytes) != 36:
        raise InvalidParams("Invalid id encoding")
    return dataBytes[:32], int.from_bytes(dataBytes[32:], byteorder="big")


def block_hash_decoder(hexStr):
    """Decode a block hash."""
    decoded = data_decoder(hexStr)
    if len(decoded) != 32:
        raise InvalidParams("Block hashes must be 32 bytes long")
    return decoded


def tx_hash_decoder(hexStr):
    """Decode a transaction hash."""
    decoded = data_decoder(hexStr)
    if len(decoded) != 32:
        raise InvalidParams("Transaction hashes must be 32 bytes long")
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
        "hashPrevBlock": data_encoder(header.hashPrevBlock),
        "idPrevBlock": data_encoder(header.hashPrevBlock),
        "nonce": quantity_encoder(header.nonce),
        "hashMerkleRoot": data_encoder(header.hashMerkleRoot),
        "miner": address_encoder(header.coinbaseAddress.serialize()),
        "difficulty": quantity_encoder(header.difficulty),
        "timestamp": quantity_encoder(header.createTime),
        "size": quantity_encoder(len(block.serialize())),
    }

    d["minorBlockHeaders"] = []
    for header in block.minorBlockHeaderList:
        h = {
            'id': id_encoder(header.get_hash(), header.branch.get_shard_id()),
            'height': quantity_encoder(header.height),
            'hash': data_encoder(header.get_hash()),
            'branch': quantity_encoder(header.branch.value),
            'shard': quantity_encoder(header.branch.get_shard_id()),
            'hashPrevMinorBlock': data_encoder(header.hashPrevMinorBlock),
            'idPrevMinorBlock': id_encoder(header.hashPrevMinorBlock, header.branch.get_shard_id()),
            'hashPrevRootBlock': data_encoder(header.hashPrevRootBlock),
            'nonce': quantity_encoder(header.nonce),
            'difficulty': quantity_encoder(header.difficulty),
            'timestamp': quantity_encoder(header.createTime),
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
        'id': id_encoder(header.get_hash(), header.branch.get_shard_id()),
        'height': quantity_encoder(header.height),
        'hash': data_encoder(header.get_hash()),
        'branch': quantity_encoder(header.branch.value),
        'shard': quantity_encoder(header.branch.get_shard_id()),
        'hashPrevMinorBlock': data_encoder(header.hashPrevMinorBlock),
        'idPrevMinorBlock': id_encoder(header.hashPrevMinorBlock, header.branch.get_shard_id()),
        'hashPrevRootBlock': data_encoder(header.hashPrevRootBlock),
        'nonce': quantity_encoder(header.nonce),
        'hashMerkleRoot': data_encoder(meta.hashMerkleRoot),
        'hashEvmStateRoot': data_encoder(meta.hashEvmStateRoot),
        'miner': address_encoder(meta.coinbaseAddress.serialize()),
        'difficulty': quantity_encoder(header.difficulty),
        'extraData': data_encoder(meta.extraData),
        'gasLimit': quantity_encoder(meta.evmGasLimit),
        'gasUsed': quantity_encoder(meta.evmGasUsed),
        'timestamp': quantity_encoder(header.createTime),
        'size': quantity_encoder(len(block.serialize())),
    }
    if include_transactions:
        d['transactions'] = []
        for i, tx in enumerate(block.txList):
            d['transactions'].append(tx_encoder(block, i))
    else:
        d['transactions'] = [id_encoder(tx.get_hash(), block.header.branch.get_shard_id()) for tx in block.txList]
    return d


def tx_encoder(block, i):
    """Encode a transaction as JSON object.

    `transaction` is the `i`th transaction in `block`.
    """
    tx = block.txList[i]
    evmTx = tx.code.get_evm_transaction()
    # TODO: shardMask is wrong when the tx is pending and block is fake
    shardMask = block.header.branch.get_shard_size() - 1
    return {
        'id': id_encoder(tx.get_hash(), evmTx.fromFullShardId),
        'hash': data_encoder(tx.get_hash()),
        'nonce': quantity_encoder(evmTx.nonce),
        'timestamp': quantity_encoder(block.header.createTime),
        'shard': quantity_encoder(block.header.branch.get_shard_id()),
        'blockId': id_encoder(block.header.get_hash(), block.header.branch.get_shard_id()),
        'blockHeight': quantity_encoder(block.header.height),
        'transactionIndex': quantity_encoder(i),
        'from': data_encoder(evmTx.sender),
        'to': data_encoder(evmTx.to),
        'fromFullShardId': full_shard_id_encoder(evmTx.fromFullShardId),
        'toFullShardId': full_shard_id_encoder(evmTx.toFullShardId),
        'from_shard_id': quantity_encoder(evmTx.fromFullShardId & shardMask),
        'to_shard_id': quantity_encoder(evmTx.toFullShardId & shardMask),
        'value': quantity_encoder(evmTx.value),
        'gasPrice': quantity_encoder(evmTx.gasprice),
        'gas': quantity_encoder(evmTx.startgas),
        'data': data_encoder(evmTx.data),
        'networkId': quantity_encoder(evmTx.networkId),
        'r': quantity_encoder(evmTx.r),
        's': quantity_encoder(evmTx.s),
        'v': quantity_encoder(evmTx.v),
    }


def loglist_encoder(loglist):
    """Encode a list of log"""
    # l = []
    # if len(loglist) > 0 and loglist[0] is None:
    #     assert all(element is None for element in l)
    #     return l
    result = []
    for l in loglist:
        result.append({
            'logIndex': quantity_encoder(l['log_idx']),
            'transactionIndex': quantity_encoder(l['tx_idx']),
            'transactionHash': data_encoder(l['txhash']),
            'blockHash': data_encoder(l['block'].header.get_hash()),
            'blockNumber': quantity_encoder(l['block'].header.height),
            'blockHeight': quantity_encoder(l['block'].header.height),
            'address': data_encoder(l['log'].recipient),
            'recipient': data_encoder(l['log'].recipient),
            'data': data_encoder(l['log'].data),
            'topics': [data_encoder(topic) for topic in l['log'].topics],
        })
    return result


def receipt_encoder(block: MinorBlock, i: int, receipt: TransactionReceipt):
    tx = block.txList[i]
    evmTx = tx.code.get_evm_transaction()
    resp = {
        'transactionId': id_encoder(tx.get_hash(), evmTx.fromFullShardId),
        'transactionHash': data_encoder(tx.get_hash()),
        'transactionIndex': quantity_encoder(i),
        'blockId': id_encoder(block.header.get_hash(), block.header.branch.get_shard_id()),
        'blockHash': data_encoder(block.header.get_hash()),
        'blockHeight': quantity_encoder(block.header.height),
        'blockNumber': quantity_encoder(block.header.height),
        'cumulativeGasUsed': quantity_encoder(receipt.gasUsed),
        'gasUsed': quantity_encoder(receipt.gasUsed - receipt.prevGasUsed),
        'status': quantity_encoder(1 if receipt.success == b"\x01" else 0),
        'contractAddress': (
            address_encoder(receipt.contractAddress.serialize())
            if not receipt.contractAddress.is_empty() else None
        ),
    }
    logs = []
    for j, log in enumerate(receipt.logs):
        logs.append({
            'log': log,
            'log_idx': j,
            'block': block,
            'txhash': tx.get_hash(),
            'tx_idx': i,
        })
    resp['logs'] = loglist_encoder(logs)

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


def block_id_decoder(data):
    """Decode a block identifier as expected from :meth:`JSONRPCServer.get_block`."""
    if data in (None, 'latest', 'earliest', 'pending'):
        return data
    else:
        return quantity_decoder(data)

def shard_id_decoder(data):
    try:
        return quantity_decoder(data)
    except Exception:
        return None

def eth_address_to_quarkchain_address_decoder(hexStr):
    ethHex = hexStr[2:]
    if len(ethHex) != 40:
        raise InvalidParams('Addresses must be 40 or 0 bytes long')
    fullShardIdHex = ""
    for i in range(4):
        index = i * 10
        fullShardIdHex += ethHex[index:index + 2]
    return address_decoder("0x" + ethHex + fullShardIdHex)


public_methods = AsyncMethods()
private_methods = AsyncMethods()


class JSONRPCServer:

    @classmethod
    def start_public_server(cls, env, masterServer):
        server = cls(env, masterServer, env.config.PUBLIC_JSON_RPC_PORT, public_methods)
        server.start()
        return server

    @classmethod
    def start_private_server(cls, env, masterServer):
        server = cls(env, masterServer, env.config.PRIVATE_JSON_RPC_PORT, private_methods)
        server.start()
        return server

    @classmethod
    def start_test_server(cls, env, masterServer):
        methods = AsyncMethods()
        for method in public_methods.values():
            methods.add(method)
        for method in private_methods.values():
            methods.add(method)
        server = cls(env, masterServer, env.config.PUBLIC_JSON_RPC_PORT, methods)
        server.start()
        return server

    def __init__(self, env, masterServer, port, methods: AsyncMethods):
        self.loop = asyncio.get_event_loop()
        self.port =port
        self.env = env
        self.master = masterServer
        self.counters = dict()

        # Bind RPC handler functions to this instance
        self.handlers = AsyncMethods()
        for rpcName in methods:
            func = methods[rpcName]
            self.handlers[rpcName] = func.__get__(self, self.__class__)

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
        else:
            return web.json_response(response, status=response.http_status)

    def start(self):
        app = web.Application(client_max_size=JSON_RPC_CLIENT_REQUEST_MAX_SIZE)
        cors = aiohttp_cors.setup(app)
        route = app.router.add_post("/", self.__handle)
        cors.add(route, {
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers=("X-Custom-Server-Header",),
                allow_methods=["POST", "PUT"],
                allow_headers=("X-Requested-With", "Content-Type")
            ),
        })
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
    async def echo_quantity(self, quantity):
        return quantity

    @public_methods.add
    @decode_arg("data", data_decoder)
    @encode_res(data_encoder)
    async def echo_data(self, data):
        return data

    @public_methods.add
    async def network_info(self):
        return {
            "networkId": quantity_encoder(self.master.env.config.NETWORK_ID),
            "shardSize": quantity_encoder(self.master.get_shard_size()),
            "syncing": self.master.is_syncing(),
            "mining": self.master.is_mining(),
            "shardServerCount": len(self.master.slavePool),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    @encode_res(quantity_encoder)
    async def get_transaction_count(self, address):
        accountBranchData = await self.master.get_primary_account_data(Address.deserialize(address))
        return accountBranchData.transactionCount

    @public_methods.add
    @decode_arg("address", address_decoder)
    async def get_balance(self, address):
        accountBranchData = await self.master.get_primary_account_data(Address.deserialize(address))
        branch = accountBranchData.branch
        balance = accountBranchData.balance
        return {
            "branch": quantity_encoder(branch.value),
            "shard": quantity_encoder(branch.get_shard_id()),
            "balance": quantity_encoder(balance),
        }

    @public_methods.add
    @decode_arg("address", address_decoder)
    async def get_account_data(self, address, includeShards=False):
        address = Address.deserialize(address)
        if not includeShards:
            accountBranchData = await self.master.get_primary_account_data(address)
            branch = accountBranchData.branch
            balance = accountBranchData.balance
            count = accountBranchData.transactionCount
            primary = {
                "branch": quantity_encoder(branch.value),
                "shard": quantity_encoder(branch.get_shard_id()),
                "balance": quantity_encoder(balance),
                "transactionCount": quantity_encoder(count),
                "isContract": accountBranchData.isContract,
            }
            return {"primary": primary}

        branchToAccountBranchData = await self.master.get_account_data(address)
        shardSize = self.master.get_shard_size()

        shards = []
        for shard in range(shardSize):
            branch = Branch.create(shardSize, shard)
            accountBranchData = branchToAccountBranchData[branch]
            data = {
                "branch": quantity_encoder(accountBranchData.branch.value),
                "shard": quantity_encoder(accountBranchData.branch.get_shard_id()),
                "balance": quantity_encoder(accountBranchData.balance),
                "transactionCount": quantity_encoder(accountBranchData.transactionCount),
                "isContract": accountBranchData.isContract,
            }
            shards.append(data)

            if shard == address.get_shard_id(shardSize):
                primary = data

        return {
            "primary": primary,
            "shards": shards,
        }

    @public_methods.add
    async def send_unsigned_transaction(self, **data):
        """ Returns the unsigned hash of the evm transaction """
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        nonce = get_data_default("nonce", quantity_decoder, None)
        to = get_data_default("to", recipient_decoder, b'')
        startgas = get_data_default("gas", quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default("gasPrice", quantity_decoder, DEFAULT_GASPRICE)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")

        fromFullShardId = get_data_default("fromFullShardId", full_shard_id_decoder, None)
        toFullShardId = get_data_default("toFullShardId", full_shard_id_decoder, None)

        if nonce is None:
            raise InvalidParams("nonce is missing")
        if fromFullShardId is None:
            raise InvalidParams("fromFullShardId is missing")

        if toFullShardId is None:
            toFullShardId = fromFullShardId

        evmTx = EvmTransaction(
            nonce, gasprice, startgas, to, value, data_,
            fromFullShardId=fromFullShardId,
            toFullShardId=toFullShardId,
            networkId=self.master.env.config.NETWORK_ID,
        )

        return {
            "txHashUnsigned": data_encoder(evmTx.hash_unsigned),
            "nonce": quantity_encoder(evmTx.nonce),
            'to': data_encoder(evmTx.to),
            'fromFullShardId': full_shard_id_encoder(evmTx.fromFullShardId),
            'toFullShardId': full_shard_id_encoder(evmTx.toFullShardId),
            'value': quantity_encoder(evmTx.value),
            'gasPrice': quantity_encoder(evmTx.gasprice),
            'gas': quantity_encoder(evmTx.startgas),
            'data': data_encoder(evmTx.data),
            'networkId': quantity_encoder(evmTx.networkId),
        }

    @public_methods.add
    async def send_transaction(self, **data):
        def get_data_default(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        to = get_data_default("to", recipient_decoder, b"")
        startgas = get_data_default('gas', quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default('gasPrice', quantity_decoder, DEFAULT_GASPRICE)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")
        v = get_data_default("v", quantity_decoder, 0)
        r = get_data_default("r", quantity_decoder, 0)
        s = get_data_default("s", quantity_decoder, 0)
        nonce = get_data_default("nonce", quantity_decoder, None)

        toFullShardId = get_data_default("toFullShardId", full_shard_id_decoder, None)
        fromFullShardId = get_data_default("fromFullShardId", full_shard_id_decoder, None)
        networkId = get_data_default("networkID", quantity_decoder, self.master.env.config.NETWORK_ID)

        if nonce is None:
            raise InvalidParams("Missing nonce")
        if not (v and r and s):
            raise InvalidParams("Missing v, r, s")
        if fromFullShardId is None:
            raise InvalidParams("Missing fromFullShardId")

        if toFullShardId is None:
            toFullShardId = fromFullShardId

        evmTx = EvmTransaction(
            nonce, gasprice, startgas, to, value, data_, v, r, s,
            fromFullShardId=fromFullShardId,
            toFullShardId=toFullShardId,
            networkId=networkId,
        )
        tx = Transaction(code=Code.create_evm_code(evmTx))
        success = await self.master.add_transaction(tx)
        if not success:
            return None

        return id_encoder(tx.get_hash(), fromFullShardId)

    @public_methods.add
    @decode_arg("txData", data_decoder)
    async def send_raw_transaction(self, txData):
        evmTx = rlp.decode(txData, EvmTransaction)
        tx = Transaction(code=Code.create_evm_code(evmTx))
        success = await self.master.add_transaction(tx)
        if not success:
            return "0x" + bytes(32 + 4).hex()
        return id_encoder(tx.get_hash(), evmTx.fromFullShardId)

    @public_methods.add
    @decode_arg("blockId", data_decoder)
    async def get_root_block_by_id(self, blockId):
        try:
            block = self.master.rootState.db.get_root_block_by_hash(blockId, False)
            return root_block_encoder(block)
        except Exception:
            return None

    @public_methods.add
    @decode_arg("height", quantity_decoder)
    async def get_root_block_by_height(self, height):
        block = self.master.rootState.get_root_block_by_height(height)
        if not block:
            return None
        return root_block_encoder(block)

    @public_methods.add
    @decode_arg("blockId", id_decoder)
    @decode_arg("includeTransactions", bool_decoder)
    async def get_minor_block_by_id(self, blockId, includeTransactions=False):
        blockHash, fullShardId = blockId
        shardSize = self.master.get_shard_size()
        branch = Branch.create(shardSize, (shardSize - 1) & fullShardId)
        block = await self.master.get_minor_block_by_hash(blockHash, branch)
        if not block:
            return None
        return minor_block_encoder(block, includeTransactions)

    @public_methods.add
    @decode_arg("shard", quantity_decoder)
    @decode_arg("includeTransactions", bool_decoder)
    async def get_minor_block_by_height(self, shard: int, height=None, includeTransactions=False):
        shardSize = self.master.get_shard_size()
        if height is not None:
            height = quantity_decoder(height)
        if shard >= shardSize:
            raise InvalidParams("shard is larger than shard size {} > {}".format(shard, shardSize))
        branch = Branch.create(shardSize, shard)
        block = await self.master.get_minor_block_by_height(height, branch)
        if not block:
            return None
        return minor_block_encoder(block, includeTransactions)

    @public_methods.add
    @decode_arg("txId", id_decoder)
    async def get_transaction_by_id(self, txId):
        txHash, fullShardId = txId
        shardSize = self.master.get_shard_size()
        branch = Branch.create(shardSize, (shardSize - 1) & fullShardId)
        minorBlock, i = await self.master.get_transaction_by_hash(txHash, branch)
        if not minorBlock:
            return None
        if len(minorBlock.txList) <= i:
            return None
        return tx_encoder(minorBlock, i)

    @public_methods.add
    async def call(self, **data):
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

        toFullShardId = int.from_bytes(to[20:], "big")

        gas = get_data_default("gas", quantity_decoder, 1000000)
        gasPrice = get_data_default("gasPrice", quantity_decoder, 0)
        value = get_data_default("value", quantity_decoder, 0)
        data_ = get_data_default("data", data_decoder, b"")
        sender = get_data_default("from", address_decoder, b"\x00" * 20 + to[20:])
        senderAddress = Address.create_from(sender)

        networkId = self.master.env.config.NETWORK_ID

        nonce = 0  # slave will fill in the real nonce
        evmTx = EvmTransaction(
            nonce, gasPrice, gas, to[:20], value, data_,
            fromFullShardId=senderAddress.fullShardId, toFullShardId=toFullShardId, networkId=networkId)

        tx = Transaction(code=Code.create_evm_code(evmTx))
        res = await self.master.execute_transaction(tx, senderAddress)
        return data_encoder(res) if res is not None else None

    @public_methods.add
    @decode_arg("txId", id_decoder)
    async def get_transaction_receipt(self, txId):
        txHash, fullShardId = txId
        shardSize = self.master.get_shard_size()
        branch = Branch.create(shardSize, (shardSize - 1) & fullShardId)
        resp = await self.master.get_transaction_receipt(txHash, branch)
        if not resp:
            return None
        minorBlock, i, receipt = resp
        if len(minorBlock.txList) <= i:
            return None

        return receipt_encoder(minorBlock, i, receipt)

    @public_methods.add
    async def get_jrpc_calls(self):
        return self.counters


    ######################## Ethereum JSON RPC ########################

    @public_methods.add
    async def net_version(self):
        """
        """
        return quantity_encoder(self.master.env.config.NETWORK_ID)

    @public_methods.add
    @encode_res(quantity_encoder)
    async def eth_gas_price(self):
        """
        """
        return 10 ** 9

    @public_methods.add
    @decode_arg('block_id', block_id_decoder)
    @decode_arg('include_transactions', bool_decoder)
    async def eth_get_block_by_number(self, block_id, include_transactions):
        """
        NOTE: only support block_id "latest" or hex
        """

        def block_transcoder(block):
            """
            QuarkChain Block => ETH Block
            """
            return {
                **block,
                'number': block['height'],
                'parentHash': block['hashPrevMinorBlock'],
                'sha3Uncles': '',
                'logsBloom': '',
                'transactionsRoot': block['hashMerkleRoot'],  # ?
                'stateRoot': block['hashEvmStateRoot'],  # ?
            }

        height = None if block_id == "latest" else block_id
        block = await self.master.get_minor_block_by_height(height, Branch.create(self.master.get_shard_size(), 0))
        if block is None:
            return None
        return block_transcoder(minor_block_encoder(block))

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("shard", shard_id_decoder)
    @encode_res(quantity_encoder)
    async def eth_get_balance(self, address, shard=None):
        address = Address.deserialize(address)
        if shard is not None:
            address = Address(address.recipient, shard)
        accountBranchData = await self.master.get_primary_account_data(address)
        balance = accountBranchData.balance
        return balance

    @public_methods.add
    @decode_arg("address", eth_address_to_quarkchain_address_decoder)
    @decode_arg("shard", shard_id_decoder)
    @encode_res(quantity_encoder)
    async def eth_get_transaction_count(self, address, shard=None):
        address = Address.deserialize(address)
        if shard is not None:
            address = Address(address.recipient, shard)
        accountBranchData = await self.master.get_primary_account_data(address)
        return accountBranchData.transactionCount

    @public_methods.add
    @decode_arg('address', eth_address_to_quarkchain_address_decoder)
    @encode_res(data_encoder)
    async def eth_get_code(self, address, shard=None):
        """TODO implement this
        """
        return bytes(10)  # to let web3.eth.contract(abi).new() work


    @public_methods.add
    @decode_arg("shard", shard_id_decoder)
    async def eth_call(self, data, shard):
        """ Returns the result of the transaction application without putting in block chain """
        toAddress = Address.create_from(eth_address_to_quarkchain_address_decoder(data["to"]))
        if shard:
            toAddress = Address(toAddress.recipient, shard)
        data["to"] = "0x" + toAddress.serialize().hex()
        if "from" in data:
            fromAddress = Address.create_from(eth_address_to_quarkchain_address_decoder(data["from"]))
            if shard:
                fromAddress = Address(fromAddress.recipient, shard)
            data["from"] = "0x" + fromAddress.serialize().hex()
        return await self.call(**data)

    @public_methods.add
    async def eth_send_raw_transaction(self, txData):
        return await self.send_raw_transaction(txData)

    @public_methods.add
    async def eth_get_transaction_receipt(self, txId):
        receipt = await self.get_transaction_receipt(txId)
        if not receipt:
            return None
        if receipt["contractAddress"]:
            receipt["contractAddress"] = receipt["contractAddress"][:42]
        return receipt


    ######################## Private Methods ########################

    @private_methods.add
    @decode_arg("coinbaseAddress", address_decoder)
    @decode_arg("shardMaskValue", quantity_decoder)
    async def get_next_block_to_mine(self, coinbaseAddress, shardMaskValue, preferRoot=False):
        address = Address.deserialize(coinbaseAddress)
        isRootBlock, block = await self.master.get_next_block_to_mine(address, shardMaskValue, preferRoot=preferRoot)
        if not block:
            return None
        return {
            "isRootBlock": isRootBlock,
            "blockData": data_encoder(block.serialize()),
        }

    @private_methods.add
    @decode_arg("branch", quantity_decoder)
    @decode_arg("blockData", data_decoder)
    async def add_block(self, branch, blockData):
        if branch == 0:
            block = RootBlock.deserialize(blockData)
            return await self.master.add_root_block_from_miner(block)
        return await self.master.add_raw_minor_block(Branch(branch), blockData)

    @private_methods.add
    async def get_peers(self):
        peerList = []
        for peerId, peer in self.master.network.activePeerPool.items():
            peerList.append({
                "id": data_encoder(peerId),
                "ip": quantity_encoder(int(peer.ip)),
                "port": quantity_encoder(peer.port),
            })
        return {
            "peers": peerList,
        }

    @private_methods.add
    async def get_stats(self):
        # This JRPC doesn't follow the standard encoding
        return await self.master.get_stats()

    @private_methods.add
    async def create_transactions(self, **loadTestData):
        """Create transactions for load testing"""
        def get_data_default(key, decoder, default=None):
            if key in loadTestData:
                return decoder(loadTestData[key])
            return default

        numTxPerShard = loadTestData["numTxPerShard"]
        xShardPercent = loadTestData["xShardPercent"]
        to = get_data_default("to", recipient_decoder, b"")
        startgas = get_data_default("gas", quantity_decoder, DEFAULT_STARTGAS)
        gasprice = get_data_default("gasPrice", quantity_decoder, int(DEFAULT_GASPRICE / 10))
        value = get_data_default("value", quantity_decoder, 0)
        data = get_data_default("data", data_decoder, b"")
        # FIXME: can't support specifying full shard ID to 0. currently is regarded as not set
        fromFullShardId = get_data_default("fromFullShardId", full_shard_id_decoder, 0)
        # build sample tx
        evmTxSample = EvmTransaction(
            0, gasprice, startgas, to, value, data,
            fromFullShardId=fromFullShardId,
        )
        tx = Transaction(code=Code.create_evm_code(evmTxSample))
        return await self.master.create_transactions(numTxPerShard, xShardPercent, tx)

    @private_methods.add
    async def set_target_block_time(self, rootBlockTime=0, minorBlockTime=0):
        """0 will not update existing value"""
        return await self.master.set_target_block_time(rootBlockTime, minorBlockTime)

    @private_methods.add
    async def set_mining(self, mining):
        """Turn on / off mining"""
        return await self.master.set_mining(mining)

    @private_methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("start", data_decoder)
    @decode_arg("limit", quantity_decoder)
    async def get_transactions_by_address(self, address, start="0x", limit="0xa"):
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
        txList, next = result
        txs = []
        for tx in txList:
            txs.append({
                "txId": id_encoder(tx.txHash, tx.fromAddress.fullShardId),
                "fromAddress": address_encoder(tx.fromAddress.serialize()),
                "toAddress": address_encoder(tx.toAddress.serialize()) if tx.toAddress else "0x",
                "value": quantity_encoder(tx.value),
                "blockHeight": quantity_encoder(tx.blockHeight),
                "timestamp": quantity_encoder(tx.timestamp),
                "success": tx.success,
            })
        return {
            "txList": txs,
            "next": data_encoder(next),
        }

    @private_methods.add
    async def get_jrpc_calls(self):
        return self.counters


if __name__ == "__main__":
    # web.run_app(app, port=5000)
    server = JSONRPCServer(DEFAULT_ENV, None)
    server.start()
    asyncio.get_event_loop().run_forever()
