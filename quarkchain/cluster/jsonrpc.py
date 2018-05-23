import asyncio
import inspect

from aiohttp import web
from decorator import decorator
from ethereum.utils import (
    is_numeric, is_string, int_to_big_endian, big_endian_to_int,
    encode_hex, zpad, denoms, int32)

from jsonrpcserver import config
from jsonrpcserver.aio import methods
from jsonrpcserver.async_methods import AsyncMethods
from jsonrpcserver.exceptions import InvalidParams, ServerError

from quarkchain.cluster.core import RootBlock
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import Logger

# defaults
default_startgas = 500 * 1000
default_gasprice = 60 * denoms.shannon


def quantity_decoder(data):
    """Decode `data` representing a quantity."""
    if not data.isdigit():
        return InvalidParams()
    try:
        return int(data, 10)
    except ValueError:
        raise InvalidParams("Invalid quantity encoding")


def quantity_encoder(i):
    """Encode integer quantity `data`."""
    assert is_numeric(i)
    return str(i)


def data_decoder(data):
    """Decode `data` representing unformatted data."""
    try:
        return bytes.fromhex(data)
    except Exception:
        raise InvalidParams("Invalid data hex encoding", data)


def data_encoder(data):
    """Encode unformatted binary `data`.
    """
    return data.hex()


def address_decoder(data):
    """Decode an address from hex with 0x prefix to 24 bytes."""
    addr = data_decoder(data)
    if len(addr) not in (24, 0):
        raise InvalidParams('Addresses must be 24 or 0 bytes long')
    return addr


def address_encoder(address):
    assert len(address) in (24, 0)
    return address.hex()


def id_encoder(hashBytes, branch):
    return hashBytes.hex() + branch.serialize().lstrip(b"\x00").hex()


def id_decoder(data):
    if len(data) <= 32:
        raise InvalidParams()
    pad = 36 - len(data)
    return data[:32], Branch.deserialize(bytes(pad) + data[32:])


def block_hash_decoder(data):
    """Decode a block hash."""
    decoded = data_decoder(data)
    if len(decoded) != 32:
        raise InvalidParams("Block hashes must be 32 bytes long")
    return decoded


def tx_hash_decoder(data):
    """Decode a transaction hash."""
    decoded = data_decoder(data)
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
        "id": data_encoder(header.getHash()),
        "height": quantity_encoder(header.height),
        "hash": data_encoder(header.getHash()),
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
            'id': id_encoder(header.getHash(), header.branch),
            'height': quantity_encoder(header.height),
            'hash': data_encoder(header.getHash()),
            'branch': quantity_encoder(header.branch.value),
            'shard': quantity_encoder(header.branch.getShardId()),
            'hashPrevMinorBlock': data_encoder(header.hashPrevMinorBlock),
            'idPrevMinorBlock': id_encoder(header.hashPrevMinorBlock, header.branch),
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
        'id': id_encoder(header.getHash(), header.branch),
        'height': quantity_encoder(header.height),
        'hash': data_encoder(header.getHash()),
        'branch': quantity_encoder(header.branch.value),
        'shard': quantity_encoder(header.branch.getShardId()),
        'hashPrevMinorBlock': data_encoder(header.hashPrevMinorBlock),
        'idPrevMinorBlock': id_encoder(header.hashPrevMinorBlock, header.branch),
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
        d['transactions'] = [id_encoder(tx.getHash(), block.header.branch) for tx in block.txList]
    return d


def tx_encoder(block, i):
    """Encode a transaction as JSON object.

    `transaction` is the `i`th transaction in `block`.
    """
    tx = block.txList[i]
    evmTx = tx.code.getEvmTransaction()
    branch = Branch(evmTx.branchValue)
    if evmTx.withdraw == 0:
        to = evmTx.to
        value = evmTx.value
        toShard = block.header.branch.getShardId()
    else:
        toAddr = Address.deserialize(evmTx.withdrawTo)
        to = toAddr.recipient
        value = evmTx.withdraw
        toShard = toAddr.getShardId(branch.getShardSize())
    return {
        'id': id_encoder(tx.getHash(), block.header.branch),
        'hash': data_encoder(tx.getHash()),
        'nonce': quantity_encoder(evmTx.nonce),
        'timestamp': quantity_encoder(block.header.createTime),
        'blockId': id_encoder(block.header.getHash(), block.header.branch),
        'blockHeight': quantity_encoder(block.header.height),
        'transactionIndex': quantity_encoder(i),
        'from': data_encoder(evmTx.sender),
        'to': data_encoder(to),
        'toShard': quantity_encoder(toShard),
        'value': quantity_encoder(value),
        'gasPrice': quantity_encoder(evmTx.gasprice),
        'gas': quantity_encoder(evmTx.startgas),
        'data': data_encoder(evmTx.data),
        'branch': quantity_encoder(branch.value),
        'shard': quantity_encoder(branch.getShardId()),
        # 'withdraw': quantity_encoder(evmTx.withdraw),
        # 'withdrawTo': data_encoder(evmTx.withdrawTo),
        'networkId': quantity_encoder(evmTx.networkId),
        'r': quantity_encoder(evmTx.r),
        's': quantity_encoder(evmTx.s),
        'v': quantity_encoder(evmTx.v),
    }


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


class JSONRPCServer:

    def __init__(self, env, masterServer):
        # Disable logging
        config.log_requests = False
        config.log_responses = False

        self.loop = asyncio.get_event_loop()
        self.port = env.config.LOCAL_SERVER_PORT
        self.env = env
        self.master = masterServer

        # Bind RPC handler functions to this instance
        self.handlers = AsyncMethods()
        for rpcName in methods:
            func = methods[rpcName]
            self.handlers[rpcName] = func.__get__(self, self.__class__)

    async def __handle(self, request):
        request = await request.text()
        response = await self.handlers.dispatch(request)
        if response.is_notification:
            return web.Response()
        else:
            return web.json_response(response, status=response.http_status)

    def start(self):
        app = web.Application()
        app.router.add_post("/", self.__handle)
        self.runner = web.AppRunner(app, access_log=None)
        self.loop.run_until_complete(self.runner.setup())
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        self.loop.run_until_complete(site.start())

    def shutdown(self):
        self.loop.run_until_complete(self.runner.cleanup())

    # JSON RPC handlers
    @methods.add
    @decode_arg("data", data_decoder)
    @encode_res(data_encoder)
    async def echo(self, data):
        return data

    @methods.add
    @decode_arg("address", address_decoder)
    async def getTransactionCount(self, address):
        accountBranchData = await self.master.getPrimaryAccountData(Address.deserialize(address))
        branch = accountBranchData.branch
        count = accountBranchData.transactionCount
        return {
            "branch": quantity_encoder(branch.value),
            "count": quantity_encoder(count),
        }

    @methods.add
    @decode_arg("address", address_decoder)
    async def getBalance(self, address):
        accountBranchData = await self.master.getPrimaryAccountData(Address.deserialize(address))
        branch = accountBranchData.branch
        balance = accountBranchData.balance
        return {
            "branch": quantity_encoder(branch.value),
            "shard": quantity_encoder(branch.getShardId()),
            "balance": quantity_encoder(balance),
        }

    @methods.add
    @decode_arg("address", address_decoder)
    async def getAccountData(self, address):
        address = Address.deserialize(address)
        branchToAccountBranchData = await self.master.getAccountData(address)
        shardSize = self.master.getShardSize()

        ret = []
        for shard in range(shardSize):
            branch = Branch.create(shardSize, shard)
            accountBranchData = branchToAccountBranchData[branch]
            ret.append({
                "branch": quantity_encoder(accountBranchData.branch.value),
                "shard": quantity_encoder(accountBranchData.branch.getShardId()),
                "balance": quantity_encoder(accountBranchData.balance),
                "transactionCount": quantity_encoder(accountBranchData.transactionCount),
            })
        return ret

    @methods.add
    async def sendUnsignedTransaction(self, **data):
        ''' Returns the unsigned hash of the evm transaction '''
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def getDataDefault(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        fromBytes = getDataDefault("from", address_decoder, None)
        to = getDataDefault("to", address_decoder, None)
        gasKey = "gas" if "gas" in data else "startgas"
        startgas = getDataDefault(gasKey, quantity_decoder, default_startgas)
        gaspriceKey = "gasPrice" if "gasPrice" in data else "gasprice"
        gasprice = getDataDefault(gaspriceKey, quantity_decoder, default_gasprice)
        value = getDataDefault("value", quantity_decoder, 0)
        data_ = getDataDefault("data", data_decoder, b"")

        if not fromBytes or not to or value == 0:
            raise InvalidParams("bad input")

        fromAddr = Address.deserialize(fromBytes)
        toAddr = Address.deserialize(to)

        shardSize = self.master.getShardSize()
        fromShard = fromAddr.getShardId(shardSize)
        toShard = toAddr.getShardId(shardSize)

        if fromShard == toShard:
            withdraw = 0
            withdrawTo = b""
        else:
            withdraw = value
            value = 0
            withdrawTo = bytes(toAddr.serialize())

        accountData = await self.master.getPrimaryAccountData(fromAddr)
        branch = accountData.branch
        nonce = accountData.transactionCount
        evmTx = EvmTransaction(
            nonce, gasprice, startgas, toAddr.recipient, value, data_,
            branchValue=branch.value,
            withdraw=withdraw,
            withdrawSign=1,
            withdrawTo=withdrawTo,
        )
        return data_encoder(evmTx.hash_unsigned)

    @methods.add
    async def sendSignedTransaction(self, **data):
        ''' Returns the unsigned hash of the evm transaction '''
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def getDataDefault(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        fromBytes = getDataDefault("from", address_decoder, None)
        to = getDataDefault("to", address_decoder, None)
        gasKey = "gas" if "gas" in data else "startgas"
        startgas = getDataDefault(gasKey, quantity_decoder, default_startgas)
        gaspriceKey = "gasPrice" if "gasPrice" in data else "gasprice"
        gasprice = getDataDefault(gaspriceKey, quantity_decoder, default_gasprice)
        value = getDataDefault("value", quantity_decoder, 0)
        data_ = getDataDefault("data", data_decoder, b"")
        v = getDataDefault("v", quantity_decoder, 0)
        r = getDataDefault("r", quantity_decoder, 0)
        s = getDataDefault("s", quantity_decoder, 0)

        if not (v and r and s):
            raise InvalidParams("Mising v, r, s")
        if not fromBytes or not to or value == 0:
            raise InvalidParams("bad input")

        fromAddr = Address.deserialize(fromBytes)
        toAddr = Address.deserialize(to)

        shardSize = self.master.getShardSize()
        fromShard = fromAddr.getShardId(shardSize)
        toShard = toAddr.getShardId(shardSize)

        if fromShard == toShard:
            withdraw = 0
            withdrawTo = b""
        else:
            withdraw = value
            value = 0
            withdrawTo = bytes(toAddr.serialize())

        accountData = await self.master.getPrimaryAccountData(fromAddr)
        branch = accountData.branch
        nonce = accountData.transactionCount
        evmTx = EvmTransaction(
            nonce, gasprice, startgas, toAddr.recipient, value, data_, v, r, s,
            branchValue=branch.value,
            withdraw=withdraw,
            withdrawSign=1,
            withdrawTo=withdrawTo,
        )

        if evmTx.sender != fromAddr.recipient:
            raise InvalidParams("Transaction sender does not match the from address.")

        tx = Transaction(code=Code.createEvmCode(evmTx))
        success = await self.master.addTransaction(tx)
        if not success:
            raise ServerError("Failed to add transaction")

        return id_encoder(tx.getHash(), branch)

    @methods.add
    async def sendTransaction(self, **data):
        if not isinstance(data, dict):
            raise InvalidParams("Transaction must be an object")

        def getDataDefault(key, decoder, default=None):
            if key in data:
                return decoder(data[key])
            return default

        to = getDataDefault("to", address_decoder, None)
        gasKey = "gas" if "gas" in data else "startgas"
        startgas = getDataDefault(gasKey, quantity_decoder, default_startgas)
        gaspriceKey = "gasPrice" if "gasPrice" in data else "gasprice"
        gasprice = getDataDefault(gaspriceKey, quantity_decoder, default_gasprice)
        value = getDataDefault("value", quantity_decoder, 0)
        data_ = getDataDefault("data", data_decoder, b"")
        v = getDataDefault("v", quantity_decoder, 0)
        r = getDataDefault("r", quantity_decoder, 0)
        s = getDataDefault("s", quantity_decoder, 0)
        nonce = getDataDefault("nonce", quantity_decoder, None)

        branch = getDataDefault("branch", quantity_decoder, 0)
        withdraw = getDataDefault("withdraw", quantity_decoder, 0)
        withdrawTo = getDataDefault("withdrawTo", data_decoder, None)

        if nonce is None:
            raise InvalidParams("Missing nonce")
        if not (v and r and s):
            raise InvalidParams("Mising v, r, s")
        if branch == 0:
            raise InvalidParams("Missing branch")
        if withdraw > 0 and withdrawTo is None:
            raise InvalidParams("Missing withdrawTo")

        toAddr = Address.deserialize(to)
        evmTx = EvmTransaction(
            nonce, gasprice, startgas, toAddr.recipient, value, data_, v, r, s,
            branchValue=branch,
            withdraw=withdraw,
            withdrawSign=1,
            withdrawTo=withdrawTo if withdrawTo else b"",
        )
        tx = Transaction(code=Code.createEvmCode(evmTx))
        success = await self.master.addTransaction(tx)
        if not success:
            raise ServerError("Failed to add transaction")

        return id_encoder(tx.getHash(), Branch(branch))

    @methods.add
    @decode_arg("coinbaseAddress", address_decoder)
    @decode_arg("shardMaskValue", quantity_decoder)
    async def getNextBlockToMine(self, coinbaseAddress, shardMaskValue):
        address = Address.deserialize(coinbaseAddress)
        isRootBlock, block = await self.master.getNextBlockToMine(address, shardMaskValue)
        return {
            "isRootBlock": isRootBlock,
            "blockData": data_encoder(block.serialize()),
        }

    @methods.add
    @decode_arg("branch", quantity_decoder)
    @decode_arg("blockData", data_decoder)
    async def addBlock(self, branch, blockData):
        if branch == 0:
            block = RootBlock.deserialize(blockData)
            return await self.master.addRootBlock(block)
        return await self.master.addRawMinorBlock(Branch(branch), blockData)

    @methods.add
    async def setArtificialTxConfig(self, numTxPerBlock, xShardTxPercent, seconds=60):
        self.master.setArtificialTxConfig(numTxPerBlock, xShardTxPercent, seconds)

    @methods.add
    async def getStats(self):
        return await self.master.getStats()

    @methods.add
    @decode_arg("blockId", data_decoder)
    async def getRootBlockById(self, blockId):
        if not self.master.rootState.containRootBlockByHash(blockId):
            return None

        block = self.master.rootState.getRootBlockByHash(blockId)
        return root_block_encoder(block)

    @methods.add
    @decode_arg("height", quantity_decoder)
    async def getRootBlockByHeight(self, height):
        block = self.master.rootState.getRootBlockByHeight(height)
        if not block:
            return None
        return root_block_encoder(block)

    @methods.add
    @decode_arg("blockId", data_decoder)
    @decode_arg("includeTransactions", bool_decoder)
    async def getMinorBlockById(self, blockId, includeTransactions=False):
        blockHash, branch = id_decoder(blockId)
        block = await self.master.getMinorBlockByHash(blockHash, branch)
        if not block:
            return None
        return minor_block_encoder(block, includeTransactions)

    @methods.add
    @decode_arg("shard", quantity_decoder)
    @decode_arg("height", quantity_decoder)
    @decode_arg("includeTransactions", bool_decoder)
    async def getMinorBlockByHeight(self, shard, height, includeTransactions=False):
        branch = Branch.create(self.master.getShardSize(), shard)
        block = await self.master.getMinorBlockByHeight(height, branch)
        if not block:
            return None
        return minor_block_encoder(block, includeTransactions)

    @methods.add
    @decode_arg("txId", data_decoder)
    async def getTransactionById(self, txId):
        txHash, branch = id_decoder(txId)
        minorBlock, i = await self.master.getTransactionByHash(txHash, branch)
        if not minorBlock:
            return None
        if len(minorBlock.txList) <= i:
            return None
        return tx_encoder(minorBlock, i)

    @methods.add
    async def getPeers(self):
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


if __name__ == "__main__":
    # web.run_app(app, port=5000)
    server = JSONRPCServer(DEFAULT_ENV, None)
    server.start()
    asyncio.get_event_loop().run_forever()
