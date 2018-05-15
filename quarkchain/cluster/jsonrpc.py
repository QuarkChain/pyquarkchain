import asyncio
import inspect

from aiohttp import web
from decorator import decorator
from ethereum.utils import (
    is_numeric, is_string, int_to_big_endian, big_endian_to_int,
    encode_hex, decode_hex, sha3, zpad, denoms, int32)

from jsonrpcserver import config
from jsonrpcserver.aio import methods
from jsonrpcserver.async_methods import AsyncMethods
from jsonrpcserver.exceptions import InvalidParams, ServerError

from quarkchain.cluster.core import MinorBlock, RootBlock
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import Logger


# defaults
default_startgas = 500 * 1000
default_gasprice = 60 * denoms.shannon


def is_json_string(data):
    return isinstance(data, str)


def quantity_decoder(data):
    """Decode `data` representing a quantity."""
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
    if not data.startswith("0x"):
        data = "0x" + data

    if len(data) % 2 != 0:
        # workaround for missing leading zeros from netstats
        assert len(data) < 64 + 2
        data = "0x" + "0" * (64 - (len(data) - 2)) + data[2:]

    try:
        return decode_hex(data[2:])
    except TypeError:
        raise InvalidParams("Invalid data hex encoding", data[2:])


def data_encoder(data, length=None):
    """Encode unformatted binary `data`.

    If `length` is given, the result will be padded like this: ``data_encoder("b\xff", 3) ==
    "0x0000ff"``.
    """
    s = encode_hex(data)
    if length is None:
        return str(s)
    else:
        return str(s.rjust(length * 2, "0"))


def obj_decoder(data, cls):
    raw = data_decoder(data)
    try:
        return cls.deserialize(raw)
    except Exception as e:
        raise InvalidParams(e)


def obj_encoder(obj, cls):
    assert isinstance(obj, cls)
    result = str("0x" + encode_hex(obj.serialize()))
    return result


def address_decoder(data):
    """Decode an address from hex with 0x prefix to 24 bytes."""
    addr = data_decoder(data)
    if len(addr) not in (24, 0):
        raise InvalidParams('Addresses must be 24 or 0 bytes long')
    return addr


def address_encoder(address):
    assert len(address) in (24, 0)
    result = str('0x' + encode_hex(address))
    return result


def block_id_decoder(data):
    """Decode a block identifier as expected from :meth:`JSONRPCServer.get_block`."""
    if data in (None, "latest", "earliest", "pending"):
        return data
    else:
        return quantity_decoder(data)


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
    @decode_arg("blockId", block_id_decoder)
    async def getTransactionCount(self, address, blockId="pending"):
        branch, count = await self.master.getTransactionCount(Address.deserialize(address))
        return {
            "branch": quantity_encoder(branch.value),
            "count": quantity_encoder(count),
        }

    @methods.add
    @decode_arg("address", address_decoder)
    @decode_arg("blockId", block_id_decoder)
    async def getBalance(self, address, blockId="pending"):
        branch, balance = await self.master.getBalance(Address.deserialize(address))
        return {
            "branch": quantity_encoder(branch.value),
            "balance": quantity_encoder(balance),
        }

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

        branch, nonce = await self.master.getTransactionCount(fromAddr)
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

        branch, nonce = await self.master.getTransactionCount(fromAddr)
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

        return data_encoder(tx.getHash())

    '''
    This JRPC requires clients to know the data structure of the trasaction.
    Commented out for now until open source.

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

        return data_encoder(tx.getHash())
    '''

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
    @decode_arg("count", quantity_decoder)
    async def setArtificialTxCount(self, count):
        self.master.setArtificialTxCount(count)

    @methods.add
    async def getStats(self):
        return await self.master.getStats()


if __name__ == "__main__":
    # web.run_app(app, port=5000)
    server = JSONRPCServer(DEFAULT_ENV, None)
    server.start()
    asyncio.get_event_loop().run_forever()
