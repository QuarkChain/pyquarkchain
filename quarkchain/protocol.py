import asyncio
from enum import Enum

from quarkchain.core import Serializable
from quarkchain.utils import Logger

ROOT_SHARD_ID = 0


class ConnectionState(Enum):
    CONNECTING = 0   # connecting before the Connection can be used
    ACTIVE = 1       # the peer is active
    CLOSED = 2       # the peer connection is closed


class Metadata(Serializable):
    ''' Metadata contains the extra info that needs to be encoded in the RPC layer'''
    FIELDS = []

    def __init__(self):
        pass

    @staticmethod
    def get_byte_size():
        ''' Returns the size (in bytes) of the serialized object '''
        return 0


class AbstractConnection:
    connId = 0
    abortedRpcCount = 0

    @classmethod
    def __get_next_connection_id(cls):
        cls.connId += 1
        return cls.connId

    def __init__(self, opSerMap, opNonRpcMap, opRpcMap, loop=None, metadataClass=Metadata, name=None):
        self.opSerMap = opSerMap
        self.opNonRpcMap = opNonRpcMap
        self.opRpcMap = opRpcMap
        self.state = ConnectionState.CONNECTING
        # Most recently received rpc id
        self.peerRpcId = -1
        self.rpcId = 0  # 0 is for non-rpc (fire-and-forget)
        self.rpcFutureMap = dict()
        loop = loop if loop else asyncio.get_event_loop()
        self.activeFuture = loop.create_future()
        self.closeFuture = loop.create_future()
        self.metadataClass = metadataClass
        if name is None:
            name = "conn_{}".format(self.__get_next_connection_id())
        self.name = name

    async def read_metadata_and_raw_data(self):
        raise NotImplementedError()

    def write_raw_data(self, metadata, rawData):
        raise NotImplementedError()

    def __parse_command(self, rawData):
        op = rawData[0]
        rpcId = int.from_bytes(rawData[1:9], byteorder="big")
        ser = self.opSerMap[op]
        cmd = ser.deserialize(rawData[9:])
        return op, cmd, rpcId

    async def read_command(self):
        # TODO: distinguish clean disconnect or unexpected disconnect
        try:
            metadata, rawData = await self.read_metadata_and_raw_data()
            if metadata is None:
                return (None, None, None)
        except Exception as e:
            self.close_with_error("Error reading command: {}".format(e))
            return (None, None, None)
        op, cmd, rpcId = self.__parse_command(rawData)

        # we don't return the metadata to not break the existing code
        return (op, cmd, rpcId)

    def write_raw_command(self, op, cmdData, rpcId=0, metadata=None):
        metadata = metadata if metadata else self.metadataClass()
        ba = bytearray()
        ba.append(op)
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(cmdData)
        self.write_raw_data(metadata, ba)

    def write_command(self, op, cmd, rpcId=0, metadata=None):
        data = cmd.serialize()
        self.write_raw_command(op, data, rpcId, metadata)

    def write_rpc_request(self, op, cmd, metadata=None):
        rpcFuture = asyncio.Future()

        if self.state != ConnectionState.ACTIVE:
            rpcFuture.set_exception(RuntimeError(
                "Peer connection is not active"))
            return rpcFuture

        self.rpcId += 1
        rpcId = self.rpcId
        self.rpcFutureMap[rpcId] = rpcFuture
        self.write_command(op, cmd, rpcId, metadata)
        return rpcFuture

    def __write_rpc_response(self, op, cmd, rpcId, metadata):
        self.write_command(op, cmd, rpcId, metadata)

    async def __handle_request(self, op, request):
        handler = self.opNonRpcMap[op]
        # TODO: remove rpcid from handler signature
        await handler(self, op, request, 0)

    async def __handle_rpc_request(self, op, request, rpcId, metadata):
        respOp, handler = self.opRpcMap[op]
        resp = await handler(self, request)
        self.__write_rpc_response(respOp, resp, rpcId, metadata)

    def validate_and_update_peer_rpc_id(self, metadata, rpcId):
        if rpcId <= self.peerRpcId:
            raise RuntimeError("incorrect rpc request id sequence")
        self.peerRpcId = rpcId

    async def handle_metadata_and_raw_data(self, metadata, rawData):
        ''' Subclass can override this to provide customized handler '''
        op, cmd, rpcId = self.__parse_command(rawData)

        if op not in self.opSerMap:
            raise RuntimeError("{}: unsupported op {}".format(self.name, op))

        if op in self.opNonRpcMap:
            if rpcId != 0:
                raise RuntimeError("{}: non-rpc command's id must be zero".format(self.name))
            await self.__handle_request(op, cmd)
        elif op in self.opRpcMap:
            # Check if it is a valid RPC request
            self.validate_and_update_peer_rpc_id(metadata, rpcId)

            await self.__handle_rpc_request(op, cmd, rpcId, metadata)
        else:
            # Check if it is a valid RPC response
            if rpcId not in self.rpcFutureMap:
                raise RuntimeError("{}: unexpected rpc response {}".format(self.name, rpcId))
            future = self.rpcFutureMap[rpcId]
            del self.rpcFutureMap[rpcId]
            future.set_result((op, cmd, rpcId))

    async def __internal_handle_metadata_and_raw_data(self, metadata, rawData):
        try:
            await self.handle_metadata_and_raw_data(metadata, rawData)
        except Exception as e:
            Logger.logException()
            self.close_with_error("{}: error processing request: {}".format(self.name, e))

    async def loop_once(self):
        try:
            metadata, rawData = await self.read_metadata_and_raw_data()
            if metadata is None:
                # Hit EOF
                self.close()
                return
        except Exception as e:
            Logger.logException()
            self.close_with_error("{}: error reading request: {}".format(self.name, e))
            return

        asyncio.ensure_future(self.__internal_handle_metadata_and_raw_data(metadata, rawData))

    async def active_and_loop_forever(self):
        if self.state == ConnectionState.CONNECTING:
            self.state = ConnectionState.ACTIVE
            self.activeFuture.set_result(None)
        while self.state == ConnectionState.ACTIVE:
            await self.loop_once()

        assert(self.state == ConnectionState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("{}: connection abort".format(self.name)))
        AbstractConnection.abortedRpcCount += len(self.rpcFutureMap)
        self.rpcFutureMap.clear()

    async def wait_until_active(self):
        await self.activeFuture

    async def wait_until_closed(self):
        await self.closeFuture

    def close(self):
        if self.state != ConnectionState.CLOSED:
            self.state = ConnectionState.CLOSED
            self.closeFuture.set_result(None)

    def close_with_error(self, error):
        self.close()
        return error

    def is_active(self):
        return self.state == ConnectionState.ACTIVE

    def is_closed(self):
        return self.state == ConnectionState.CLOSED


class Connection(AbstractConnection):
    ''' A TCP/IP connection based on socket stream
    '''

    def __init__(
            self,
            env,
            reader,
            writer,
            opSerMap,
            opNonRpcMap,
            opRpcMap,
            loop=None,
            metadataClass=Metadata,
            name=None):
        loop = loop if loop else asyncio.get_event_loop()
        super().__init__(opSerMap, opNonRpcMap, opRpcMap, loop, metadataClass, name=name)
        self.env = env
        self.reader = reader
        self.writer = writer

    async def __read_fully(self, n, allowEOF=False):
        ba = bytearray()
        bs = await self.reader.read(n)
        if allowEOF and len(bs) == 0 and self.reader.at_eof():
            return None

        ba.extend(bs)
        while len(ba) < n:
            bs = await self.reader.read(n - len(ba))
            if len(bs) == 0 and self.reader.at_eof():
                raise RuntimeError("{}: read unexpected EOF".format(self.name))
            ba.extend(bs)
        return ba

    async def read_metadata_and_raw_data(self):
        ''' Override AbstractConnection.read_metadata_and_raw_data()
        '''
        sizeBytes = await self.__read_fully(4, allowEOF=True)
        if sizeBytes is None:
            self.close()
            return None, None
        size = int.from_bytes(sizeBytes, byteorder="big")

        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            raise RuntimeError("{}: command package exceed limit".format(self.name))

        metadataBytes = await self.__read_fully(self.metadataClass.get_byte_size())
        metadata = self.metadataClass.deserialize(metadataBytes)

        rawDataWithoutSize = await self.__read_fully(1 + 8 + size)
        return metadata, rawDataWithoutSize

    def write_raw_data(self, metadata, rawData):
        ''' Override AbstractConnection.write_raw_data()
        '''
        cmdLengthBytes = (len(rawData) - 8 - 1).to_bytes(4, byteorder="big")
        self.writer.write(cmdLengthBytes)
        self.writer.write(metadata.serialize())
        self.writer.write(rawData)

    def close(self):
        ''' Override AbstractConnection.close()
        '''
        self.writer.close()
        super().close()
