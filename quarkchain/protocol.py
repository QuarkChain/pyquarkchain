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
    def getByteSize():
        ''' Returns the size (in bytes) of the serialized object '''
        return 0


class AbstractConnection:
    connId = 0
    abortedRpcCount = 0

    @classmethod
    def __getNextConnectionId(cls):
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
            name = "conn_{}".format(self.__getNextConnectionId())
        self.name = name

    async def readMetadataAndRawData(self):
        raise NotImplementedError()

    def writeRawData(self, metadata, rawData):
        raise NotImplementedError()

    def __parseCommand(self, rawData):
        op = rawData[0]
        rpcId = int.from_bytes(rawData[1:9], byteorder="big")
        ser = self.opSerMap[op]
        cmd = ser.deserialize(rawData[9:])
        return op, cmd, rpcId

    async def readCommand(self):
        # TODO: distinguish clean disconnect or unexpected disconnect
        try:
            metadata, rawData = await self.readMetadataAndRawData()
            if metadata is None:
                return (None, None, None)
        except Exception as e:
            self.closeWithError("Error reading command: {}".format(e))
            return (None, None, None)
        op, cmd, rpcId = self.__parseCommand(rawData)

        # we don't return the metadata to not break the existing code
        return (op, cmd, rpcId)

    def writeRawCommand(self, op, cmdData, rpcId=0, metadata=None):
        metadata = metadata if metadata else self.metadataClass()
        ba = bytearray()
        ba.append(op)
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(cmdData)
        self.writeRawData(metadata, ba)

    def writeCommand(self, op, cmd, rpcId=0, metadata=None):
        data = cmd.serialize()
        self.writeRawCommand(op, data, rpcId, metadata)

    def writeRpcRequest(self, op, cmd, metadata=None):
        rpcFuture = asyncio.Future()

        if self.state != ConnectionState.ACTIVE:
            rpcFuture.set_exception(RuntimeError(
                "Peer connection is not active"))
            return rpcFuture

        self.rpcId += 1
        rpcId = self.rpcId
        self.rpcFutureMap[rpcId] = rpcFuture
        self.writeCommand(op, cmd, rpcId, metadata)
        return rpcFuture

    def __writeRpcResponse(self, op, cmd, rpcId, metadata):
        self.writeCommand(op, cmd, rpcId, metadata)

    async def __handleRequest(self, op, request):
        handler = self.opNonRpcMap[op]
        # TODO: remove rpcid from handler signature
        await handler(self, op, request, 0)

    async def __handleRpcRequest(self, op, request, rpcId, metadata):
        respOp, handler = self.opRpcMap[op]
        resp = await handler(self, request)
        self.__writeRpcResponse(respOp, resp, rpcId, metadata)

    def validateAndUpdatePeerRpcId(self, metadata, rpcId):
        if rpcId <= self.peerRpcId:
            raise RuntimeError("incorrect rpc request id sequence")
        self.peerRpcId = rpcId

    async def handleMetadataAndRawData(self, metadata, rawData):
        ''' Subclass can override this to provide customized handler '''
        op, cmd, rpcId = self.__parseCommand(rawData)

        if op not in self.opSerMap:
            raise RuntimeError("{}: unsupported op {}".format(self.name, op))

        if op in self.opNonRpcMap:
            if rpcId != 0:
                raise RuntimeError("{}: non-rpc command's id must be zero".format(self.name))
            await self.__handleRequest(op, cmd)
        elif op in self.opRpcMap:
            # Check if it is a valid RPC request
            self.validateAndUpdatePeerRpcId(metadata, rpcId)

            await self.__handleRpcRequest(op, cmd, rpcId, metadata)
        else:
            # Check if it is a valid RPC response
            if rpcId not in self.rpcFutureMap:
                raise RuntimeError("{}: unexpected rpc response {}".format(self.name, rpcId))
            future = self.rpcFutureMap[rpcId]
            del self.rpcFutureMap[rpcId]
            future.set_result((op, cmd, rpcId))

    async def __internalHandleMetadataAndRawData(self, metadata, rawData):
        try:
            await self.handleMetadataAndRawData(metadata, rawData)
        except Exception as e:
            Logger.logException()
            self.closeWithError("{}: error processing request: {}".format(self.name, e))

    async def loopOnce(self):
        try:
            metadata, rawData = await self.readMetadataAndRawData()
            if metadata is None:
                # Hit EOF
                self.close()
                return
        except Exception as e:
            Logger.logException()
            self.closeWithError("{}: error reading request: {}".format(self.name, e))
            return

        asyncio.ensure_future(self.__internalHandleMetadataAndRawData(metadata, rawData))

    async def activeAndLoopForever(self):
        if self.state == ConnectionState.CONNECTING:
            self.state = ConnectionState.ACTIVE
            self.activeFuture.set_result(None)
        while self.state == ConnectionState.ACTIVE:
            await self.loopOnce()

        assert(self.state == ConnectionState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("{}: connection abort".format(self.name)))
        AbstractConnection.abortedRpcCount += len(self.rpcFutureMap)
        self.rpcFutureMap.clear()

    async def waitUntilActive(self):
        await self.activeFuture

    async def waitUntilClosed(self):
        await self.closeFuture

    def close(self):
        if self.state != ConnectionState.CLOSED:
            self.state = ConnectionState.CLOSED
            self.closeFuture.set_result(None)

    def closeWithError(self, error):
        self.close()
        return error

    def isActive(self):
        return self.state == ConnectionState.ACTIVE

    def isClosed(self):
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

    async def __readFully(self, n, allowEOF=False):
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

    async def readMetadataAndRawData(self):
        ''' Override AbstractConnection.readMetadataAndRawData()
        '''
        sizeBytes = await self.__readFully(4, allowEOF=True)
        if sizeBytes is None:
            self.close()
            return None, None
        size = int.from_bytes(sizeBytes, byteorder="big")

        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            raise RuntimeError("{}: command package exceed limit".format(self.name))

        metadataBytes = await self.__readFully(self.metadataClass.getByteSize())
        metadata = self.metadataClass.deserialize(metadataBytes)

        rawDataWithoutSize = await self.__readFully(1 + 8 + size)
        return metadata, rawDataWithoutSize

    def writeRawData(self, metadata, rawData):
        ''' Override AbstractConnection.writeRawData()
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
