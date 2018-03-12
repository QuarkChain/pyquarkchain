import asyncio
from enum import Enum

from quarkchain.utils import Logger


class ConnectionState(Enum):
    CONNECTING = 0   # connecting before the Connection can be used
    ACTIVE = 1       # the peer is active
    CLOSED = 2       # the peer connection is closed


class Connection:

    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None):
        self.env = env
        self.reader = reader
        self.writer = writer
        self.opSerMap = opSerMap
        self.opNonRpcMap = opNonRpcMap
        self.opRpcMap = opRpcMap
        self.state = ConnectionState.CONNECTING
        # Most recently received rpc id
        self.peerRpcId = -1
        self.rpcId = 0  # 0 is for non-rpc (fire-and-forget)
        self.rpcFutureMap = dict()
        loop = loop if loop else asyncio.get_event_loop()
        self.closeFuture = loop.create_future()

    async def readFully(self, n):
        bs = await self.reader.read(n)
        while len(bs) < n:
            nbs = await self.reader.read(n - len(bs))
            if len(nbs) == 0 and self.reader.at_eof():
                raise RuntimeError("read unexpected EOF")
            bs = bs + nbs
        return bs

    async def readCommand(self):
        opBytes = await self.reader.read(1)
        if len(opBytes) == 0:
            self.close()
            return (None, None, None)

        op = opBytes[0]
        if op not in self.opSerMap:
            self.closeWithError("Unsupported op {}".format(op))
            return (None, None, None)

        ser = self.opSerMap[op]
        sizeBytes = await self.readFully(4)
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            self.closeWithError("command package exceed limit")
            return (None, None, None)

        rpcIdBytes = await self.readFully(8)
        rpcId = int.from_bytes(rpcIdBytes, byteorder="big")

        cmdBytes = await self.readFully(size)
        try:
            cmd = ser.deserialize(cmdBytes)
        except Exception as e:
            self.closeWithError("%s" % e)
            return (None, None, None)

        return (op, cmd, rpcId)

    def writeRawCommand(self, op, cmdData, rpcId=0):
        ba = bytearray()
        ba.append(op)
        ba.extend(len(cmdData).to_bytes(4, byteorder="big"))
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(cmdData)
        self.writer.write(ba)

    def writeCommand(self, op, cmd, rpcId=0):
        data = cmd.serialize()
        self.writeRawCommand(op, data, rpcId)

    def writeRpcRequest(self, op, cmd):
        rpcFuture = asyncio.Future()

        if self.state != ConnectionState.ACTIVE:
            rpcFuture.set_exception(RuntimeError(
                "Peer connection is not active"))
            return rpcFuture

        self.rpcId += 1
        rpcId = self.rpcId
        self.rpcFutureMap[rpcId] = rpcFuture
        self.writeCommand(op, cmd, rpcId)
        return rpcFuture

    def writeRpcResponse(self, op, cmd, rpcId):
        self.writeCommand(op, cmd, rpcId)

    async def handleRpcRequest(self, request, respOp, handler, rpcId):
        try:
            resp = await handler(self, request)
        except Exception as e:
            self.closeWithError("Unable to process rpc: {}".format(e))
            Logger.errorExceptionEverySec(1)
            return

        self.writeRpcResponse(respOp, resp, rpcId)

    async def activeAndLoopForever(self):
        if self.state == ConnectionState.CONNECTING:
            self.state = ConnectionState.ACTIVE
        while self.state == ConnectionState.ACTIVE:
            try:
                op, cmd, rpcId = await self.readCommand()
            except Exception as e:
                self.closeWithError("Error when reading {}".format(e))
                break

            if op is None:
                break

            if op in self.opNonRpcMap:
                if rpcId != 0:
                    self.closeWithError("non-rpc command's id must be zero")
                    break
                handler = self.opNonRpcMap[op]
                asyncio.ensure_future(handler(self, op, cmd, rpcId))
            elif op in self.opRpcMap:
                # Check if it is a valid RPC request
                if rpcId <= self.peerRpcId:
                    self.closeWithError("incorrect rpc request id sequence")
                    break
                self.peerRpcId = max(rpcId, self.peerRpcId)

                respOp, handler = self.opRpcMap[op]
                asyncio.ensure_future(self.handleRpcRequest(cmd, respOp, handler, rpcId))
            else:
                # Check if it is a valid RPC response
                if rpcId not in self.rpcFutureMap:
                    self.closeWithError("Unexpected rpc response %d" % rpcId)
                    break
                future = self.rpcFutureMap[rpcId]
                del self.rpcFutureMap[rpcId]
                future.set_result((op, cmd, rpcId))
        assert(self.state == ConnectionState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("Connection abort"))
        self.rpcFutureMap.clear()

    async def waitUntilClosed(self):
        await self.closeFuture

    def close(self):
        self.writer.close()
        if self.state != ConnectionState.CLOSED:
            self.state = ConnectionState.CLOSED
            self.closeFuture.set_result(None)

    def closeWithError(self, error):
        self.close()
        return error
