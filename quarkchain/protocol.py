
import asyncio
from enum import Enum


class ConnectionState(Enum):
    CONNECTING = 0   # connecting before the Connection can be used
    ACTIVE = 1       # the peer is active
    CLOSED = 2       # the peer connection is closed


class Connection:

    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap):
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
        self.closeFuture = asyncio.Future()

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
        sizeBytes = await self.reader.read(4)
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            self.closeWithError("command package exceed limit")
            return (None, None, None)

        rpcIdBytes = await self.reader.read(8)
        rpcId = int.from_bytes(rpcIdBytes, byteorder="big")

        cmdBytes = await self.reader.read(size)
        try:
            cmd = ser.deserialize(cmdBytes)
        except Exception as e:
            self.closeWithError("%s" % e)
            return (None, None, None)

        return (op, cmd, rpcId)

    def writeCommand(self, op, cmd, rpcId=0):
        data = cmd.serialize()
        ba = bytearray()
        ba.append(op)
        ba.extend(len(data).to_bytes(4, byteorder="big"))
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(data)
        self.writer.write(ba)

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
            return

        self.writeRpcResponse(respOp, resp, rpcId)

    async def loopForever(self):
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
