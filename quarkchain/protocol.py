
import asyncio
from enum import Enum


class ClientState(Enum):
    CONNECTING = 0   # connecting before the client can be used
    ACTIVE = 1       # the peer is active
    CLOSED = 2       # the peer connection is closed


class Client:

    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap):
        self.env = env
        self.reader = reader
        self.writer = writer
        self.opSerMap = opSerMap
        self.opNonRpcMap = opNonRpcMap
        self.opRpcMap = opRpcMap
        self.state = ClientState.CONNECTING
        # Most recently received rpc id
        self.peerRpcId = -1
        self.rpcId = 0  # 0 is for non-rpc (fire-and-forget)
        self.rpcFutureMap = dict()

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

        if rpcId <= self.peerRpcId:
            self.closeWithError("incorrect rpc id sequence")
            return (None, None, None)

        self.peerRpcId = max(rpcId, self.peerRpcId)
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

        if self.state != ClientState.ACTIVE:
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
        while self.state == ClientState.ACTIVE:
            try:
                op, cmd, rpcId = await self.readCommand()
            except Exception as e:
                self.closeWithError("Error when reading {}".format(e))
                break

            if op is None:
                break

            if op in self.opNonRpcMap:
                handler = self.opNonRpcMap[op]
                asyncio.ensure_future(handler(self, op, cmd, rpcId))
            elif op in self.opRpcMap:
                respOp, handler = self.opRpcMap[op]
                asyncio.ensure_future(self.handleRpcRequest(cmd, respOp, handler, rpcId))
            else:
                if rpcId not in self.rpcFutureMap:
                    self.closeWithError("Unexpected rpc response %d" % rpcId)
                    break
                future = self.rpcFutureMap[rpcId]
                del self.rpcFutureMap[rpcId]
                future.set_result((op, cmd, rpcId))
        assert(self.state == ClientState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("Connection abort"))
        self.rpcFutureMap.clear()

    def close(self):
        self.writer.close()
        self.state = ClientState.CLOSED

    def closeWithError(self, error):
        self.close()
        return error
