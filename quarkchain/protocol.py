import asyncio
from enum import Enum

from quarkchain.utils import Logger


ROOT_SHARD_ID = 2 * 32 - 1


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
        self.activeFuture = loop.create_future()
        self.closeFuture = loop.create_future()

    def getConnectionToForward(self, shardId):
        ''' Returns the Connection object to forward a request for shardId.
        Returns None if the request should not be forwarded for shardId.

        Subclass should override this if the shardId is on another node and this node
        will forward the request without deserialize the request.

        shardId is a uint32, the max value (2**32-1) represents the root chain.

        For example:
        Assuming requests are sent to shards and master does the forwarding.

        1. slave -> master -> peer
            For requests coming from the slaves inside the cluster, this should return
            a connection to the peer

        2. peer -> master -> slave
            For requests coming from other peers, this should return a connection
            to the slave running the shard
        '''
        return None

    async def readFully(self, n):
        bs = await self.reader.read(n)
        while len(bs) < n:
            nbs = await self.reader.read(n - len(bs))
            if len(nbs) == 0 and self.reader.at_eof():
                raise RuntimeError("read unexpected EOF")
            bs = bs + nbs
        return bs

    async def readRawCommand(self):
        shardIdBytes = await self.readFully(4)
        shardId = int.from_bytes(shardIdBytes, byteorder="big")

        opBytes = await self.reader.read(1)
        if len(opBytes) == 0:
            self.close()
            return (None, None, None)

        op = opBytes[0]
        if op not in self.opSerMap:
            self.closeWithError("Unsupported op {}".format(op))
            return (None, None, None)

        sizeBytes = await self.readFully(4)
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            self.closeWithError("command package exceed limit")
            return (None, None, None)

        rpcIdBytes = await self.readFully(8)
        rpcId = int.from_bytes(rpcIdBytes, byteorder="big")

        cmdBytes = await self.readFully(size)

        return (shardId, op, cmdBytes, rpcId)

    def __deserializeCommand(self, op, rawCommand):
        ser = self.opSerMap[op]
        cmd = ser.deserialize(rawCommand)
        return cmd

    async def readCommand(self):
        shardId, op, cmdBytes, rpcId = await self.readRawCommand()
        cmd = self.__deserializeCommand(op, cmdBytes)
        # we don't return the shardId to not break the existing code
        return (op, cmd, rpcId)

    def writeRawCommand(self, op, cmdData, rpcId=0, shardId=ROOT_SHARD_ID):
        ba = bytearray()
        ba.extend(shardId.to_bytes(4, byteorder="big"))
        ba.append(op)
        ba.extend(len(cmdData).to_bytes(4, byteorder="big"))
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(cmdData)
        self.writer.write(ba)

    def writeCommand(self, op, cmd, rpcId=0, shardId=ROOT_SHARD_ID):
        data = cmd.serialize()
        self.writeRawCommand(op, data, rpcId, shardId)

    def writeRawRpcRequest(self, op, rawCommand, shardId=ROOT_SHARD_ID):
        rpcFuture = asyncio.Future()

        if self.state != ConnectionState.ACTIVE:
            rpcFuture.set_exception(RuntimeError(
                "Peer connection is not active"))
            return rpcFuture

        self.rpcId += 1
        rpcId = self.rpcId
        self.rpcFutureMap[rpcId] = rpcFuture
        self.writeRawCommand(op, rawCommand, rpcId, shardId)
        return rpcFuture

    def writeRpcRequest(self, op, cmd, shardId=ROOT_SHARD_ID):
        return self.writeRawRpcRequest(op, cmd.serialize(), shardId)

    def writeRawRpcResponse(self, op, rawCmd, rpcId, shardId=ROOT_SHARD_ID):
        self.writeRawCommand(op, rawCmd, rpcId, shardId)

    def writeRpcResponse(self, op, cmd, rpcId, shardId=ROOT_SHARD_ID):
        self.writeCommand(op, cmd, rpcId, shardId)

    async def handleRpcRequest(self, request, respOp, handler, rpcId, shardId):
        try:
            resp = await handler(self, request)
        except Exception as e:
            self.closeWithError("Unable to process rpc: {}".format(e))
            Logger.errorExceptionEverySec(1)
            return

        self.writeRpcResponse(respOp, resp, rpcId, shardId)

    async def forwardRpcRequest(self, forwardConn, op, rawCmd, rpcId, shardId):
        try:
            respOp, rawResp, _ = await forwardConn.writeRawRpcRequest(op, rawCmd, shardId)
        except Exception as e:
            self.closeWithError("Unable to forward rpc: {}".format(e))
            Logger.errorExceptionEverySec(1)
            return

        self.writeRawRpcResponse(respOp, rawResp, rpcId, shardId)

    async def activeAndLoopForever(self):
        if self.state == ConnectionState.CONNECTING:
            self.state = ConnectionState.ACTIVE
            self.activeFuture.set_result(None)
        while self.state == ConnectionState.ACTIVE:
            try:
                shardId, op, rawCmd, rpcId = await self.readRawCommand()
                forwardConn = self.getConnectionToForward(shardId)
                if forwardConn is None:
                    cmd = self.__deserializeCommand(op, rawCmd)
            except Exception as e:
                self.closeWithError("Error when reading {}".format(e))
                break

            if op is None:
                break

            if op in self.opNonRpcMap:
                if rpcId != 0:
                    self.closeWithError("non-rpc command's id must be zero")
                    break
                if forwardConn is None:
                    handler = self.opNonRpcMap[op]
                    asyncio.ensure_future(handler(self, op, cmd, rpcId))
                else:
                    forwardConn.writeRawCommand(op, rawCmd, shardId)
            elif op in self.opRpcMap:
                # Check if it is a valid RPC request
                if rpcId <= self.peerRpcId:
                    self.closeWithError("incorrect rpc request id sequence")
                    break
                self.peerRpcId = max(rpcId, self.peerRpcId)

                if forwardConn is None:
                    respOp, handler = self.opRpcMap[op]
                    asyncio.ensure_future(self.handleRpcRequest(cmd, respOp, handler, rpcId, shardId))
                else:
                    asyncio.ensure_future(self.forwardRpcRequest(forwardConn, op, rawCmd, rpcId, shardId))
            else:
                # Check if it is a valid RPC response
                if rpcId not in self.rpcFutureMap:
                    self.closeWithError("Unexpected rpc response %d" % rpcId)
                    break
                future = self.rpcFutureMap[rpcId]
                del self.rpcFutureMap[rpcId]
                if forwardConn is None:
                    future.set_result((op, cmd, rpcId))
                else:
                    future.set_result((op, rawCmd, rpcId))
        assert(self.state == ConnectionState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("Connection abort"))
        self.rpcFutureMap.clear()

    async def waitUntilActive(self):
        await self.activeFuture

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

    def isActive(self):
        return self.state == ConnectionState.ACTIVE

    def isClosed(self):
        return self.state == ConnectionState.CLOSED
