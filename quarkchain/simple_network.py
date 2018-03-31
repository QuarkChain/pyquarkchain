import argparse
import asyncio
import ipaddress
import random
import socket

from quarkchain.core import random_bytes
from quarkchain.config import DEFAULT_ENV
from quarkchain.chain import QuarkChainState
from quarkchain.protocol import Connection, ConnectionState
from quarkchain.local import LocalServer
from quarkchain.db import PersistentDb
from quarkchain.commands import *
from quarkchain.utils import set_logging_level, Logger, check


class Downloader():
    """ A downloader from a peer.
    """

    def __init__(self, peer):
        self.peer = peer

    async def getRootBlockByHash(self, h):
        return None

    async def getMinorBlockByHash(self, h):
        return None

    async def getPreviousMinorBlockHeaderList(self, h, maxBlocks=1):
        """ Get the previous minor block header starting from h
        The returned list must be ordered.
        Return empty if the minor block of the hash is not in the active chain.
        """
        return []

    async def getPreviousRootBlockHeaderList(self, h, maxBlocks=1):
        """ Get the previous root block header starting from h
        The returned list must be ordered.
        Return empty if the root block of the hash is not in the active chain.
        """
        return []

    async def isPeerClosed(self):
        return self.peer.isClosed()

    async def closePeerWithError(self, error):
        return self.peer.closeWithError(error)


class RootForkResolver():

    def __init__(self, stateContainer, downloader, header):
        self.stateContainer = stateContainer
        self.downloader = downloader
        self.header = header

    async def __resolve(self):
        tip = self.stateContainer.qcState.getRootBlockTip()

        if tip.height >= self.header.height:
            return

        parentHeader = None
        if tip.getHash() == self.header.hashPrevBlock:
            if tip.height + 1 != self.header.height:
                raise RuntimeError("RootForkResolver: height mismatches")
            parentHeader = tip

        currentHash = self.header.getHash()
        currentHeader = self.header
        rHeaderList = [self.header]
        while parentHeader is None:
            hList = await self.downloader.getPreviousRootBlockHeaderList(currentHash, maxBlocks=10)
            if len(hList) == 0:
                # The root chain in peer has changed
                # TODO: download latest tip from the peer immediately
                return

            for rHeader in hList:
                if currentHeader.hashPrevBlock != rHeader.getHash():
                    raise RuntimeError("RootForkResolver: hashPrevBlock mismatches current hash!")
                if currentHeader.height != rHeader.height + 1:
                    raise RuntimeError("RootForkResolver: header height mismatches")

                currentHash = rHeader.getHash()
                currentHeader = rHeader

                rHeaderList.append(rHeader)
                if currentHeader.height <= tip.height + 1:
                    header = self.stateContainer.qcState.getRootBlockHeaderByHash(
                        currentHeader.hashPrevBlock)
                    if header is not None:
                        if header.height + 1 != currentHeader.height:
                            raise RuntimeError("RootForkResolver: incorrect header height from peer")

                        parentHeader = header
                        break

            # TODO: Check rHeaderList length and may stop resolving the fork if the difference is too large

        # TODO: Check difficulty and other sanity check before downloading
        # TODO: Check local db before downloading
        rBlockList = []
        for rHeader in rHeaderList:
            rBlock = await self.downloader.getRootBlockByHash(rHeader.getHash())
            if rBlock is None:
                # Active chain is changed
                return
            rBlockList.append(rBlock)

            # Local miner may append new blocks.  Make sure the peer still has the longer root chain.
            if self.header.height <= self.stateContainer.qcState.getRootBlockTip().height:
                return

        # Resolve shards if needed
        # TODO: perform parallel shard block downloading
        self.qcState = self.stateContainer.qcState.copy()
        self.qcState.rollBackRootChainTo(parentHeader)
        for rBlock in reversed(rBlockList):
            for mHeader in reversed(rBlock.minorBlockHeaderList):
                shardId = mHeader.branch.getShardId()
                if self.qcState.getMinorBlockHeaderByHash(mHeader.getHash(), shardId) is None:
                    # Cannot find the minor block in the shard
                    # Try to roll back the shard so that tip height is smaller than mHeader's height and
                    # thus run ShardForkResolver to address the following of the fork.
                    while self.qcState.getShardTip(shardId).height >= mHeader.height:
                        errMsg = self.qcState.rollBackMinorBlock(shardId)
                        if errMsg is not None:
                            raise RuntimeError(errMsg)

                    await ShardForkResolver(self, self.downloader, mHeader).resolve()
                    if self.qcState.getMinorBlockHeaderByHash(mHeader.getHash()) is None:
                        # Failed to resolve, the peer should be close by error
                        raise RuntimeError("unable to resolve the shard")
            errMsg = self.qcState.appendRootBlock(rBlock)
            if errMsg is not None:
                raise RuntimeError(errMsg)

        self.stateContainer.qcState = self.qcState

    async def resolve(self):
        try:
            await self.__resolve()
        except Exception as e:
            self.downloader.closePeerWithError(str(e))
            raise e


class ShardForkResolver():

    def __init__(self, stateContainer, downloader, header):
        self.stateContainer = stateContainer
        self.downloader = downloader
        self.header = header
        self.branch = header.branch

    async def __resolve(self):
        shardId = self.header.branch.getShardId()
        tip = self.stateContainer.qcState.getShardTip(shardId)

        if self.stateContainer.qcState.getRootBlockHeaderByHash(self.header.hashPrevRootBlock) is None:
            # Cannot find the root block.  The fork must be resolved by root chain first.
            return

        if tip.height >= self.header.height:
            return

        # Fast-path: the header can be appended to the tip of the shard
        if tip.height + 1 == self.header.height and self.header.hashPrevMinorBlock == tip.getHash():
            mBlock = await self.downloader.getMinorBlockByHash(self.header.getHash())
            if mBlock is None:
                # Active chain is changed
                return
            errMsg = self.stateContainer.qcState.appendMinorBlock(mBlock)
            if errMsg is not None:
                raise RuntimeError(errMsg)
            return

        # Find the closest parent of the fork and current chain (uncommited part)
        commitedTip = self.stateContainer.qcState.getCommittedShardTip(shardId)
        mHeaderList = [self.header]
        currentHash = self.header.getHash()
        currentHeader = self.header
        parentHeader = None
        Logger.info("resolving shard {} fork with remote height {}, tip height {}".format(
            shardId, self.header.height, tip.height))
        while True:
            hList = await self.downloader.getPreviousMinorBlockHeaderList(currentHash, maxBlocks=10)
            if len(hList) == 0:
                # The shard in peer has changed
                # TODO: download latest tip from the peer immediately
                return

            for mHeader in hList:
                if currentHeader.hashPrevMinorBlock != mHeader.getHash():
                    raise RuntimeError("ShardForkResolver: hashPrevMinorBlock mismatches current hash!")
                if currentHeader.height != mHeader.height + 1:
                    raise RuntimeError("ShardForkResolver: header height mismatches")

                currentHash = mHeader.getHash()
                currentHeader = mHeader

                if currentHeader.height < commitedTip.height:
                    # Cannot resolve the fork until root chain is resolved
                    return

                mHeaderList.append(mHeader)
                if currentHeader.height <= tip.height + 1:
                    header = self.stateContainer.qcState.getMinorBlockHeaderByHash(
                        currentHeader.hashPrevMinorBlock, shardId)
                    if header is not None:
                        if header.height + 1 != currentHeader.height:
                            raise RuntimeError("ShardForkResolver: incorrect header height from peer")

                        parentHeader = header
                        break

            # TODO: Check mHeaderList length and may stop resolving the fork if the difference is too large
            if parentHeader is not None:
                break

        # TODO: Check difficulty before downloading
        # TODO: Check local db before downloading
        mBlockList = []
        for mHeader in mHeaderList:
            mBlock = await self.downloader.getMinorBlockByHash(mHeader.getHash())
            if mBlock is None:
                # Active chain is changed
                return
            mBlockList.append(mBlock)

            # Local miner may append new blocks.  Make sure the peer still has the longer shard.
            if self.header.height <= self.stateContainer.qcState.getShardTip(shardId).height:
                return

        # Apply blocks atomically.  Make sure we have latest copy of qcState
        # (as other resolver may change it in parallel).
        qcState = self.stateContainer.qcState.copy()
        while qcState.getShardTip(shardId) != parentHeader:
            qcState.rollBackMinorBlock(shardId)

        for mBlock in reversed(mBlockList):
            errMsg = qcState.appendMinorBlock(mBlock)
            if errMsg is not None:
                raise RuntimeError(errMsg)

        self.stateContainer.qcState = qcState

    async def resolve(self):
        try:
            await self.__resolve()
        except Exception as e:
            self.downloader.closePeerWithError(str(e))
            raise e


class ForkResolverManager:
    """ To save CPU and bandwidith, we only allow
    - One root fork resolver is running; or
    - Multiple shard fork resolvers are running, with each shard having at most one resolver.

    Current all resolvers are not cancelable at the monent.
    """

    def __init__(self, downloaderFactory):
        self.downloaderFactory = downloaderFactory
        self.rootForkResolver = None
        self.shardForkResolverMap = dict()
        self.completionFuture = None

    def getCompletionFuture(self):
        check(self.completionFuture is None)
        future = asyncio.get_event_loop().create_future()
        self.completionFuture = future
        self.__trySetCompletionFuture()
        return future

    def __trySetCompletionFuture(self):
        if self.completionFuture is None:
            return
        if len(self.shardForkResolverMap) == 0 and self.rootForkResolver is None:
            self.completionFuture.set_result(None)
            self.completionFuture = None

    async def __resolveRootFork(self):
        check(len(self.shardForkResolverMap) == 0)
        try:
            await self.rootForkResolver.resolve()
        except Exception as e:
            Logger.errorException()
            Logger.error("failed to resolve root fork {}".format(e))
        self.rootForkResolver = None
        self.__trySetCompletionFuture()

    def tryResolveRootFork(self, network, peer, rHeader):
        if self.rootForkResolver is not None:
            return False
        self.rootForkResolver = RootForkResolver(network, self.downloaderFactory(peer), rHeader)
        if len(self.shardForkResolverMap) == 0:
            asyncio.ensure_future(self.__resolveRootFork())
        return True

    async def __resolveShardFork(self, mHeader):
        try:
            await self.shardForkResolverMap[mHeader.branch].resolve()
        except Exception as e:
            Logger.errorException()
            Logger.error("failed to resolve shard fork {}".format(e))
        del self.shardForkResolverMap[mHeader.branch]
        if len(self.shardForkResolverMap) == 0 and self.rootForkResolver is not None:
            asyncio.ensure_future(self.__resolveRootFork())
        else:
            self.__trySetCompletionFuture()

    def tryResolveShardFork(self, network, peer, mHeader):
        if self.rootForkResolver is not None:
            return False
        if mHeader.branch in self.shardForkResolverMap:
            return False
        self.shardForkResolverMap[mHeader.branch] = ShardForkResolver(network, self.downloaderFactory(peer), mHeader)
        asyncio.ensure_future(self.__resolveShardFork(mHeader))
        return True


class Peer(Connection):

    def __init__(self, env, reader, writer, network):
        super().__init__(env, reader, writer, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP)
        self.network = network

        # The following fields should be set once active
        self.id = None
        self.shardMaskList = None
        self.bestRootBlockHeaderObserved = None

    def sendHello(self):
        cmd = HelloCommand(version=self.env.config.P2P_PROTOCOL_VERSION,
                           networkId=self.env.config.NETWORK_ID,
                           peerId=self.network.selfId,
                           peerIp=int(self.network.ip),
                           peerPort=self.network.port,
                           shardMaskList=[],
                           rootBlockHeader=RootBlockHeader())
        # Send hello request
        self.writeCommand(CommandOp.HELLO, cmd)

    async def start(self, isServer=False):
        op, cmd, rpcId = await self.readCommand()
        if op is None:
            assert(self.state == ConnectionState.CLOSED)
            return "Failed to read command"

        if op != CommandOp.HELLO:
            return self.closeWithError("Hello must be the first command")

        if cmd.version != self.env.config.P2P_PROTOCOL_VERSION:
            return self.closeWithError("incompatible protocol version")

        if cmd.networkId != self.env.config.NETWORK_ID:
            return self.closeWithError("incompatible network id")

        self.id = cmd.peerId
        self.shardMaskList = cmd.shardMaskList
        self.ip = ipaddress.ip_address(cmd.peerIp)
        self.port = cmd.peerPort
        self.bestRootBlockHeaderObserved = cmd.rootBlockHeader
        # TODO handle root block header
        if self.id == self.network.selfId:
            # connect to itself, stop it
            return self.closeWithError("Cannot connect to itself")

        if self.id in self.network.activePeerPool:
            return self.closeWithError("Peer %s already connected" % self.id)

        self.network.activePeerPool[self.id] = self
        Logger.info("Peer {} connected".format(self.id.hex()))

        # Send hello back
        if isServer:
            self.sendHello()

        asyncio.ensure_future(self.activeAndLoopForever())
        return None

    def close(self):
        if self.state == ConnectionState.ACTIVE:
            assert(self.id is not None)
            if self.id in self.network.activePeerPool:
                del self.network.activePeerPool[self.id]
            Logger.info("Peer {} disconnected, remaining {}".format(
                self.id.hex(), len(self.network.activePeerPool)))
        super().close()

    def closeWithError(self, error):
        Logger.info(
            "Closing peer %s with the following reason: %s" %
            (self.id.hex() if self.id is not None else "unknown", error))
        return super().closeWithError(error)

    async def handleError(self, op, cmd, rpcId):
        self.closeWithError("Unexpected op {}".format(op))

    async def handleNewMinorBlockHeaderList(self, op, cmd, rpcId):
        # Make sure the root block height is non-decreasing
        rHeader = cmd.rootBlockHeader
        if self.bestRootBlockHeaderObserved.height > rHeader.height:
            self.closeWithError("Root block height should be non-decreasing")
            return
        elif self.bestRootBlockHeaderObserved.height == rHeader.height:
            if self.bestRootBlockHeaderObserved != cmd.rootBlockHeader:
                self.closeWithError(
                    "Root block the same height should not be changed")
                return
            # TODO: Make sure the height of each shard is increasing
        elif rHeader.shardInfo.getShardSize() != self.bestRootBlockHeaderObserved.shardInfo.getShardSize():
            # TODO: Support reshard
            self.closeWithError("Incorrect root block shard size")
            return
        else:
            self.bestRootBlockHeaderObserved = rHeader

        if self.bestRootBlockHeaderObserved.height == rHeader.height:
            # Try to resolve all minor headers
            for mHeader in cmd.minorBlockHeaderList:
                if mHeader.branch.getShardSize() != rHeader.shardInfo.getShardSize():
                    self.closeWithError("Incorrect minor block shard size")
                    return
                if mHeader.height <= self.network.qcState.getShardTip(mHeader.branch.getShardId()):
                    continue
                self.network.forkResolverManager.tryResolveShardFork(self.network, Downloader(self), mHeader)
        elif self.bestRootBlockHeaderObserved.height > rHeader.height:
            self.network.forkResolverManager.tryResolveRootFork(
                self.network, Downloader(self), self.bestRootBlockHeaderObserved)
        else:
            pass

    def broadcastNewBlockCommand(self, cmd):
        pass

    async def handleNewBlockCommand(self, op, cmd, rpcId):
        # New block is arrived.  This only applies to simple network with one miner.
        if cmd.isRootBlock:
            try:
                rBlock = RootBlock.deserialize(cmd.blockData)
            except Exception as e:
                Logger.logException()
                self.closeWithError("failed to deserialize root block")

            Logger.info("[R] Received block with height {}, local height {}".format(
                rBlock.header.height, self.network.qcState.getRootBlockTip().height))
            heightExpected = self.network.qcState.getRootBlockTip().height + 1
            if rBlock.header.height > heightExpected:
                await self.network.sync()
                return
            elif rBlock.header.height < heightExpected:
                return
            errorMsg = self.network.qcState.appendRootBlock(rBlock)
            if errorMsg is None:
                self.broadcastNewBlockCommand(cmd)
            else:
                Logger.info("[R] Failed to append block {}".format(rBlock.header.height))
        else:
            try:
                mBlock = MinorBlock.deserialize(cmd.blockData)
            except Exception as e:
                Logger.logException()
                self.closeWithError("failed to deserialize minor block")

            Logger.info("[{}] Received block with height {}".format(
                mBlock.header.branch.getShardId(),
                mBlock.header.height))

            if mBlock.header.branch.getShardSize() != self.network.qcState.getShardSize():
                self.closeWithError("new block with mismatched shard size")

            shardId = mBlock.header.branch.getShardId()
            heightExpected = self.network.qcState.getMinorBlockTip(shardId).height + 1
            if mBlock.header.height > heightExpected:
                await self.network.sync()
                return
            elif mBlock.header.height < heightExpected:
                return

            errorMsg = self.network.qcState.appendMinorBlock(mBlock)
            if errorMsg is None:
                self.broadcastNewBlockCommand(cmd)
            else:
                Logger.info("[{}] Failed to append block {}".format(
                    mBlock.header.branch.getShardId(),
                    mBlock.header.height))

    async def handleNewTransactionList(self, op, cmd, rpcId):
        for newTransaction in cmd.transactionList:
            Logger.info("[{}] Received transaction {}".format(
                newTransaction.shardId,
                newTransaction.transaction.getHashHex()))
            self.network.qcState.addTransactionToQueue(newTransaction.shardId, newTransaction.transaction)

    async def handleGetRootBlockListRequest(self, request):
        qcState = self.network.qcState
        blockList = []
        for h in request.rootBlockHashList:
            blockList.append(qcState.db.getRootBlockByHash(h))
        return GetRootBlockListResponse(blockList)

    async def handleGetMinorBlockListRequest(self, request):
        qcState = self.network.qcState
        blockList = []
        for h in request.minorBlockHashList:
            blockList.append(qcState.db.getMinorBlockByHash(h))
        return GetMinorBlockListResponse(blockList)

    async def handleGetBlockHashListRequest(self, request):
        qcState = self.network.qcState
        if request.isRoot:
            hList = qcState.getRootBlockHeaderListByHash(request.blockHash, request.maxBlocks, request.direction)
            if hList is not None:
                return GetBlockHashListResponse(
                    rootTip=qcState.getRootBlockTip(),
                    shardTip=MinorBlockHeader(),
                    blockHashList=[header.getHash() for header in hList])
            else:
                return GetBlockHashListResponse(
                    rootTip=qcState.getRootBlockTip(),
                    shardTip=MinorBlockHeader(),
                    blockHashList=[])

        hList = qcState.getMinorBlockHeaderListByHash(
            h=request.blockHash,
            shardId=request.shardId,
            maxBlocks=request.maxBlocks,
            direction=request.direction)
        if hList is None:
            return GetBlockHashListResponse(
                rootTip=qcState.getRootBlockTip(),
                shardTip=qcState.getShardTip(request.shardId),
                blockHashList=[])
        return GetBlockHashListResponse(
            rootTip=qcState.getRootBlockTip(),
            shardTip=qcState.getShardTip(request.shardId),
            blockHashList=[header.getHash() for header in hList])

    async def handleGetPeerListRequest(self, request):
        resp = GetPeerListResponse()
        for peerId, peer in self.network.activePeerPool.items():
            if peer == self:
                continue
            resp.peerInfoList.append(PeerInfo(int(peer.ip), peer.port))
            if len(resp.peerInfoList) >= request.maxPeers:
                break
        return resp


# Only for non-RPC (fire-and-forget) and RPC request commands
OP_NONRPC_MAP = {
    CommandOp.HELLO: Peer.handleError,
    CommandOp.NEW_BLOCK_COMMAND: Peer.handleNewBlockCommand,
    CommandOp.NEW_TRANSACTION_LIST: Peer.handleNewTransactionList,
}

# For RPC request commands
OP_RPC_MAP = {
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST:
        (CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE,
         Peer.handleGetRootBlockListRequest),
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST:
        (CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE,
         Peer.handleGetMinorBlockListRequest),
    CommandOp.GET_PEER_LIST_REQUEST:
        (CommandOp.GET_PEER_LIST_RESPONSE, Peer.handleGetPeerListRequest),
    CommandOp.GET_BLOCK_HASH_LIST_REQUEST:
        (CommandOp.GET_BLOCK_HASH_LIST_RESPONSE, Peer.handleGetBlockHashListRequest)
}


class SimpleNetwork:

    def __init__(self, env, qcState):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.activePeerPool = dict()    # peer id => peer
        self.selfId = random_bytes(32)
        self.qcState = qcState
        self.ip = ipaddress.ip_address(
            socket.gethostbyname(socket.gethostname()))
        self.port = self.env.config.P2P_SERVER_PORT
        self.localPort = self.env.config.LOCAL_SERVER_PORT
        self.syncing = False
        self.forkResolverManager = ForkResolverManager(
            downloaderFactory=lambda peer: Downloader(peer))

    async def newClient(self, client_reader, client_writer):
        peer = Peer(self.env, client_reader, client_writer, self)
        await peer.start(isServer=True)

    async def newLocalClient(self, reader, writer):
        localServer = LocalServer(self.env, reader, writer, self)
        await localServer.start()

    async def connect(self, ip, port):
        Logger.info("connecting {} {}".format(ip, port))
        try:
            reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
        except Exception as e:
            Logger.info("failed to connect {} {}: {}".format(ip, port, e))
            return None
        peer = Peer(self.env, reader, writer, self)
        peer.sendHello()
        result = await peer.start(isServer=False)
        if result is not None:
            return None
        return peer

    async def connectSeed(self, ip, port):
        peer = await self.connect(ip, port)
        if peer is None:
            # Fail to connect
            return

        # Make sure the peer is ready for incoming messages
        await peer.waitUntilActive()
        try:
            op, resp, rpcId = await peer.writeRpcRequest(
                CommandOp.GET_PEER_LIST_REQUEST, GetPeerListRequest(10))
        except Exception as e:
            Logger.logException()
            return

        Logger.info("connecting {} peers ...".format(len(resp.peerInfoList)))
        for peerInfo in resp.peerInfoList:
            asyncio.ensure_future(self.connect(
                str(ipaddress.ip_address(peerInfo.ip)), peerInfo.port))

        await self.sync(peer)

    def __broadcastCommand(self, op, cmd, sourcePeerId=None):
        data = cmd.serialize()
        for peerId, peer in self.activePeerPool.items():
            if peerId == sourcePeerId:
                continue
            peer.writeRawCommand(op, data)

    def broadcastNewBlockWithRawData(self, isRootBlock, blockData, sourcePeerId=None):
        cmd = NewBlockCommand(isRootBlock, blockData)
        self.__broadcastCommand(CommandOp.NEW_BLOCK_COMMAND, cmd, sourcePeerId)

    def broadcastTransaction(self, shardId, tx, sourcePeerId=None):
        cmd = NewTransactionListCommand([NewTransaction(shardId, tx)])
        self.__broadcastCommand(CommandOp.NEW_TRANSACTION_LIST, cmd, sourcePeerId)

    def isSyncing(self):
        return self.syncing

    async def sync(self, peer=None):
        '''Only allow one sync at a time'''
        if self.syncing:
            return
        self.syncing = True
        try:
            await self.__doSync(peer)
        except Exception:
            Logger.logException()

        self.syncing = False

    async def __syncMinorBlocks(self, peer, minorBlockHashList):
        '''Download and append the minor blocks.
           Appending failures are ignored as we might got root blocks that
           includes already synced minor blocks.
        '''
        # We fetch and append one block at a time rather than all at once
        # so that the server can be responsive to JRPCs during the await slots
        for minorBlockHash in minorBlockHashList:
            try:
                op, resp, rpcId = await peer.writeRpcRequest(
                    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST,
                    GetMinorBlockListRequest(
                        minorBlockHashList=[minorBlockHash],
                    )
                )
            except Exception as e:
                Logger.logException()
                return "Failed to fetch minor blocks: " + str(e)

            for minorBlock in resp.minorBlockList:
                errorMsg = self.qcState.appendMinorBlock(minorBlock)
                if errorMsg:
                    Logger.info("[SYNC] Ignoring minor block appending failure {}/{}: {}".format(
                        minorBlock.header.height, minorBlock.header.branch.getShardId(), errorMsg))

    async def __syncRootBlocksAndConfirmedMinorBlocks(self, peer, rootBlockHashList):
        '''Download and append the root blocks and all the confirmed minor blocks
        '''
        try:
            op, resp, rpcId = await peer.writeRpcRequest(
                CommandOp.GET_ROOT_BLOCK_LIST_REQUEST,
                GetRootBlockListRequest(
                    rootBlockHashList=rootBlockHashList,
                )
            )
        except Exception as e:
            Logger.logException()
            return "Failed to fetch root blocks: " + str(e)

        numNewRootBlocks = len(resp.rootBlockList)
        Logger.info("[SYNC] syncing {} root blocks".format(numNewRootBlocks))

        for rootBlock in resp.rootBlockList:
            minorBlockHashList = [header.getHash() for header in rootBlock.minorBlockHeaderList]
            Logger.info("[SYNC] syncing {} confirmed minor blocks on root block {}".format(
                len(minorBlockHashList), rootBlock.header.height))
            errorMsg = await self.__syncMinorBlocks(peer, minorBlockHashList)
            if errorMsg:
                return "error syncing minor blocks: {}".format(errorMsg)

            errorMsg = self.qcState.appendRootBlock(rootBlock)
            if errorMsg:
                return "error appending root block {}: {}".format(
                    rootBlock.header.height, errorMsg)

    async def __doSync(self, peer=None):
        '''Sync the state of all the block chains with the given peer or a randomly picked remote peer.
           1) Sync the root blocks and all the confirmed minor blocks starting from the current root tip
           2) Sync the unconfirmed minor blocks for each shard starting from the current shard tip
           Assuming no folk in the network and every node returns the correct data.
        '''
        if not peer:
            if not self.activePeerPool:
                Logger.info("[SYNC] No available peer to sync with")
                return

            peerId, peer = random.choice(list(self.activePeerPool.items()))

        Logger.info("[SYNC] start syncing with " + peer.id.hex())

        # Sync root blocks and all the minor blocks confirmed
        while True:
            rootTip = self.qcState.getRootBlockTip()
            try:
                op, resp, rpcId = await peer.writeRpcRequest(
                    CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
                    GetBlockHashListRequest(
                        isRoot=True,
                        shardId=0,      # ignore
                        blockHash=rootTip.getHash(),
                        maxBlocks=1024,
                        direction=1,
                    ),
                )
            except Exception as e:
                Logger.logException()
                return

            if len(resp.blockHashList) - 1 <= 0:
                Logger.info("[SYNC] Finished syncing root blocks and all the confirmed minor blocks")
                break

            errorMsg = await self.__syncRootBlocksAndConfirmedMinorBlocks(peer, resp.blockHashList[1:])
            if errorMsg:
                Logger.info("[SYNC] FAILED " + errorMsg)
                return

        # Sync unconfirmed minor blocks
        # TODO: we currently assume the number of pending minor blocks is less than 1024
        for shardId in range(self.qcState.getShardSize()):
            minorTip = self.qcState.getMinorBlockTip(shardId)
            try:
                op, resp, rpcId = await peer.writeRpcRequest(
                    CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
                    GetBlockHashListRequest(
                        isRoot=False,
                        shardId=shardId,
                        blockHash=minorTip.getHash(),
                        maxBlocks=1024,
                        direction=1,
                    ),
                )
            except Exception as e:
                Logger.logException()
                return

            if len(resp.blockHashList) - 1 <= 0:
                continue

            Logger.info("[SYNC] Syncing {} unconfirmed minor blocks on shard {}!".format(
                len(resp.blockHashList) - 1, shardId))
            errorMsg = await self.__syncMinorBlocks(peer, resp.blockHashList[1:])
            if errorMsg:
                Logger.info("[SYNC] FAILED " + errorMsg)
                return

        Logger.info("[SYNC] Finished syncing all the unconfirmed minor blocks")

    def shutdownPeers(self):
        activePeerPool = self.activePeerPool
        self.activePeerPool = dict()
        for peerId, peer in activePeerPool.items():
            peer.close()

    def startServer(self):
        coro = asyncio.start_server(
            self.newClient, "0.0.0.0", self.port, loop=self.loop)
        self.server = self.loop.run_until_complete(coro)
        Logger.info("Self id {}".format(self.selfId.hex()))
        Logger.info("Listening on {} for p2p".format(
            self.server.sockets[0].getsockname()))

    def shutdown(self):
        self.shutdownPeers()
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    def start(self):
        self.startServer()

        if self.env.config.LOCAL_SERVER_ENABLE:
            coro = asyncio.start_server(
                self.newLocalClient, "0.0.0.0", self.localPort, loop=self.loop)
            self.local_server = self.loop.run_until_complete(coro)
            Logger.info("Listening on {} for local".format(
                self.local_server.sockets[0].getsockname()))

        self.loop.create_task(
            self.connectSeed(self.env.config.P2P_SEED_HOST, self.env.config.P2P_SEED_PORT))

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        self.shutdown()
        self.loop.close()
        Logger.info("Server is shutdown")


def parse_args():
    parser = argparse.ArgumentParser()
    # P2P port
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT, type=int)
    # Local port for JSON-RPC, wallet, etc
    parser.add_argument(
        "--enable_local_server", default=False, type=bool)
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    # Seed host which provides the list of available peers
    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST, type=str)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT, type=int)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--db_path", default="./db", type=str)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server
    if not args.in_memory_db:
        env.db = PersistentDb(path=args.db_path, clean=True)

    return env


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    qcState = QuarkChainState(env)
    network = SimpleNetwork(env, qcState)
    network.start()


if __name__ == '__main__':
    main()
