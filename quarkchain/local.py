from quarkchain.core import Serializable, uint8, uint32, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import Address
from quarkchain.protocol import Connection
import asyncio


class GetBlockTemplateRequest(Serializable):
    FIELDS = (
        ("address", Address)
        ("includeRoot", uint8),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
    )

    def __init__(self, includeRoot=True, shardMaskList=None):
        shardMaskList = [] if shardMaskList is None else shardMaskList
        self.includeRoot = includeRoot
        self.shardMaskList = shardMaskList


class GetBlockTemplateResponse(Serializable):
    FIELDS = (
        ("isRootBlock", uint8),
        ("blockData", PreprendedSizeBytesSerializer(4))
    )

    def __init__(self, isRootBlock, blockData):
        self.isRootBlock = isRootBlock
        self.blockData = blockData


class LocalCommandOp:
    GET_BLOCK_TEMPLATE_REQUEST = 0
    GET_BLOCK_TEMPLATE_RESPONSE = 1


OP_SER_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST: GetBlockTemplateRequest,
    LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE: GetBlockTemplateResponse
}


class LocalServer(Connection):

    def __init__(self, env, reader, writer, network):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), OP_RPC_MAP)
        self.network = network

    async def start(self):
        asyncio.ensure_future(self.loopForever())

    async def handleGetBlockTemplateRequest(self, request):
        isRootBlock, block = self.network.qcState.findBestBlockToMine()

        if isRootBlock is None:
            response = GetBlockTemplateResponse(0, bytes(0))
        elif isRootBlock:
            response = GetBlockTemplateResponse(1, block.serialize())
        else:
            response = GetBlockTemplateResponse(0, block.serialize())
        return response


OP_RPC_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST:
        (LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE,
         LocalServer.handleGetBlockTemplateRequest),
}
