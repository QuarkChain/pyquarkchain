from quarkchain.core import Serializable, uint8, uint32, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import Address, RootBlock, MinorBlock, Transaction
from quarkchain.protocol import Connection, ConnectionState
import asyncio


class GetBlockTemplateRequest(Serializable):
    FIELDS = (
        ("address", Address),
        ("includeRoot", uint8),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
    )

    def __init__(self, address, includeRoot=True, shardMaskList=None):
        shardMaskList = [] if shardMaskList is None else shardMaskList
        self.address = address
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


class SubmitNewBlockRequest(Serializable):
    FIELDS = (
        ("isRootBlock", uint8),
        ("blockData", PreprendedSizeBytesSerializer(4))
    )

    def __init__(self, isRootBlock, blockData):
        self.isRootBlock = isRootBlock
        self.blockData = blockData


class SubmitNewBlockResponse(Serializable):
    FIELDS = (
        ("resultCode", uint8),
        ("resultMessage", PreprendedSizeBytesSerializer(4))
    )

    def __init__(self, resultCode, resultMessage=bytes(0)):
        self.resultCode = resultCode
        self.resultMessage = resultMessage


class NewTransaction(Serializable):
    FIELDS = (
        ("shardId", uint32),
        ("transaction", Transaction),
    )

    def __init__(self, shardId, transaction):
        """ Negative shardId indicates unknown shard (not support yet)
        """
        self.shardId = shardId
        self.transaction = transaction


class AddNewTransactionListRequest(Serializable):
    FIELDS = (
        ("txList", PreprendedSizeListSerializer(4, NewTransaction)),
    )

    def __init__(self, txList):
        self.txList = txList


class AddNewTransactionListResponse(Serializable):
    FIELDS = (
        ("numTxAdded", uint32)
    )

    def __init__(self, numTxAdded):
        self.numTxAdded = numTxAdded


class LocalCommandOp:
    GET_BLOCK_TEMPLATE_REQUEST = 0
    GET_BLOCK_TEMPLATE_RESPONSE = 1
    SUBMIT_NEW_BLOCK_REQUEST = 2
    SUBMIT_NEW_BLOCK_RESPONSE = 3
    ADD_NEW_TRANSACTION_LIST_REQUEST = 4
    ADD_NEW_TRANSACTION_LIST_RESPONSE = 5


OP_SER_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST: GetBlockTemplateRequest,
    LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE: GetBlockTemplateResponse,
    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST: SubmitNewBlockRequest,
    LocalCommandOp.SUBMIT_NEW_BLOCK_RESPONSE: SubmitNewBlockResponse,
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_REQUEST: AddNewTransactionListRequest,
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_RESPONSE: AddNewTransactionListResponse,
}


class LocalServer(Connection):

    def __init__(self, env, reader, writer, network):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), OP_RPC_MAP)
        self.network = network

    async def start(self):
        self.state = ConnectionState.ACTIVE
        asyncio.ensure_future(self.loopForever())

    async def handleGetBlockTemplateRequest(self, request):
        isRootBlock, block = self.network.qcState.findBestBlockToMine()

        if isRootBlock is None:
            response = GetBlockTemplateResponse(0, bytes(0))
        elif isRootBlock:
            response = GetBlockTemplateResponse(1, block.serialize())
            print("obtained root block to mine, height {}, diff {}".format(
                block.header.height, block.header.difficulty))
        else:
            response = GetBlockTemplateResponse(0, block.serialize())
            print("obtained minor block to mine, shard {}, height {}, diff {}".format(
                block.header.branch.getShardId(), block.header.height, block.header.difficulty))
        return response

    async def handleSubmitNewBlockRequest(self, request):
        if request.isRootBlock:
            try:
                rBlock = RootBlock.deserialize(request.blockData)
            except Exception as e:
                return SubmitNewBlockResponse(1, bytes("{}".format(e), "ascii"))
            msg = self.network.qcState.appendRootBlock(rBlock)
            if msg is None:
                return SubmitNewBlockResponse(resultCode=0)
            else:
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes(msg, "ascii"))
        else:
            try:
                mBlock = MinorBlock.deserialize(request.blockData)
            except Exception as e:
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes("{}".format(e), "ascii"))

            msg = self.network.qcState.appendMinorBlock(mBlock)
            if msg is None:
                return SubmitNewBlockResponse(resultCode=0)
            else:
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes(msg, "ascii"))

    async def handleAddNewTransactionListRequest(self, request):
        for newTx in request.txList:
            self.network.qcState.addTransactionToQueue()
        return AddNewTransactionListResponse(len(request.txList))

    def closeWithError(self, error):
        print("Closing with error {}".format(error))
        return super().closeWithError(error)


OP_RPC_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST:
        (LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE,
         LocalServer.handleGetBlockTemplateRequest),
    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST:
        (LocalCommandOp.SUBMIT_NEW_BLOCK_RESPONSE,
         LocalServer.handleSubmitNewBlockRequest),
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_REQUEST:
        (LocalCommandOp.ADD_NEW_TRANSACTION_LIST_RESPONSE,
         LocalServer.handleAddNewTransactionListRequest)
}
