from quarkchain.core import uint32, boolean, uint8
from quarkchain.core import Serializable, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import Address, RootBlock, MinorBlock, Transaction, TransactionInput, TransactionOutput
from quarkchain.protocol import Connection
from quarkchain.utils import Logger
import asyncio
import statistics
import time
import json


class GetBlockTemplateRequest(Serializable):
    FIELDS = [
        ("address", Address),
        ("includeRoot", boolean),
        ("includeTx", boolean),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
    ]

    def __init__(self, address, includeRoot=True, includeTx=True, shardMaskList=None):
        shardMaskList = [] if shardMaskList is None else shardMaskList
        self.address = address
        self.includeRoot = includeRoot
        self.includeTx = includeTx
        self.shardMaskList = shardMaskList


class GetBlockTemplateResponse(Serializable):
    FIELDS = [
        ("isRootBlock", boolean),
        ("blockData", PreprendedSizeBytesSerializer(4))
    ]

    def __init__(self, isRootBlock, blockData):
        self.isRootBlock = isRootBlock
        self.blockData = blockData


class SubmitNewBlockRequest(Serializable):
    FIELDS = [
        ("isRootBlock", boolean),
        ("blockData", PreprendedSizeBytesSerializer(4))
    ]

    def __init__(self, isRootBlock, blockData):
        self.isRootBlock = isRootBlock
        self.blockData = blockData


class SubmitNewBlockResponse(Serializable):
    FIELDS = [
        ("resultCode", uint8),
        ("resultMessage", PreprendedSizeBytesSerializer(4))
    ]

    def __init__(self, resultCode, resultMessage=bytes(0)):
        self.resultCode = resultCode
        self.resultMessage = resultMessage


class NewTransaction(Serializable):
    FIELDS = [
        ("shardId", uint32),
        ("transaction", Transaction),
    ]

    def __init__(self, shardId, transaction):
        """ Negative shardId indicates unknown shard (not support yet)
        """
        self.shardId = shardId
        self.transaction = transaction


class AddNewTransactionListRequest(Serializable):
    FIELDS = [
        ("txList", PreprendedSizeListSerializer(4, NewTransaction)),
    ]

    def __init__(self, txList):
        self.txList = txList


class AddNewTransactionListResponse(Serializable):
    FIELDS = [
        ("numTxAdded", uint32)
    ]

    def __init__(self, numTxAdded):
        self.numTxAdded = numTxAdded


class JsonRpcRequest(Serializable):
    FIELDS = [
        ("jrpcRequest", PreprendedSizeBytesSerializer(4)),
    ]

    def __init__(self, jrpcRequest):
        self.jrpcRequest = jrpcRequest


class JsonRpcResponse(Serializable):
    FIELDS = [
        ("jrpcResponse", PreprendedSizeBytesSerializer(4)),
    ]

    def __init__(self, jrpcResponse):
        self.jrpcResponse = jrpcResponse


class GetUtxoRequest(Serializable):
    # TODO: Add address filter, shard filter
    FIELDS = [
        ("utxoLimit", uint32),
    ]

    def __init__(self, utxoLimit):
        self.utxoLimit = utxoLimit


class UtxoItem(Serializable):
    FIELDS = [
        ("shardId", uint32),
        ("txInput", TransactionInput),
        ("txOutput", TransactionOutput),
    ]

    def __init__(self, shardId, txInput, txOutput):
        self.shardId = shardId
        self.txInput = txInput
        self.txOutput = txOutput


class GetUtxoResponse(Serializable):
    FIELDS = [
        ("utxoItemList", PreprendedSizeListSerializer(4, UtxoItem))
    ]

    def __init__(self, utxoItemList):
        self.utxoItemList = utxoItemList


class LocalCommandOp:
    GET_BLOCK_TEMPLATE_REQUEST = 0
    GET_BLOCK_TEMPLATE_RESPONSE = 1
    SUBMIT_NEW_BLOCK_REQUEST = 2
    SUBMIT_NEW_BLOCK_RESPONSE = 3
    ADD_NEW_TRANSACTION_LIST_REQUEST = 4
    ADD_NEW_TRANSACTION_LIST_RESPONSE = 5
    JSON_RPC_REQUEST = 6
    JSON_RPC_RESPONSE = 7
    GET_UTXO_REQUEST = 8
    GET_UTXO_RESPONSE = 9


OP_SER_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST: GetBlockTemplateRequest,
    LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE: GetBlockTemplateResponse,
    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST: SubmitNewBlockRequest,
    LocalCommandOp.SUBMIT_NEW_BLOCK_RESPONSE: SubmitNewBlockResponse,
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_REQUEST: AddNewTransactionListRequest,
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_RESPONSE: AddNewTransactionListResponse,
    LocalCommandOp.JSON_RPC_REQUEST: JsonRpcRequest,
    LocalCommandOp.JSON_RPC_RESPONSE: JsonRpcResponse,
    LocalCommandOp.GET_UTXO_REQUEST: GetUtxoRequest,
    LocalCommandOp.GET_UTXO_RESPONSE: GetUtxoResponse,
}


class LocalServer(Connection):

    def __init__(self, env, reader, writer, network):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), OP_RPC_MAP)
        self.network = network
        self.db = self.env.db

    async def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())

    async def handleGetBlockTemplateRequest(self, request):
        isRootBlock, block = self.network.qcState.findBestBlockToMine(includeTx=request.includeTx)

        if isRootBlock is None:
            response = GetBlockTemplateResponse(False, bytes(0))
        elif isRootBlock:
            response = GetBlockTemplateResponse(True, block.serialize())
            print("obtained root block to mine, height {}, diff {}".format(
                block.header.height, block.header.difficulty))
        else:
            response = GetBlockTemplateResponse(False, block.serialize())
            print("obtained minor block to mine, shard {}, height {}, diff {}".format(
                block.header.branch.getShardId(), block.header.height, block.header.difficulty))
        return response

    async def handleSubmitNewBlockRequest(self, request):
        if request.isRootBlock:
            try:
                rBlock = RootBlock.deserialize(request.blockData)
            except Exception as e:
                Logger.logException()
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes("failed to deserialize root block", "ascii"))
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
                Logger.logException()
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes("failed to deserialize minor block", "ascii"))

            msg = self.network.qcState.appendMinorBlock(mBlock)
            if msg is None:
                return SubmitNewBlockResponse(resultCode=0)
            else:
                return SubmitNewBlockResponse(
                    resultCode=1, resultMessage=bytes(msg, "ascii"))

    async def handleAddNewTransactionListRequest(self, request):
        for newTx in request.txList:
            self.network.qcState.addTransactionToQueue(
                newTx.shardId, newTx.transaction)
        return AddNewTransactionListResponse(len(request.txList))

    async def handleGetUtxoRequest(self, request):
        utxoList = []
        for shardId in range(self.network.qcState.getShardSize()):
            for key, value in self.network.qcState.getUtxoPool(shardId).items():
                utxoList.append(UtxoItem(shardId, key, TransactionOutput(value.address, value.quarkash)))
                if len(utxoList) >= request.utxoLimit:
                    break

        return GetUtxoResponse(utxoList)

    def closeWithError(self, error):
        print("Closing with error {}".format(error))
        return super().closeWithError(error)

    def countMinorBlockHeaderStatsIn(self, sec, func):
        qcState = self.network.qcState
        now = time.time()
        metric = 0
        for shardId in range(qcState.getShardSize()):
            header = qcState.getMinorBlockTip(shardId)
            while header.createTime >= now - sec:
                metric += func(header)
                if header.height == 0:
                    break
                header = qcState.getMinorBlockHeaderByHeight(
                    shardId, header.height - 1)
        return metric

    def countShardStatsIn(self, shardId, sec, func):
        qcState = self.network.qcState
        now = time.time()
        metric = 0
        header = qcState.getMinorBlockTip(shardId)
        while header.createTime >= now - sec:
            block = self.env.db.getMinorBlockByHash(header.getHash())
            metric += func(block)
            if header.height == 0:
                break
            header = qcState.getMinorBlockHeaderByHeight(
                shardId, header.height - 1)
        return metric

    def countMinorBlockStatsIn(self, sec, func):
        qcState = self.network.qcState
        metric = 0
        for shardId in range(qcState.getShardSize()):
            metric += self.countShardStatsIn(shardId, sec, func)
        return metric

    def countShardTxIn(self, shardId, sec):
        qcState = self.network.qcState
        now = time.time()
        metric = 0
        header = qcState.getMinorBlockTip(shardId)
        while header.createTime >= now - sec:
            metric += self.env.db.getMinorBlockTxCount(header.getHash())
            if header.height == 0:
                break
            header = qcState.getMinorBlockHeaderByHeight(
                shardId, header.height - 1)
        return metric

    def countMinorBlockTxIn(self, sec):
        qcState = self.network.qcState
        metric = 0
        for shardId in range(qcState.getShardSize()):
            metric += self.countShardTxIn(shardId, sec)
        return metric

    async def jrpcGetStats(self, params):
        qcState = self.network.qcState
        resp = {
            "shardSize": qcState.getShardSize(),
            "rootHeight": qcState.getRootBlockTip().height,
            "rootDifficulty": qcState.getRootBlockTip().difficulty,
            "avgMinorHeight": statistics.mean(
                [qcState.getMinorBlockTip(shardId).height for shardId in range(qcState.getShardSize())]),
            "avgMinorDifficulty": statistics.mean(
                [qcState.getMinorBlockTip(shardId).difficulty for shardId in range(qcState.getShardSize())]),
            "minorBlocksIn60s": self.countMinorBlockHeaderStatsIn(60, lambda h: 1),
            "minorBlocksIn300s": self.countMinorBlockHeaderStatsIn(300, lambda h: 1),
            "transactionsIn60s": self.countMinorBlockTxIn(60),
            "transactionsIn300s": self.countMinorBlockTxIn(300),
            "utxoPoolSize": sum([len(qcState.getUtxoPool(i)) for i in range(qcState.getShardSize())]),
            "pendingTxSize": sum([qcState.getPendingTxSize(i) for i in range(qcState.getShardSize())]),
        }
        return resp

    async def jrpcGetFullStats(self, params):
        qcState = self.network.qcState
        resp = self.jrpcGetStats(params)
        for shardId in range(qcState.getShardSize()):
            shardMetric = {
                "blocksIn60s": self.countShardStatsIn(shardId, 60, lambda h: 1),
                "blocksIn300s": self.countShardStatsIn(shardId, 300, lambda h: 1),
                "difficulty": qcState.getMinorBlockTip(shardId).difficulty,
                "transactionsIn60s": self.countShardTxIn(shardId, 60),
                "transactionsIn300s": self.countShardTxIn(shardId, 300),
                "height": qcState.getMinorBlockTip(shardId).height,
                "utxoPoolSize": len(qcState.getUtxoPool(shardId)),
                "pendingTxSize": qcState.getPendingTxSize(shardId),
            }
            resp["shard{}".format(shardId)] = shardMetric

        return resp

    async def jrpcGetTxOutputInfo(self, params):
        try:
            txHash = params["txHash"]
            if len(txHash) == 64:
                txHash = bytes.fromhex(txHash)
            tx = self.db.getTx(txHash)
            index = int(params["index"])
        except Exception as e:
            raise RuntimeError("failed to get tx")

        if index > len(tx.outList):
            raise RuntimeError("output index is too large")

        txOutput = tx.outList[index]
        unspent, utxoShardId = self.network.qcState.getUtxoInfo(TransactionInput(txHash, index))
        resp = {
            "address": txOutput.address.serialize().hex(),
            "quarkash": txOutput.quarkash,
            "unspent": unspent,
        }

        if unspent:
            resp["utxoShardId"] = utxoShardId
        else:
            resp["spentTxHash"] = self.db.getSpentTxHash(TransactionInput(txHash, index)).hex()

        return resp

    async def jrpcGetBlockTx(self, params):
        try:
            shardId = int(params["shardId"])
            height = int(params["height"])
        except Exception as e:
            raise RuntimeError("failed to get minor block")

        if shardId >= self.network.qcState.getShardSize():
            raise RuntimeError("incorrect shardId")

        if height > self.network.qcState.getMinorBlockTip(shardId).height:
            raise RuntimeError("incorrect height")

        header = self.network.qcState.getMinorBlockHeaderByHeight(shardId, height)
        block = self.db.getMinorBlockByHash(header.getHash())
        txList = []
        for tx in block.txList:
            txList.append(tx.getHash().hex())
        resp = {
            "hashPrevMinorBlock": header.hashPrevMinorBlock.hex(),
            "hashMerkleRoot": header.hashMerkleRoot.hex(),
            "difficulty": header.difficulty,
            "nonce": header.nonce,
            "txList": txList,
        }
        return resp

    def jrpcError(self, errorCode, jrpcId=None, errorMessage=None):
        response = {
            "jsonrpc": "2.0",
            "error": {"code": errorCode, "message": "" if errorMessage is None else errorMessage},
        }
        if jrpcId is not None:
            response["id"] = jrpcId
        return JsonRpcResponse(json.dumps(response).encode())

    async def handleJsonRpcRequest(self, request):
        # TODO: Better jrpc handling
        try:
            jrpcRequest = json.loads(request.jrpcRequest.decode("utf8"))
        except Exception as e:
            return self.jrpcError(-32700)

        if "jsonrpc" not in jrpcRequest or jrpcRequest["jsonrpc"] != "2.0":
            return self.jrpcError(-32600)

        # Ignore id at the monent

        if "method" not in jrpcRequest:
            return self.jrpcError(-32600)

        method = jrpcRequest["method"]
        if method not in JRPC_MAP:
            return self.jrpcError(-32601)

        params = None if "params" not in jrpcRequest else jrpcRequest["params"]

        try:
            jrpcResponse = await JRPC_MAP[method](self, params)
            return JsonRpcResponse(json.dumps(jrpcResponse).encode())
        except Exception as e:
            return self.jrpcError(-32603, errorMessage=str(e))


OP_RPC_MAP = {
    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST:
        (LocalCommandOp.GET_BLOCK_TEMPLATE_RESPONSE,
         LocalServer.handleGetBlockTemplateRequest),
    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST:
        (LocalCommandOp.SUBMIT_NEW_BLOCK_RESPONSE,
         LocalServer.handleSubmitNewBlockRequest),
    LocalCommandOp.ADD_NEW_TRANSACTION_LIST_REQUEST:
        (LocalCommandOp.ADD_NEW_TRANSACTION_LIST_RESPONSE,
         LocalServer.handleAddNewTransactionListRequest),
    LocalCommandOp.JSON_RPC_REQUEST:
        (LocalCommandOp.JSON_RPC_RESPONSE,
         LocalServer.handleJsonRpcRequest),
    LocalCommandOp.GET_UTXO_REQUEST:
        (LocalCommandOp.GET_UTXO_RESPONSE,
         LocalServer.handleGetUtxoRequest),
}

JRPC_MAP = {
    "getStats": LocalServer.jrpcGetStats,
    "getFullStats": LocalServer.jrpcGetFullStats,
    "getTxOutputInfo": LocalServer.jrpcGetTxOutputInfo,
    "getBlockTx": LocalServer.jrpcGetBlockTx,
}
