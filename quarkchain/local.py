import asyncio
import json
import statistics
import time
from decimal import Decimal

from quarkchain.core import uint32, boolean, uint8
from quarkchain.core import Serializable, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import Address, Branch, Code, Constant, RootBlock, RootBlockHeader, MinorBlock, MinorBlockHeader
from quarkchain.core import Transaction, TransactionInput, TransactionOutput
from quarkchain.protocol import Connection
from quarkchain.utils import Logger


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
        isRootBlock, block = self.network.qcState.findBestBlockToMine(
            address=request.address, includeTx=request.includeTx)

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

    async def jrpcGetTxTemplate(self, params):
        """ This only uses the utxos on the shard indicated by the fromAddr
        And only support one fromAddr.
        """
        Logger.debug("GetTxTemplate: {}".format(params))

        qcState = self.network.qcState
        fromAddr = params["fromAddr"]
        toAddr = params["toAddr"]
        quarkash = int(Decimal(params["quarkash"]) * qcState.env.config.QUARKSH_TO_JIAOZI)
        fee = int(Decimal(params["fee"]) * qcState.env.config.QUARKSH_TO_JIAOZI)

        # sanity checks
        if len(fromAddr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid fromAddr length {}".format(len(fromAddr)))
        if len(toAddr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid toAddr length {}".format(len(toAddr)))
        if quarkash <= 0:
            raise RuntimeError("Invalid quarkash amount {}".format(quarkash))
        if fee < 0:
            raise RuntimeError("Negative fee {}".format(fee))

        fromAddress = Address.createFrom(fromAddr)
        toAddress = Address.createFrom(toAddr)
        shardId = fromAddress.getShardId(qcState.getShardSize())
        balance = qcState.getAccountBalance(fromAddress)
        requiredQuarkash = quarkash + fee
        if balance < requiredQuarkash:
            raise RuntimeError("Not enough balance {} > {}".format(
                requiredQuarkash, balance))

        # collect enough utxos to complete the tx
        # TODO: better algorithm to reduce the total number of utxos
        utxoPool = qcState.getUtxoPool(shardId)
        inList = []
        inQuarkash = 0
        for txInput, utxo in utxoPool.items():
            if utxo.address != fromAddress:
                continue
            inList.append(txInput)
            inQuarkash += utxo.quarkash
            if requiredQuarkash <= inQuarkash:
                break

        unspent = inQuarkash - requiredQuarkash
        outList = [TransactionOutput(toAddress, quarkash)]
        if unspent < 0:
            raise RuntimeError("Not enough balance!")
        elif unspent > 0:
            outList.append(TransactionOutput(fromAddress, unspent))
        tx = Transaction(
            inList=inList,
            code=Code.getTransferCode(),
            outList=outList,
        )
        resp = {
            "balance": balance,
            "requiredQuarkash": requiredQuarkash,
            "inQuarkash": inQuarkash,
            "tx": tx.serialize().hex(),
            "txHashUnsigned": tx.getHashUnsigned().hex(),
            "code": tx.code.serialize().hex(),
        }
        resp["inList"] = []
        for txInput in inList:
            resp["inList"].append({
                "hash": txInput.hash.hex(),
                "index": txInput.index,
                "quarkash": utxoPool[txInput].quarkash,
            })
        resp["outList"] = []
        for txOutput in outList:
            resp["outList"].append({
                "address": txOutput.address.serialize().hex(),
                "quarkash": txOutput.quarkash,
            })
        return resp

    async def jrpcAddSignedTx(self, params):
        qcState = self.network.qcState
        tx = Transaction.deserialize(bytes.fromhex(params["tx"]))
        signature = params["signature"]
        fromAddr = params["fromAddr"]

        # sanity checks
        if len(fromAddr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid fromAddr length {}".format(len(fromAddr)))
        if len(signature) != Constant.SIGNATURE_HEX_LENGTH:
            raise RuntimeError(
                "Invalid signature length {}".format(len(signature)))
        fromAddress = Address.createFrom(fromAddr)
        shardId = fromAddress.getShardId(qcState.getShardSize())

        # one signer owns all the input utxos
        tx.signList = [bytes.fromhex(signature)] * len(tx.inList)
        qcState.addTransactionToQueue(shardId, tx)

        resp = {
            "txHash": tx.getHash().hex(),
        }
        return resp

    async def jrpcAddTx(self, params):
        """ This only uses the utxos on the shard indicated by the fromAddr
        """
        qcState = self.network.qcState
        key = params["key"]
        fromAddr = params["fromAddr"]

        # sanity checks
        if len(fromAddr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid fromAddr length {}".format(len(fromAddr)))
        if len(key) != Constant.KEY_HEX_LENGTH:
            raise RuntimeError("Invalid key {}".format(key))

        fromAddress = Address.createFrom(fromAddr)
        shardId = fromAddress.getShardId(qcState.getShardSize())

        resp = await self.jrpcGetTxTemplate(params)
        tx = Transaction.deserialize(bytes.fromhex(resp["tx"]))

        # provide signatures to unlock the utxos in inList
        tx.sign([bytes.fromhex(key)] * len(tx.inList))

        qcState.addTransactionToQueue(shardId, tx)
        resp = {
            "txHash": tx.getHash().hex(),
        }
        return resp

    async def jrpcGetAccountBalance(self, params):
        qcState = self.network.qcState
        addr = params["addr"]
        if len(addr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid address length {}".format(len(addr))
            )

        address = Address.createFrom(addr)
        primary = Decimal(qcState.getAccountBalance(address))
        total = Decimal(qcState.getBalance(address.recipient))
        secondary = total - primary
        resp = {
            "primary": str(primary / qcState.env.config.QUARKSH_TO_JIAOZI),
            "secondary": str(secondary / qcState.env.config.QUARKSH_TO_JIAOZI),
            'shardId': address.getShardId(qcState.getShardSize()),
        }
        return resp

    async def jrpcGetAccountTx(self, params):
        addr = params["addr"]
        if len(addr) != Constant.ADDRESS_HEX_LENGTH:
            raise RuntimeError(
                "Invalid address length {}".format(len(addr))
            )
        limit = params.get("limit", 0)
        address = Address.createFrom(addr)
        txList = []
        for txHash, timestamp in self.db.accountTxIter(address, limit):
            txList.append({
                "txHash": txHash.hex(),
                "timestamp": timestamp,
            })
        return {
            "txList": txList,
        }

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
        resp = await self.jrpcGetStats(params)
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

    async def jrpcGetTx(self, params):
        qcState = self.network.qcState

        txHash = params["txHash"]
        if len(txHash) != Constant.TX_HASH_HEX_LENGTH:
            raise RuntimeError("Invalid transaction hash length {}".format(len(txHash)))
        txHash = bytes.fromhex(txHash)
        try:
            tx = self.db.getTx(txHash)
        except Exception as e:
            raise RuntimeError("Failed to get TX {}".format(params["txHash"]))

        inList = []
        for txInput in tx.inList:
            t = self.db.getTx(txInput.hash)
            addr = t.outList[txInput.index].address.toHex()
            inList.append({
                "address": addr,
                "quarkash": str(Decimal(t.outList[txInput.index].quarkash) / qcState.env.config.QUARKSH_TO_JIAOZI),
            })

        outList = []
        for txOutput in tx.outList:
            outList.append({
                "address": txOutput.address.toHex(),
                "quarkash": str(Decimal(txOutput.quarkash) / qcState.env.config.QUARKSH_TO_JIAOZI),
            })

        code = {}
        if tx.code.code == Code.OP_TRANSFER:
            code = {
                "op": "1",
            }
        elif tx.code.code[:1] == b'm':
            height = int.from_bytes(tx.code.code[1:1 + 4], byteorder="big")
            branch = Branch.deserialize(tx.code.code[1 + 4:])
            code = {
                "op": "m",
                "height": height,
                "shardId": branch.getShardId(),
            }
        elif tx.code.code[:1] == b'r':
            height = int.from_bytes(tx.code.code[1:], byteorder="big")
            code = {
                "op": "r",
                "height": height,
            }

        if tx.code.code[:1] == b'r':
            header = self.db.getTxBlockHeader(txHash, RootBlockHeader)
            block = {
                "height": header.height,
                "hash": header.getHash().hex(),
                "type": "r",
            }
        else:
            header = self.db.getTxBlockHeader(txHash, MinorBlockHeader)
            block = {
                "shardId": header.branch.getShardId(),
                "height": header.height,
                "hash": header.getHash().hex(),
                "type": "m",
            }

        return {
            "inList": inList,
            "outList": outList,
            "code": code,
            "block": block,
        }

    async def jrpcGetTxOutputInfo(self, params):
        txHash = params["txHash"]
        if len(txHash) != Constant.TX_HASH_HEX_LENGTH:
            raise RuntimeError("Invalid transaction hash length {}".format(len(txHash)))
        txHash = bytes.fromhex(txHash)
        try:
            tx = self.db.getTx(txHash)
        except Exception as e:
            raise RuntimeError("Failed to get TX {}".format(params["txHash"]))

        index = int(params["index"])
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
            headerHashHex = params["headerHash"]
        except Exception as e:
            raise RuntimeError("failed to get minor block")

        if shardId >= self.network.qcState.getShardSize():
            raise RuntimeError("incorrect shardId")

        if height > self.network.qcState.getMinorBlockTip(shardId).height:
            raise RuntimeError("incorrect height")

        if headerHashHex == "":
            header = self.network.qcState.getMinorBlockHeaderByHeight(shardId, height)
            headerHash = header.getHash()
        else:
            headerHash = bytes.fromhex(headerHashHex)

        block = self.db.getMinorBlockByHash(headerHash)
        header = block.header
        txList = []
        for tx in block.txList:
            txList.append(tx.getHash().hex())
        resp = {
            "shardId": header.branch.getShardId(),
            "height": header.height,
            "hashPrevMinorBlock": header.hashPrevMinorBlock.hex(),
            "hashMerkleRoot": header.hashMerkleRoot.hex(),
            "difficulty": header.difficulty,
            "nonce": header.nonce,
            "createTime": header.createTime,
            "hash": header.getHash().hex(),
            "hashPrevMinorBlock": header.hashPrevMinorBlock.hex(),
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
    "addSignedTx": LocalServer.jrpcAddSignedTx,
    "addTx": LocalServer.jrpcAddTx,
    "getAccountBalance": LocalServer.jrpcGetAccountBalance,
    "getAccountTx": LocalServer.jrpcGetAccountTx,
    "getBlockTx": LocalServer.jrpcGetBlockTx,
    "getFullStats": LocalServer.jrpcGetFullStats,
    "getStats": LocalServer.jrpcGetStats,
    "getTx": LocalServer.jrpcGetTx,
    "getTxOutputInfo": LocalServer.jrpcGetTxOutputInfo,
    "getTxTemplate": LocalServer.jrpcGetTxTemplate,
}
