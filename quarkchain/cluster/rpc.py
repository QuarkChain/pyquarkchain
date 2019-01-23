import typing
from typing import List

from quarkchain.core import (
    CrossShardTransactionList,
    MinorBlock,
    MinorBlockHeader,
    RootBlock,
    TransactionReceipt,
    Log,
    FixedSizeBytesSerializer,
    biguint,
    Constant,
)
from quarkchain.core import (
    Transaction,
    Optional,
    PrependedSizeBytesSerializer,
    PrependedSizeListSerializer,
    Serializable,
    Address,
    Branch,
    ChainMask,
    TokenBalancePair,
)
from quarkchain.core import hash256, uint16, uint32, uint64, uint128, uint256, boolean


# RPCs to initialize a cluster


class Ping(Serializable):
    FIELDS = [
        ("id", PrependedSizeBytesSerializer(4)),
        ("chain_mask_list", PrependedSizeListSerializer(4, ChainMask)),
        ("root_tip", Optional(RootBlock)),  # Initialize ShardState if not None
    ]

    def __init__(self, id, chain_mask_list, root_tip):
        """ Empty chain_mask_list means root """
        if isinstance(id, bytes):
            self.id = id
        else:
            self.id = bytes(id, "ascii")
        self.chain_mask_list = chain_mask_list
        self.root_tip = root_tip


class Pong(Serializable):
    FIELDS = [
        ("id", PrependedSizeBytesSerializer(4)),
        ("chain_mask_list", PrependedSizeListSerializer(4, ChainMask)),
    ]

    def __init__(self, id, chain_mask_list):
        """ Empty slave_id and chain_mask_list means root """
        if isinstance(id, bytes):
            self.id = id
        else:
            self.id = bytes(id, "ascii")
        self.chain_mask_list = chain_mask_list


class SlaveInfo(Serializable):
    FIELDS = [
        ("id", PrependedSizeBytesSerializer(4)),
        ("host", PrependedSizeBytesSerializer(4)),
        ("port", uint16),
        ("chain_mask_list", PrependedSizeListSerializer(4, ChainMask)),
    ]

    def __init__(self, id, host, port, chain_mask_list):
        self.id = id if isinstance(id, bytes) else bytes(id, "ascii")
        self.host = host if isinstance(host, bytes) else bytes(host, "ascii")
        self.port = port
        self.chain_mask_list = chain_mask_list


class ConnectToSlavesRequest(Serializable):
    """ Master instructs a slave to connect to other slaves """

    FIELDS = [("slave_info_list", PrependedSizeListSerializer(4, SlaveInfo))]

    def __init__(self, slave_info_list):
        self.slave_info_list = slave_info_list


class ConnectToSlavesResponse(Serializable):
    """ result_list must have the same size as salve_info_list in the request.
    Empty result means success otherwise it would a serialized error message.
    """

    FIELDS = [
        ("result_list", PrependedSizeListSerializer(4, PrependedSizeBytesSerializer(4)))
    ]

    def __init__(self, result_list):
        self.result_list = result_list


class ArtificialTxConfig(Serializable):
    FIELDS = [("target_root_block_time", uint32), ("target_minor_block_time", uint32)]

    def __init__(self, target_root_block_time, target_minor_block_time):
        self.target_root_block_time = target_root_block_time
        self.target_minor_block_time = target_minor_block_time


class MineRequest(Serializable):
    """Send mining instructions to slaves"""

    FIELDS = [
        ("artificial_tx_config", ArtificialTxConfig),
        ("mining", boolean),  # False to halt mining
    ]

    def __init__(self, artificial_tx_config, mining):
        self.artificial_tx_config = artificial_tx_config
        self.mining = mining


class MineResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class GenTxRequest(Serializable):
    """Generate transactions for loadtesting"""

    FIELDS = [
        ("num_tx_per_shard", uint32),
        ("x_shard_percent", uint32),  # [0, 100]
        ("tx", Transaction),  # sample tx
    ]

    def __init__(self, num_tx_per_shard, x_shard_percent, tx):
        self.num_tx_per_shard = num_tx_per_shard
        self.x_shard_percent = x_shard_percent
        self.tx = tx


class GenTxResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


# Virtual connection management


class CreateClusterPeerConnectionRequest(Serializable):
    """ Broadcast to the cluster and announce that a peer connection is created
    Assume always succeed.
    """

    FIELDS = [("cluster_peer_id", uint64)]

    def __init__(self, cluster_peer_id):
        self.cluster_peer_id = cluster_peer_id


class CreateClusterPeerConnectionResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class DestroyClusterPeerConnectionCommand(Serializable):
    """ Broadcast to the cluster and announce that a peer connection is lost
    As a contract, the master will not send traffic after the command.
    """

    FIELDS = [("cluster_peer_id", uint64)]

    def __init__(self, cluster_peer_id):
        self.cluster_peer_id = cluster_peer_id


# RPCs to lookup data from shards (master -> slaves)


class GetMinorBlockRequest(Serializable):
    FIELDS = [("branch", Branch), ("minor_block_hash", hash256), ("height", uint64)]

    def __init__(self, branch, minor_block_hash=None, height=0):
        self.branch = branch
        self.minor_block_hash = minor_block_hash if minor_block_hash else bytes(32)
        self.height = height


class GetMinorBlockResponse(Serializable):
    FIELDS = [("error_code", uint32), ("minor_block", MinorBlock)]

    def __init__(self, error_code, minor_block):
        self.error_code = error_code
        self.minor_block = minor_block


class GetTransactionRequest(Serializable):
    FIELDS = [("tx_hash", hash256), ("branch", Branch)]

    def __init__(self, tx_hash, branch):
        self.tx_hash = tx_hash
        self.branch = branch


class GetTransactionResponse(Serializable):
    FIELDS = [("error_code", uint32), ("minor_block", MinorBlock), ("index", uint32)]

    def __init__(self, error_code, minor_block, index):
        self.error_code = error_code
        self.minor_block = minor_block
        self.index = index


class ExecuteTransactionRequest(Serializable):
    FIELDS = [
        ("tx", Transaction),
        ("from_address", Address),
        ("block_height", Optional(uint64)),
    ]

    def __init__(self, tx, from_address, block_height: typing.Optional[int]):
        self.tx = tx
        self.from_address = from_address
        self.block_height = block_height


class ExecuteTransactionResponse(Serializable):
    FIELDS = [("error_code", uint32), ("result", PrependedSizeBytesSerializer(4))]

    def __init__(self, error_code, result):
        self.error_code = error_code
        self.result = result


class GetTransactionReceiptRequest(Serializable):
    FIELDS = [("tx_hash", hash256), ("branch", Branch)]

    def __init__(self, tx_hash, branch):
        self.tx_hash = tx_hash
        self.branch = branch


class GetTransactionReceiptResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("minor_block", MinorBlock),
        ("index", uint32),
        ("receipt", TransactionReceipt),
    ]

    def __init__(self, error_code, minor_block, index, receipt):
        self.error_code = error_code
        self.minor_block = minor_block
        self.index = index
        self.receipt = receipt


class GetTransactionListByAddressRequest(Serializable):
    FIELDS = [
        ("address", Address),
        ("start", PrependedSizeBytesSerializer(4)),
        ("limit", uint32),
    ]

    def __init__(self, address, start, limit):
        self.address = address
        self.start = start
        self.limit = limit


class TransactionDetail(Serializable):
    FIELDS = [
        ("tx_hash", hash256),
        ("from_address", Address),
        ("to_address", Optional(Address)),
        ("value", uint256),
        ("block_height", uint64),
        ("timestamp", uint64),  # block timestamp
        ("success", boolean),
        ("gas_token_id", uint64),
        ("transfer_token_id", uint64),
    ]

    def __init__(
        self,
        tx_hash,
        from_address,
        to_address,
        value,
        block_height,
        timestamp,
        success,
        gas_token_id,
        transfer_token_id,
    ):
        self.tx_hash = tx_hash
        self.from_address = from_address
        self.to_address = to_address
        self.value = value
        self.block_height = block_height
        self.timestamp = timestamp
        self.success = success
        self.gas_token_id = gas_token_id
        self.transfer_token_id = transfer_token_id


class GetTransactionListByAddressResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("tx_list", PrependedSizeListSerializer(4, TransactionDetail)),
        ("next", PrependedSizeBytesSerializer(4)),
    ]

    def __init__(self, error_code, tx_list, next):
        self.error_code = error_code
        self.tx_list = tx_list
        self.next = next


# RPCs to update blockchains

# master -> slave


class AddRootBlockRequest(Serializable):
    """ Add root block to each slave
    """

    FIELDS = [("root_block", RootBlock), ("expect_switch", boolean)]

    def __init__(self, root_block, expect_switch):
        self.root_block = root_block
        self.expect_switch = expect_switch


class AddRootBlockResponse(Serializable):
    FIELDS = [("error_code", uint32), ("switched", boolean)]

    def __init__(self, error_code, switched):
        self.error_code = error_code
        self.switched = switched


class EcoInfo(Serializable):
    """ Necessary information for master to decide the best block to mine """

    FIELDS = [
        ("branch", Branch),
        ("height", uint64),
        ("coinbase_amount", uint256),
        ("difficulty", biguint),
        ("unconfirmed_headers_coinbase_amount", uint256),
    ]

    def __init__(
        self,
        branch,
        height,
        coinbase_amount,
        difficulty,
        unconfirmed_headers_coinbase_amount,
    ):
        self.branch = branch
        self.height = height
        self.coinbase_amount = coinbase_amount
        self.difficulty = difficulty
        self.unconfirmed_headers_coinbase_amount = unconfirmed_headers_coinbase_amount


class GetEcoInfoListRequest(Serializable):
    FIELDS = []

    def __init__(self):
        pass


class GetEcoInfoListResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("eco_info_list", PrependedSizeListSerializer(4, EcoInfo)),
    ]

    def __init__(self, error_code, eco_info_list):
        self.error_code = error_code
        self.eco_info_list = eco_info_list


class GetNextBlockToMineRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("address", Address),
        ("artificial_tx_config", ArtificialTxConfig),
    ]

    def __init__(self, branch, address, artificial_tx_config):
        self.branch = branch
        self.address = address
        self.artificial_tx_config = artificial_tx_config


class GetNextBlockToMineResponse(Serializable):
    FIELDS = [("error_code", uint32), ("block", MinorBlock)]

    def __init__(self, error_code, block):
        self.error_code = error_code
        self.block = block


class AddMinorBlockRequest(Serializable):
    """For adding blocks mined through JRPC"""

    FIELDS = [("minor_block_data", PrependedSizeBytesSerializer(4))]

    def __init__(self, minor_block_data):
        self.minor_block_data = minor_block_data


class AddMinorBlockResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class HeadersInfo(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("header_list", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, branch, header_list):
        self.branch = branch
        self.header_list = header_list


class GetUnconfirmedHeadersRequest(Serializable):
    """To collect minor block headers to build a new root block"""

    FIELDS = []

    def __init__(self):
        pass


class GetUnconfirmedHeadersResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("headers_info_list", PrependedSizeListSerializer(4, HeadersInfo)),
    ]

    def __init__(self, error_code, headers_info_list):
        self.error_code = error_code
        self.headers_info_list = headers_info_list


class GetAccountDataRequest(Serializable):
    FIELDS = [("address", Address), ("block_height", Optional(uint64))]

    def __init__(self, address: Address, block_height: typing.Optional[int] = None):
        self.address = address
        self.block_height = block_height


class AccountBranchData(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("transaction_count", uint256),
        ("token_balances", PrependedSizeListSerializer(4, TokenBalancePair)),
        ("is_contract", boolean),
    ]

    def __init__(self, branch, transaction_count, token_balances, is_contract):
        self.branch = branch
        self.transaction_count = transaction_count
        self.token_balances = token_balances
        self.is_contract = is_contract


class GetAccountDataResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("account_branch_data_list", PrependedSizeListSerializer(4, AccountBranchData)),
    ]

    def __init__(self, error_code, account_branch_data_list):
        self.error_code = error_code
        self.account_branch_data_list = account_branch_data_list


class AddTransactionRequest(Serializable):
    FIELDS = [("tx", Transaction)]

    def __init__(self, tx):
        self.tx = tx


class AddTransactionResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class ShardStats(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("height", uint64),
        ("difficulty", biguint),
        ("coinbase_address", Address),
        ("timestamp", uint64),
        ("tx_count60s", uint32),
        ("pending_tx_count", uint32),
        ("total_tx_count", uint32),
        ("block_count60s", uint32),
        ("stale_block_count60s", uint32),
        ("last_block_time", uint32),
    ]

    def __init__(
        self,
        branch: Branch,
        height: int,
        difficulty: int,
        coinbase_address: Address,
        timestamp: int,
        tx_count60s: int,
        pending_tx_count: int,
        total_tx_count: int,
        block_count60s: int,
        stale_block_count60s: int,
        last_block_time: int,
    ):
        self.branch = branch
        self.height = height
        self.difficulty = difficulty
        self.coinbase_address = coinbase_address
        self.timestamp = timestamp
        self.tx_count60s = tx_count60s
        self.pending_tx_count = pending_tx_count
        self.total_tx_count = total_tx_count
        self.block_count60s = block_count60s
        self.stale_block_count60s = stale_block_count60s
        self.last_block_time = last_block_time


class SyncMinorBlockListRequest(Serializable):
    FIELDS = [
        ("minor_block_hash_list", PrependedSizeListSerializer(4, hash256)),
        ("branch", Branch),
        ("cluster_peer_id", uint64),
    ]

    def __init__(self, minor_block_hash_list, branch, cluster_peer_id):
        self.minor_block_hash_list = minor_block_hash_list
        self.branch = branch
        self.cluster_peer_id = cluster_peer_id


class SyncMinorBlockListResponse(Serializable):
    FIELDS = [("error_code", uint32), ("shard_stats", Optional(ShardStats))]

    def __init__(self, error_code, shard_stats=None):
        self.error_code = error_code
        self.shard_stats = shard_stats


# slave -> master


class AddMinorBlockHeaderRequest(Serializable):
    """ Notify master about a successfully added minro block.
    Piggyback the ShardStats in the same request.
    """

    FIELDS = [
        ("minor_block_header", MinorBlockHeader),
        ("tx_count", uint32),  # the total number of tx in the block
        ("x_shard_tx_count", uint32),  # the number of xshard tx in the block
        ("shard_stats", ShardStats),
    ]

    def __init__(self, minor_block_header, tx_count, x_shard_tx_count, shard_stats):
        self.minor_block_header = minor_block_header
        self.tx_count = tx_count
        self.x_shard_tx_count = x_shard_tx_count
        self.shard_stats = shard_stats


class AddMinorBlockHeaderResponse(Serializable):
    FIELDS = [("error_code", uint32), ("artificial_tx_config", ArtificialTxConfig)]

    def __init__(self, error_code, artificial_tx_config):
        self.error_code = error_code
        self.artificial_tx_config = artificial_tx_config


# slave -> slave


class AddXshardTxListRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("minor_block_hash", hash256),
        ("tx_list", CrossShardTransactionList),
    ]

    def __init__(self, branch, minor_block_hash, tx_list):
        self.branch = branch
        self.minor_block_hash = minor_block_hash
        self.tx_list = tx_list


class AddXshardTxListResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class BatchAddXshardTxListRequest(Serializable):
    FIELDS = [
        (
            "add_xshard_tx_list_request_list",
            PrependedSizeListSerializer(4, AddXshardTxListRequest),
        )
    ]

    def __init__(self, add_xshard_tx_list_request_list):
        self.add_xshard_tx_list_request_list = add_xshard_tx_list_request_list


class BatchAddXshardTxListResponse(Serializable):
    FIELDS = [("error_code", uint32)]

    def __init__(self, error_code):
        self.error_code = error_code


class GetLogRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("addresses", PrependedSizeListSerializer(4, Address)),
        (
            "topics",
            PrependedSizeListSerializer(
                4, PrependedSizeListSerializer(4, FixedSizeBytesSerializer(32))
            ),
        ),
        ("start_block", uint64),
        ("end_block", uint64),
    ]

    def __init__(
        self,
        branch: Branch,
        addresses: List[Address],
        topics: List[List[bytes]],
        start_block: int,
        end_block: int,
    ):
        self.branch = branch
        self.addresses = addresses
        self.topics = topics
        self.start_block = start_block
        self.end_block = end_block


class GetLogResponse(Serializable):
    FIELDS = [("error_code", uint32), ("logs", PrependedSizeListSerializer(4, Log))]

    def __init__(self, error_code: int, logs: List[Log]):
        self.error_code = error_code
        self.logs = logs


class EstimateGasRequest(Serializable):
    FIELDS = [("tx", Transaction), ("from_address", Address)]

    def __init__(self, tx: Transaction, from_address: Address):
        self.tx = tx
        self.from_address = from_address


class EstimateGasResponse(Serializable):
    FIELDS = [("error_code", uint32), ("result", uint32)]

    def __init__(self, error_code: int, result: int):
        self.error_code = error_code
        self.result = result


class GetStorageRequest(Serializable):
    FIELDS = [
        ("address", Address),
        ("key", uint256),
        ("block_height", Optional(uint64)),
    ]

    def __init__(
        self, address: Address, key: int, block_height: typing.Optional[int] = None
    ):
        self.address = address
        self.key = key
        self.block_height = block_height


class GetStorageResponse(Serializable):
    FIELDS = [("error_code", uint32), ("result", FixedSizeBytesSerializer(32))]

    def __init__(self, error_code: int, result: bytes):
        self.error_code = error_code
        self.result = result


class GetCodeRequest(Serializable):
    FIELDS = [("address", Address), ("block_height", Optional(uint64))]

    def __init__(self, address: Address, block_height: typing.Optional[int] = None):
        self.address = address
        self.block_height = block_height


class GetCodeResponse(Serializable):
    FIELDS = [("error_code", uint32), ("result", PrependedSizeBytesSerializer(4))]

    def __init__(self, error_code: int, result: bytes):
        self.error_code = error_code
        self.result = result


class GasPriceRequest(Serializable):
    FIELDS = [("branch", Branch)]

    def __init__(self, branch: Branch):
        self.branch = branch


class GasPriceResponse(Serializable):
    FIELDS = [("error_code", uint32), ("result", uint64)]

    def __init__(self, error_code: int, result: int):
        self.error_code = error_code
        self.result = result


class GetWorkRequest(Serializable):
    FIELDS = [("branch", Branch)]

    def __init__(self, branch: Branch):
        self.branch = branch


class GetWorkResponse(Serializable):
    FIELDS = [
        ("error_code", uint32),
        ("header_hash", hash256),
        ("height", uint64),
        ("difficulty", biguint),
    ]

    def __init__(
        self,
        error_code: int,
        header_hash: bytes = bytes(Constant.HASH_LENGTH),
        height: int = 0,
        difficulty: int = 0,
    ):
        self.error_code = error_code
        self.header_hash = header_hash
        self.height = height
        self.difficulty = difficulty


class SubmitWorkRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("header_hash", hash256),
        ("nonce", uint64),
        ("mixhash", hash256),
    ]

    def __init__(self, branch: Branch, header_hash: bytes, nonce: int, mixhash: bytes):
        self.branch = branch
        self.header_hash = header_hash
        self.nonce = nonce
        self.mixhash = mixhash


class SubmitWorkResponse(Serializable):
    FIELDS = [("error_code", uint32), ("success", boolean)]

    def __init__(self, error_code: int, success: bool):
        self.error_code = error_code
        self.success = success


CLUSTER_OP_BASE = 128


class ClusterOp:

    # TODO: Remove cluster op base as cluster op should be independent to p2p op
    PING = 1 + CLUSTER_OP_BASE
    PONG = 2 + CLUSTER_OP_BASE
    CONNECT_TO_SLAVES_REQUEST = 3 + CLUSTER_OP_BASE
    CONNECT_TO_SLAVES_RESPONSE = 4 + CLUSTER_OP_BASE
    ADD_ROOT_BLOCK_REQUEST = 5 + CLUSTER_OP_BASE
    ADD_ROOT_BLOCK_RESPONSE = 6 + CLUSTER_OP_BASE
    GET_ECO_INFO_LIST_REQUEST = 7 + CLUSTER_OP_BASE
    GET_ECO_INFO_LIST_RESPONSE = 8 + CLUSTER_OP_BASE
    GET_NEXT_BLOCK_TO_MINE_REQUEST = 9 + CLUSTER_OP_BASE
    GET_NEXT_BLOCK_TO_MINE_RESPONSE = 10 + CLUSTER_OP_BASE
    GET_UNCONFIRMED_HEADERS_REQUEST = 11 + CLUSTER_OP_BASE
    GET_UNCONFIRMED_HEADERS_RESPONSE = 12 + CLUSTER_OP_BASE
    GET_ACCOUNT_DATA_REQUEST = 13 + CLUSTER_OP_BASE
    GET_ACCOUNT_DATA_RESPONSE = 14 + CLUSTER_OP_BASE
    ADD_TRANSACTION_REQUEST = 15 + CLUSTER_OP_BASE
    ADD_TRANSACTION_RESPONSE = 16 + CLUSTER_OP_BASE
    ADD_MINOR_BLOCK_HEADER_REQUEST = 17 + CLUSTER_OP_BASE
    ADD_MINOR_BLOCK_HEADER_RESPONSE = 18 + CLUSTER_OP_BASE
    ADD_XSHARD_TX_LIST_REQUEST = 19 + CLUSTER_OP_BASE
    ADD_XSHARD_TX_LIST_RESPONSE = 20 + CLUSTER_OP_BASE
    SYNC_MINOR_BLOCK_LIST_REQUEST = 21 + CLUSTER_OP_BASE
    SYNC_MINOR_BLOCK_LIST_RESPONSE = 22 + CLUSTER_OP_BASE
    ADD_MINOR_BLOCK_REQUEST = 23 + CLUSTER_OP_BASE
    ADD_MINOR_BLOCK_RESPONSE = 24 + CLUSTER_OP_BASE
    CREATE_CLUSTER_PEER_CONNECTION_REQUEST = 25 + CLUSTER_OP_BASE
    CREATE_CLUSTER_PEER_CONNECTION_RESPONSE = 26 + CLUSTER_OP_BASE
    DESTROY_CLUSTER_PEER_CONNECTION_COMMAND = 27 + CLUSTER_OP_BASE
    GET_MINOR_BLOCK_REQUEST = 29 + CLUSTER_OP_BASE
    GET_MINOR_BLOCK_RESPONSE = 30 + CLUSTER_OP_BASE
    GET_TRANSACTION_REQUEST = 31 + CLUSTER_OP_BASE
    GET_TRANSACTION_RESPONSE = 32 + CLUSTER_OP_BASE
    BATCH_ADD_XSHARD_TX_LIST_REQUEST = 33 + CLUSTER_OP_BASE
    BATCH_ADD_XSHARD_TX_LIST_RESPONSE = 34 + CLUSTER_OP_BASE
    EXECUTE_TRANSACTION_REQUEST = 35 + CLUSTER_OP_BASE
    EXECUTE_TRANSACTION_RESPONSE = 36 + CLUSTER_OP_BASE
    GET_TRANSACTION_RECEIPT_REQUEST = 37 + CLUSTER_OP_BASE
    GET_TRANSACTION_RECEIPT_RESPONSE = 38 + CLUSTER_OP_BASE
    MINE_REQUEST = 39 + CLUSTER_OP_BASE
    MINE_RESPONSE = 40 + CLUSTER_OP_BASE
    GEN_TX_REQUEST = 41 + CLUSTER_OP_BASE
    GEN_TX_RESPONSE = 42 + CLUSTER_OP_BASE
    GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST = 43 + CLUSTER_OP_BASE
    GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE = 44 + CLUSTER_OP_BASE
    GET_LOG_REQUEST = 45 + CLUSTER_OP_BASE
    GET_LOG_RESPONSE = 46 + CLUSTER_OP_BASE
    ESTIMATE_GAS_REQUEST = 47 + CLUSTER_OP_BASE
    ESTIMATE_GAS_RESPONSE = 48 + CLUSTER_OP_BASE
    GET_STORAGE_REQUEST = 49 + CLUSTER_OP_BASE
    GET_STORAGE_RESPONSE = 50 + CLUSTER_OP_BASE
    GET_CODE_REQUEST = 51 + CLUSTER_OP_BASE
    GET_CODE_RESPONSE = 52 + CLUSTER_OP_BASE
    GAS_PRICE_REQUEST = 53 + CLUSTER_OP_BASE
    GAS_PRICE_RESPONSE = 54 + CLUSTER_OP_BASE
    GET_WORK_REQUEST = 55 + CLUSTER_OP_BASE
    GET_WORK_RESPONSE = 56 + CLUSTER_OP_BASE
    SUBMIT_WORK_REQUEST = 57 + CLUSTER_OP_BASE
    SUBMIT_WORK_RESPONSE = 58 + CLUSTER_OP_BASE


CLUSTER_OP_SERIALIZER_MAP = {
    ClusterOp.PING: Ping,
    ClusterOp.PONG: Pong,
    ClusterOp.CONNECT_TO_SLAVES_REQUEST: ConnectToSlavesRequest,
    ClusterOp.CONNECT_TO_SLAVES_RESPONSE: ConnectToSlavesResponse,
    ClusterOp.ADD_ROOT_BLOCK_REQUEST: AddRootBlockRequest,
    ClusterOp.ADD_ROOT_BLOCK_RESPONSE: AddRootBlockResponse,
    ClusterOp.GET_ECO_INFO_LIST_REQUEST: GetEcoInfoListRequest,
    ClusterOp.GET_ECO_INFO_LIST_RESPONSE: GetEcoInfoListResponse,
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST: GetNextBlockToMineRequest,
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_RESPONSE: GetNextBlockToMineResponse,
    ClusterOp.ADD_MINOR_BLOCK_REQUEST: AddMinorBlockRequest,
    ClusterOp.ADD_MINOR_BLOCK_RESPONSE: AddMinorBlockResponse,
    ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST: GetUnconfirmedHeadersRequest,
    ClusterOp.GET_UNCONFIRMED_HEADERS_RESPONSE: GetUnconfirmedHeadersResponse,
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: AddMinorBlockHeaderRequest,
    ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE: AddMinorBlockHeaderResponse,
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST: AddXshardTxListRequest,
    ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE: AddXshardTxListResponse,
    ClusterOp.GET_ACCOUNT_DATA_REQUEST: GetAccountDataRequest,
    ClusterOp.GET_ACCOUNT_DATA_RESPONSE: GetAccountDataResponse,
    ClusterOp.ADD_TRANSACTION_REQUEST: AddTransactionRequest,
    ClusterOp.ADD_TRANSACTION_RESPONSE: AddTransactionResponse,
    ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST: SyncMinorBlockListRequest,
    ClusterOp.SYNC_MINOR_BLOCK_LIST_RESPONSE: SyncMinorBlockListResponse,
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST: CreateClusterPeerConnectionRequest,
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_RESPONSE: CreateClusterPeerConnectionResponse,
    ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: DestroyClusterPeerConnectionCommand,
    ClusterOp.GET_MINOR_BLOCK_REQUEST: GetMinorBlockRequest,
    ClusterOp.GET_MINOR_BLOCK_RESPONSE: GetMinorBlockResponse,
    ClusterOp.GET_TRANSACTION_REQUEST: GetTransactionRequest,
    ClusterOp.GET_TRANSACTION_RESPONSE: GetTransactionResponse,
    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST: BatchAddXshardTxListRequest,
    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_RESPONSE: BatchAddXshardTxListResponse,
    ClusterOp.EXECUTE_TRANSACTION_REQUEST: ExecuteTransactionRequest,
    ClusterOp.EXECUTE_TRANSACTION_RESPONSE: ExecuteTransactionResponse,
    ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST: GetTransactionReceiptRequest,
    ClusterOp.GET_TRANSACTION_RECEIPT_RESPONSE: GetTransactionReceiptResponse,
    ClusterOp.MINE_REQUEST: MineRequest,
    ClusterOp.MINE_RESPONSE: MineResponse,
    ClusterOp.GEN_TX_REQUEST: GenTxRequest,
    ClusterOp.GEN_TX_RESPONSE: GenTxResponse,
    ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST: GetTransactionListByAddressRequest,
    ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE: GetTransactionListByAddressResponse,
    ClusterOp.GET_LOG_REQUEST: GetLogRequest,
    ClusterOp.GET_LOG_RESPONSE: GetLogResponse,
    ClusterOp.ESTIMATE_GAS_REQUEST: EstimateGasRequest,
    ClusterOp.ESTIMATE_GAS_RESPONSE: EstimateGasResponse,
    ClusterOp.GET_STORAGE_REQUEST: GetStorageRequest,
    ClusterOp.GET_STORAGE_RESPONSE: GetStorageResponse,
    ClusterOp.GET_CODE_REQUEST: GetCodeRequest,
    ClusterOp.GET_CODE_RESPONSE: GetCodeResponse,
    ClusterOp.GAS_PRICE_REQUEST: GasPriceRequest,
    ClusterOp.GAS_PRICE_RESPONSE: GasPriceResponse,
    ClusterOp.GET_WORK_REQUEST: GetWorkRequest,
    ClusterOp.GET_WORK_RESPONSE: GetWorkResponse,
    ClusterOp.SUBMIT_WORK_REQUEST: SubmitWorkRequest,
    ClusterOp.SUBMIT_WORK_RESPONSE: SubmitWorkResponse,
}
