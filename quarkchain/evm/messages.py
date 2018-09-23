# Modified from pyethereum under MIT license
import rlp

# to bypass circular imports
import quarkchain.core

from quarkchain.evm.utils import int256, safe_ord, bytearray_to_bytestr
from rlp.sedes import big_endian_int, binary, CountableList, BigEndianInt
from rlp.sedes.binary import Binary
from rlp.utils import decode_hex, encode_hex
from quarkchain.evm import utils  # FIXME: use eth_utils
from quarkchain.evm import bloom  # FIXME: use eth_bloom
from quarkchain.evm import transactions
from quarkchain.evm import opcodes
from quarkchain.evm import vm
from quarkchain.evm.specials import specials as default_specials
from quarkchain.evm.exceptions import (
    InvalidNonce,
    InsufficientStartGas,
    UnsignedTransaction,
    BlockGasLimitReached,
    InsufficientBalance,
    InvalidTransaction,
)
from quarkchain.evm.slogging import get_logger


null_address = b"\xff" * 20

log = get_logger("eth.block")
log_tx = get_logger("eth.pb.tx")
log_msg = get_logger("eth.pb.msg")
log_state = get_logger("eth.pb.msg.state")

# contract creating transactions send to an empty address
CREATE_CONTRACT_ADDRESS = b""

# DEV OPTIONS
SKIP_MEDSTATES = False


def rp(tx, what, actual, target):
    return "%r: %r actual:%r target:%r" % (tx, what, actual, target)


class Log(rlp.Serializable):

    # TODO: original version used zpad (here replaced by int32.serialize); had
    # comment "why zpad"?
    fields = [
        ("address", Binary.fixed_length(20, allow_empty=True)),
        ("topics", CountableList(BigEndianInt(32))),
        ("data", binary),
    ]

    def __init__(self, address, topics, data):
        if len(address) == 40:
            address = decode_hex(address)
        assert len(address) == 20
        super(Log, self).__init__(address, topics, data)

    def bloomables(self):
        return [self.address] + [BigEndianInt(32).serialize(x) for x in self.topics]

    def to_dict(self):
        return {
            "bloom": encode_hex(bloom.b64(bloom.bloom_from_list(self.bloomables()))),
            "address": encode_hex(self.address),
            "data": b"0x" + encode_hex(self.data),
            "topics": [encode_hex(utils.int32.serialize(t)) for t in self.topics],
        }

    def __repr__(self):
        return "<Log(address=%r, topics=%r, data=%r)>" % (
            encode_hex(self.address),
            self.topics,
            self.data,
        )


class Receipt(rlp.Serializable):

    fields = [
        ("state_root", binary),
        (
            "gas_used",
            big_endian_int,
        ),  # TODO: this is actually the cumulative gas used. fix it.
        ("bloom", int256),
        ("logs", CountableList(Log)),
        ("contract_address", Binary.fixed_length(20, allow_empty=True)),
        ("contract_full_shard_id", BigEndianInt(4)),
    ]

    def __init__(
        self,
        state_root,
        gas_used,
        logs,
        contract_address,
        contract_full_shard_id,
        bloom=None,
    ):
        # does not call super.__init__ as bloom should not be an attribute but
        # a property
        self.state_root = state_root
        self.gas_used = gas_used
        self.logs = logs
        if bloom is not None and bloom != self.bloom:
            raise ValueError("Invalid bloom filter")
        self.contract_address = contract_address
        self.contract_full_shard_id = contract_full_shard_id
        self._cached_rlp = None
        self._mutable = True

    @property
    def bloom(self):
        bloomables = [x.bloomables() for x in self.logs]
        return bloom.bloom_from_list(utils.flatten(bloomables))


def mk_receipt(state, success, logs, contract_address, contract_full_shard_id):
    o = Receipt(
        b"\x01" if success else b"",
        state.gas_used,
        logs,
        contract_address,
        contract_full_shard_id,
    )
    return o


def config_fork_specific_validation(config, blknum, tx):
    # (1) The transaction signature is valid;
    _ = tx.sender
    if _ is None:
        pass
    if blknum >= config["CONSTANTINOPLE_FORK_BLKNUM"]:
        tx.check_low_s_metropolis()
    else:
        if tx.sender == null_address:
            raise InvalidTransaction("EIP86 transactions not available yet")
        if blknum >= config["HOMESTEAD_FORK_BLKNUM"]:
            tx.check_low_s_homestead()

    if tx.network_id != config["NETWORK_ID"]:
        raise InvalidTransaction("Wrong network ID")
    return True


def validate_transaction(state, tx):

    # (1) The transaction signature is valid;
    if not tx.sender:  # sender is set and validated on Transaction initialization
        raise UnsignedTransaction(tx)

    # assert config_fork_specific_validation(
    #     state.config, state.block_number, tx)

    # (2) the transaction nonce is valid (equivalent to the
    #     sender account's current nonce);
    req_nonce = 0 if tx.sender == null_address else state.get_nonce(tx.sender)
    if req_nonce != tx.nonce:
        raise InvalidNonce(rp(tx, "nonce", tx.nonce, req_nonce))

    # (3) the gas limit is no smaller than the intrinsic gas,
    # g0, used by the transaction;
    total_gas = tx.intrinsic_gas_used
    if tx.startgas < total_gas:
        raise InsufficientStartGas(rp(tx, "startgas", tx.startgas, total_gas))

    # (4) the sender account balance contains at least the
    # cost, v0, required in up-front payment.
    total_cost = tx.value + tx.gasprice * tx.startgas

    if state.get_balance(tx.sender) < total_cost:
        raise InsufficientBalance(
            rp(tx, "balance", state.get_balance(tx.sender), total_cost)
        )

    # check block gas limit
    if state.gas_used + tx.startgas > state.gas_limit:
        raise BlockGasLimitReached(
            rp(tx, "gaslimit", state.gas_used + tx.startgas, state.gas_limit)
        )

    # EIP86-specific restrictions
    if tx.sender == null_address and (tx.value != 0 or tx.gasprice != 0):
        raise InvalidTransaction("EIP86 transactions must have 0 value and gasprice")

    return True


def apply_message(state, msg=None, **kwargs):
    if msg is None:
        msg = vm.Message(**kwargs)
    else:
        assert not kwargs
    ext = VMExt(state, transactions.Transaction(0, 0, 21000, b"", 0, b""))
    result, gas_remained, data = apply_msg(ext, msg)
    return bytearray_to_bytestr(data) if result else None


def apply_transaction(state, tx: transactions.Transaction, tx_wrapper_hash):
    """tx_wrapper_hash is the hash for quarkchain.core.Transaction
    TODO: remove quarkchain.core.Transaction wrapper and use evm.Transaction directly
    """
    state.logs = []
    state.suicides = []
    state.refunds = 0
    validate_transaction(state, tx)

    state.full_shard_id = tx.to_full_shard_id

    intrinsic_gas = tx.intrinsic_gas_used
    log_tx.debug("TX NEW", txdict=tx.to_dict())

    # start transacting #################
    if tx.sender != null_address:
        state.increment_nonce(tx.sender)

    # buy startgas
    assert state.get_balance(tx.sender) >= tx.startgas * tx.gasprice
    state.delta_balance(tx.sender, -tx.startgas * tx.gasprice)

    message_data = vm.CallData([safe_ord(x) for x in tx.data], 0, len(tx.data))
    message = vm.Message(
        tx.sender,
        tx.to,
        tx.value,
        tx.startgas - intrinsic_gas,
        message_data,
        code_address=tx.to,
        is_cross_shard=tx.is_cross_shard(),
        from_full_shard_id=tx.from_full_shard_id,
        to_full_shard_id=tx.to_full_shard_id,
        tx_hash=tx_wrapper_hash,
    )

    # MESSAGE
    ext = VMExt(state, tx)

    contract_address = b""
    if tx.to != b"":
        result, gas_remained, data = apply_msg(ext, message)
    else:  # CREATE
        result, gas_remained, data = create_contract(ext, message)
        contract_address = (
            data if data else b""
        )  # data could be [] when vm failed execution

    assert gas_remained >= 0

    log_tx.debug("TX APPLIED", result=result, gas_remained=gas_remained, data=data)

    gas_used = tx.startgas - gas_remained

    # Transaction failed
    if not result:
        log_tx.debug(
            "TX FAILED",
            reason="out of gas",
            startgas=tx.startgas,
            gas_remained=gas_remained,
        )
        state.delta_balance(tx.sender, tx.gasprice * gas_remained)
        state.delta_balance(state.block_coinbase, tx.gasprice * gas_used)
        state.block_fee += tx.gasprice * gas_used
        output = b""
        success = 0
    # Transaction success
    else:
        log_tx.debug("TX SUCCESS", data=data)
        state.refunds += len(set(state.suicides)) * opcodes.GSUICIDEREFUND
        if state.refunds > 0:
            log_tx.debug("Refunding", gas_refunded=min(state.refunds, gas_used // 2))
            gas_remained += min(state.refunds, gas_used // 2)
            gas_used -= min(state.refunds, gas_used // 2)
            state.refunds = 0
        # sell remaining gas
        state.delta_balance(tx.sender, tx.gasprice * gas_remained)
        # if x-shard, reserve part of the gas for the target shard miner
        fee = tx.gasprice * (
            gas_used - (opcodes.GTXXSHARDCOST if tx.is_cross_shard() else 0)
        )
        state.delta_balance(state.block_coinbase, fee)
        state.block_fee += fee
        if tx.to:
            output = bytearray_to_bytestr(data)
        else:
            output = data
        success = 1

        # TODO: check if the destination address is correct, and consume xshard gas of the state
        # the xshard gas and fee is consumed by destination shard block

    state.gas_used += gas_used

    # Clear suicides
    suicides = state.suicides
    state.suicides = []
    for s in suicides:
        state.set_balance(s, 0)
        state.del_account(s)

    # Pre-Metropolis: commit state after every tx
    if not state.is_METROPOLIS() and not SKIP_MEDSTATES:
        state.commit()

    # Construct a receipt
    r = mk_receipt(state, success, state.logs, contract_address, state.full_shard_id)
    state.logs = []
    state.add_receipt(r)
    state.set_param("bloom", state.bloom | r.bloom)
    state.set_param("txindex", state.txindex + 1)

    return success, output


# VM interface
class VMExt:
    def __init__(self, state, tx):
        self.specials = {k: v for k, v in default_specials.items()}
        for k, v in state.config["CUSTOM_SPECIALS"]:
            self.specials[k] = v
        self._state = state
        self.get_code = state.get_code
        self.set_code = state.set_code
        self.get_balance = state.get_balance
        self.set_balance = state.set_balance
        self.get_nonce = state.get_nonce
        self.set_nonce = state.set_nonce
        self.increment_nonce = state.increment_nonce
        self.set_storage_data = state.set_storage_data
        self.get_storage_data = state.get_storage_data
        self.log_storage = lambda x: state.account_to_dict(x)
        self.add_suicide = lambda x: state.add_suicide(x)
        self.add_refund = lambda x: state.set_param("refunds", state.refunds + x)
        self.block_hash = (
            lambda x: state.get_block_hash(state.block_number - x - 1)
            if (1 <= state.block_number - x <= 256 and x <= state.block_number)
            else b""
        )
        self.block_coinbase = state.block_coinbase
        self.block_timestamp = state.timestamp
        self.block_number = state.block_number
        self.block_difficulty = state.block_difficulty
        self.block_gas_limit = state.gas_limit
        self.log = lambda addr, topics, data: state.add_log(Log(addr, topics, data))
        self.create = lambda msg: create_contract(self, msg)
        self.msg = lambda msg: _apply_msg(self, msg, self.get_code(msg.code_address))
        self.account_exists = state.account_exists
        self.post_homestead_hardfork = lambda: state.is_HOMESTEAD()
        self.post_metropolis_hardfork = lambda: state.is_METROPOLIS()
        self.post_constantinople_hardfork = lambda: state.is_CONSTANTINOPLE()
        self.post_serenity_hardfork = lambda: state.is_SERENITY()
        self.post_anti_dos_hardfork = lambda: state.is_ANTI_DOS()
        self.post_spurious_dragon_hardfork = lambda: state.is_SPURIOUS_DRAGON()
        self.blockhash_store = state.config["METROPOLIS_BLOCKHASH_STORE"]
        self.snapshot = state.snapshot
        self.revert = state.revert
        self.transfer_value = state.transfer_value
        self.deduct_value = state.deduct_value
        self.add_cross_shard_transaction_deposit = lambda deposit: state.xshard_list.append(
            deposit
        )
        self.reset_storage = state.reset_storage
        self.tx_origin = tx.sender if tx else b"\x00" * 20
        self.tx_gasprice = tx.gasprice if tx else 0


def apply_msg(ext, msg):
    return _apply_msg(ext, msg, ext.get_code(msg.code_address))


def _apply_msg(ext, msg, code):
    trace_msg = log_msg.is_active("trace")
    if trace_msg:
        log_msg.debug(
            "MSG APPLY",
            sender=encode_hex(msg.sender),
            to=encode_hex(msg.to),
            gas=msg.gas,
            value=msg.value,
            codelen=len(code),
            data=encode_hex(msg.data.extract_all())
            if msg.data.size < 2500
            else ("data<%d>" % msg.data.size),
            pre_storage=ext.log_storage(msg.to),
            static=msg.static,
            depth=msg.depth,
        )

    # transfer value, quit if not enough
    snapshot = ext.snapshot()
    if msg.transfers_value:
        if msg.is_cross_shard:
            if not ext.deduct_value(msg.sender, msg.value):
                return 1, msg.gas, []
            ext.add_cross_shard_transaction_deposit(
                quarkchain.core.CrossShardTransactionDeposit(
                    tx_hash=msg.tx_hash,
                    from_address=quarkchain.core.Address(
                        msg.sender, msg.from_full_shard_id
                    ),
                    to_address=quarkchain.core.Address(msg.to, msg.to_full_shard_id),
                    value=msg.value,
                    gas_price=ext.tx_gasprice,
                )
            )
        elif not ext.transfer_value(msg.sender, msg.to, msg.value):
            log_msg.debug(
                "MSG TRANSFER FAILED", have=ext.get_balance(msg.to), want=msg.value
            )
            return 1, msg.gas, []

    if msg.is_cross_shard:
        # Cross shard contract call is not supported
        return 1, msg.gas, []

    # Main loop
    if msg.code_address in ext.specials:
        res, gas, dat = ext.specials[msg.code_address](ext, msg)
    else:
        res, gas, dat = vm.vm_execute(ext, msg, code)

    if trace_msg:
        log_msg.debug(
            "MSG APPLIED",
            gas_remained=gas,
            sender=encode_hex(msg.sender),
            to=encode_hex(msg.to),
            data=dat if len(dat) < 2500 else ("data<%d>" % len(dat)),
            post_storage=ext.log_storage(msg.to),
        )

    if res == 0:
        log_msg.debug("REVERTING")
        ext.revert(snapshot)

    return res, gas, dat


def mk_contract_address(sender, full_shard_id, nonce):
    return utils.sha3(
        rlp.encode([utils.normalize_address(sender), full_shard_id, nonce])
    )[12:]


def create_contract(ext, msg):
    log_msg.debug("CONTRACT CREATION")

    if msg.is_cross_shard:
        return 0, msg.gas, b""

    code = msg.data.extract_all()

    if ext.tx_origin != msg.sender:
        ext.increment_nonce(msg.sender)

    if ext.post_constantinople_hardfork() and msg.sender == null_address:
        msg.to = mk_contract_address(msg.sender, msg.to_full_shard_id, 0)
        # msg.to = sha3(msg.sender + code)[12:]
    else:
        nonce = utils.encode_int(ext.get_nonce(msg.sender) - 1)
        msg.to = mk_contract_address(msg.sender, msg.to_full_shard_id, nonce)

    if ext.post_metropolis_hardfork() and (
        ext.get_nonce(msg.to) or len(ext.get_code(msg.to))
    ):
        log_msg.debug("CREATING CONTRACT ON TOP OF EXISTING CONTRACT")
        return 0, 0, b""

    b = ext.get_balance(msg.to)
    if b > 0:
        ext.set_balance(msg.to, b)
        ext.set_nonce(msg.to, 0)
        ext.set_code(msg.to, b"")
        # ext.reset_storage(msg.to)

    msg.is_create = True
    # assert not ext.get_code(msg.to)
    msg.data = vm.CallData([], 0, 0)
    snapshot = ext.snapshot()

    ext.set_nonce(msg.to, 1 if ext.post_spurious_dragon_hardfork() else 0)
    res, gas, dat = _apply_msg(ext, msg, code)

    log_msg.debug(
        "CONTRACT CREATION FINISHED",
        res=res,
        gas=gas,
        dat=dat if len(dat) < 2500 else ("data<%d>" % len(dat)),
    )

    if res:
        if not len(dat):
            # ext.set_code(msg.to, b'')
            return 1, gas, msg.to
        gcost = len(dat) * opcodes.GCONTRACTBYTE
        if gas >= gcost and (
            len(dat) <= 24576 or not ext.post_spurious_dragon_hardfork()
        ):
            gas -= gcost
        else:
            dat = []
            log_msg.debug(
                "CONTRACT CREATION FAILED",
                have=gas,
                want=gcost,
                block_number=ext.block_number,
            )
            if ext.post_homestead_hardfork():
                ext.revert(snapshot)
                return 0, 0, b""
        ext.set_code(msg.to, bytearray_to_bytestr(dat))
        log_msg.debug("SETTING CODE", addr=encode_hex(msg.to), lendat=len(dat))
        return 1, gas, msg.to
    else:
        ext.revert(snapshot)
        return 0, gas, dat
