# Modified based on pyethereum under MIT license
import copy
import rlp
from rlp.sedes.lists import CountableList
from rlp.sedes import binary
from quarkchain.config import ChainConfig, ShardConfig
from quarkchain.db import Db, OverlayDb
from quarkchain.evm.utils import (
    hash32,
    trie_root,
    big_endian_int,
    encode_hex,
    big_endian_to_int,
    parse_as_bin,
    parse_as_int,
    decode_hex,
    sha3,
    is_string,
    is_numeric,
    BigEndianInt,
)
from quarkchain.evm import utils
from quarkchain.evm import trie
from quarkchain.evm.trie import Trie
from quarkchain.evm.securetrie import SecureTrie
from quarkchain.evm.config import Env
from quarkchain.evm.common import FakeHeader
from quarkchain.utils import token_id_encode


BLANK_HASH = utils.sha3(b"")
BLANK_ROOT = utils.sha3rlp(b"")

THREE = b"\x00" * 19 + b"\x03"

TOKEN_TRIE_THRESHOLD = 16


def snapshot_form(val):
    if is_numeric(val):
        return str(val)
    elif is_string(val):
        return "0x" + encode_hex(val)


STATE_DEFAULTS = {
    "gas_used": 0,
    "xshard_receive_gas_used": 0,
    "gas_limit": 3141592,
    "block_number": 0,
    "block_coinbase": b"\x00" * 20,
    "block_difficulty": 1,
    "block_fee_tokens": {},
    "timestamp": 0,
    "logs": [],
    "receipts": [],
    "xshard_deposit_receipts": [],
    "suicides": [],
    "recent_uncles": {},
    "prev_headers": [],
    "refunds": 0,
    "xshard_list": [],
    "full_shard_key": 0,  # should be updated before applying each tx
    "xshard_tx_cursor_info": None,
}


class _Account(rlp.Serializable):
    fields = [
        ("nonce", big_endian_int),
        ("token_balances", binary),
        ("storage", trie_root),
        ("code_hash", hash32),
        ("full_shard_key", BigEndianInt(4)),
        ("optional", binary),
    ]


class _MockAccount(rlp.Serializable):
    """Used in EVM tests, compatible with existing ledger model in Ethereum."""

    fields = [
        ("nonce", big_endian_int),
        ("balance", big_endian_int),
        ("storage", trie_root),
        ("code_hash", hash32),
    ]


class TokenBalancePair(rlp.Serializable):
    fields = [("token_id", big_endian_int), ("balance", big_endian_int)]


class TokenBalances:
    """Token balances including non-genesis native tokens."""

    def __init__(self, data: bytes, db):
        self.token_trie = None
        self.token_root = None
        # We don't want outside world to know this
        self._balances = {}
        self._db = db
        self._committed = False

        if len(data) != 0:
            enum = data[:1]
            if enum == b"\x00":
                for p in rlp.decode(data[1:], CountableList(TokenBalancePair)):
                    self._balances[p.token_id] = p.balance
            elif enum == b"\x01":
                self.token_root = data[1:]
                self.token_trie = SecureTrie(Trie(db, data[1:]))
            else:
                raise ValueError("Unknown enum byte in token_balances")

    def commit(self):
        self._committed = True
        # No-op if not using trie and token size is small
        if not self.token_trie and self._non_zero_entries <= TOKEN_TRIE_THRESHOLD:
            return
        # Use trie if size exceeds threshold
        if not self.token_trie:
            self.token_trie = SecureTrie(Trie(self._db))

        keys_to_del = []
        for token_id, bal in self._balances.items():
            k = utils.encode_int32(token_id)
            if bal:
                self.token_trie.update(k, rlp.encode(bal))
            else:
                self.token_trie.delete(k)
                keys_to_del.append(token_id)
        for token_id in keys_to_del:
            del self._balances[token_id]

        self.token_root = self.token_trie.root_hash

    def serialize(self):
        if not self._committed:
            self.commit()

        # Fast path, since already committed
        if self.token_trie:
            return b"\x01" + self.token_root

        # Serialize in-memory balance representation as an array
        if len(self._balances) == 0:
            return b""
        # Don't serialize 0 balance
        ls = [TokenBalancePair(k, v) for k, v in self._balances.items() if v > 0]
        # Sort by token id to make token balances serialization deterministic
        ls.sort(key=lambda b: b.token_id)
        return b"\x00" + rlp.encode(ls)

    def balance(self, token_id):
        if token_id in self._balances:
            return self._balances[token_id]
        if self.token_trie:
            v = self.token_trie.get(utils.encode_int32(token_id))
            ret = utils.big_endian_to_int(rlp.decode(v)) if v else 0
            self._balances[token_id] = ret
            return ret
        return 0

    def set_balance(self, journal, token_id, value):
        preval = self.balance(token_id)
        self._balances[token_id] = value
        journal.append(lambda: self._balances.__setitem__(token_id, preval))
        if self._committed:
            self._committed = False
            journal.append(lambda: setattr(self, "_committed", True))

    def is_blank(self):
        return not self.token_trie and self._balances == {}

    def to_dict(self):
        if not self.token_trie:
            return self._balances

        trie_dict = self.token_trie.to_dict()
        ret = {
            utils.big_endian_to_int(k): utils.big_endian_to_int(rlp.decode(v))
            for k, v in trie_dict.items()
        }
        # Get latest update if not committed
        if not self._committed:
            for k, v in self._balances.items():
                if v == 0:
                    ret.pop(k, None)
                else:
                    ret[k] = v
        return ret

    def reset(self, journal):
        pre_balance = self._balances
        self._balances = {}
        journal.append(lambda: setattr(self, "_balance", pre_balance))
        pre_token_trie = self.token_trie
        self.token_trie = None
        journal.append(lambda: setattr(self, "token_trie", pre_token_trie))
        pre_token_root = self.token_root
        self.token_root = None
        journal.append(lambda: setattr(self, "token_root", pre_token_root))
        self._committed = False

    @property
    def _non_zero_entries(self):
        return sum(1 for bal in self._balances.values() if bal > 0)


class Account:
    def __init__(
        self,
        nonce,
        token_balances,
        storage,
        code_hash,
        full_shard_key,
        env,
        address,
        db=None,
    ):
        self.db = env.db if db is None else db
        assert isinstance(db, Db)
        self.env = env
        self.address = address

        acc = _Account(nonce, token_balances, storage, code_hash, full_shard_key, b"")
        self.nonce = acc.nonce
        self.storage = acc.storage
        self.code_hash = acc.code_hash
        self.full_shard_key = acc.full_shard_key
        self.token_balances = TokenBalances(token_balances, self.db)

        self.storage_cache = {}
        self.storage_trie = SecureTrie(Trie(self.db))
        self.storage_trie.root_hash = self.storage
        self.touched = False
        self.existent_at_start = True
        self._mutable = True
        self.deleted = False

    def commit(self):
        for k, v in self.storage_cache.items():
            if v:
                self.storage_trie.update(utils.encode_int32(k), rlp.encode(v))
            else:
                self.storage_trie.delete(utils.encode_int32(k))
        self.storage_cache = {}
        self.storage = self.storage_trie.root_hash
        self.token_balances.commit()

    @property
    def code(self):
        return self.db[self.code_hash]

    @code.setter
    def code(self, value):
        self.code_hash = utils.sha3(value)
        # Technically a db storage leak, but doesn't really matter; the only
        # thing that fails to get garbage collected is when code disappears due
        # to a suicide
        self.db.put(self.code_hash, value)

    def get_storage_data(self, key):
        if key not in self.storage_cache:
            v = self.storage_trie.get(utils.encode_int32(key))
            self.storage_cache[key] = utils.big_endian_to_int(
                rlp.decode(v) if v else b""
            )
        return self.storage_cache[key]

    def set_storage_data(self, key, value):
        self.storage_cache[key] = value

    @classmethod
    def blank_account(cls, env, address, full_shard_key, initial_nonce=0, db=None):
        if db is None:
            db = env.db
        db.put(BLANK_HASH, b"")
        o = cls(
            initial_nonce,
            b"",
            trie.BLANK_ROOT,
            BLANK_HASH,
            full_shard_key,
            env,
            address,
            db=db,
        )
        o.existent_at_start = False
        return o

    def is_blank(self):
        return (
            self.nonce == 0
            and self.token_balances.is_blank()
            and self.code_hash == BLANK_HASH
        )

    def to_dict(self):
        odict = self.storage_trie.to_dict()
        for k, v in self.storage_cache.items():
            odict[utils.encode_int(k)] = rlp.encode(utils.encode_int(v))
        return {
            "token_balances": self.token_balances.to_dict(),
            "nonce": str(self.nonce),
            "code": "0x" + encode_hex(self.code),
            "storage": {
                "0x"
                + encode_hex(key.lstrip(b"\x00") or b"\x00"): "0x"
                + encode_hex(rlp.decode(val))
                for key, val in odict.items()
            },
        }


# from ethereum.state import State
class State:
    def __init__(
        self,
        root=BLANK_ROOT,
        env=Env(),
        qkc_config=None,
        executing_on_head=False,
        db=None,
        use_mock_evm_account=False,
        **kwargs
    ):
        if db is None:
            db = env.db
        self.env = env
        self.__db = db
        self.trie = SecureTrie(Trie(self.db, root))
        for k, v in STATE_DEFAULTS.items():
            setattr(self, k, kwargs.get(k, copy.copy(v)))
        self.journal = []
        self.cache = {}
        self.log_listeners = []
        self.deletes = []
        self.changed = {}
        self.executing_on_head = executing_on_head
        self.qkc_config = qkc_config
        self.sender_disallow_map = dict()  # type: Dict[bytes, int]
        self.shard_config = ShardConfig(ChainConfig())
        self.use_mock_evm_account = use_mock_evm_account

    @property
    def db(self):
        return self.__db

    @property
    def config(self):
        return self.env.config

    def get_bloom(self):
        bloom = 0
        for r in self.receipts:
            bloom |= r.bloom
        for r in self.xshard_deposit_receipts:
            bloom |= r.bloom
        return bloom

    def get_block_hash(self, n):
        if self.block_number < n or n > 256 or n < 0:
            o = b"\x00" * 32
        else:
            o = (
                self.prev_headers[n].get_hash()
                if self.prev_headers[n]
                else b"\x00" * 32
            )
        return o

    def add_block_header(self, block_header):
        self.prev_headers = [block_header] + self.prev_headers

    def get_and_cache_account(self, address):
        if address in self.cache:
            return self.cache[address]
        if self.executing_on_head and False:
            try:
                rlpdata = self.db[b"address:" + address]
            except KeyError:
                rlpdata = b""
        else:
            rlpdata = self.trie.get(address)
        if rlpdata != trie.BLANK_NODE:
            if self.use_mock_evm_account:
                o = rlp.decode(rlpdata, _MockAccount)
                raw_token_balances = TokenBalances(b"", self.db)
                raw_token_balances._balances = {token_id_encode("QKC"): o.balance}
                token_balances = raw_token_balances.serialize()
                full_shard_key = self.full_shard_key
            else:
                o = rlp.decode(rlpdata, _Account)
                token_balances = o.token_balances
                full_shard_key = o.full_shard_key
            o = Account(
                nonce=o.nonce,
                token_balances=token_balances,
                storage=o.storage,
                code_hash=o.code_hash,
                full_shard_key=full_shard_key,
                env=self.env,
                address=address,
                db=self.db,
            )
        else:
            o = Account.blank_account(
                self.env,
                address,
                self.full_shard_key,
                self.config["ACCOUNT_INITIAL_NONCE"],
                db=self.db,
            )
        self.cache[address] = o
        o._mutable = True
        o._cached_rlp = None
        return o

    def get_balances(self, address) -> dict:
        return self.get_and_cache_account(
            utils.normalize_address(address)
        ).token_balances.to_dict()

    def get_balance(self, address, token_id=None):
        if token_id is None:
            token_id = self.shard_config.default_chain_token
        return self.get_and_cache_account(
            utils.normalize_address(address)
        ).token_balances.balance(token_id)

    def get_code(self, address):
        return self.get_and_cache_account(utils.normalize_address(address)).code

    def get_nonce(self, address):
        return self.get_and_cache_account(utils.normalize_address(address)).nonce

    def get_full_shard_key(self, address):
        return self.get_and_cache_account(
            utils.normalize_address(address)
        ).full_shard_key

    def set_and_journal(self, acct, param, val):
        # self.journal.append((acct, param, getattr(acct, param)))
        preval = getattr(acct, param)
        self.journal.append(lambda: setattr(acct, param, preval))
        setattr(acct, param, val)

    def reset_balances(self, address):
        acct = self.get_and_cache_account(utils.normalize_address(address))
        if acct.token_balances.is_blank():
            self.set_and_journal(acct, "touched", True)
        else:
            acct.token_balances.reset(self.journal)

    def set_code(self, address, value):
        # assert is_string(value)
        acct = self.get_and_cache_account(utils.normalize_address(address))
        self.set_and_journal(acct, "code", value)
        self.set_and_journal(acct, "touched", True)

    def set_nonce(self, address, value):
        acct = self.get_and_cache_account(utils.normalize_address(address))
        self.set_and_journal(acct, "nonce", value)
        self.set_and_journal(acct, "touched", True)

    def set_token_balance(self, address, token_id, val):
        acct = self.get_and_cache_account(utils.normalize_address(address))
        if val == self.get_balance(address, token_id=token_id):
            self.set_and_journal(acct, "touched", True)
            return
        self._set_token_balance_and_journal(acct, token_id, val)
        self.set_and_journal(acct, "touched", True)

    def set_balance(self, address, val):
        self.set_token_balance(
            address, token_id=self.shard_config.default_chain_token, val=val
        )

    def _set_token_balance_and_journal(self, acct, token_id, val):
        """if token_id was not set, journal will erase token_id when reverted
        """
        acct.token_balances.set_balance(self.journal, token_id, val)

    def delta_token_balance(self, address, token_id, value):
        address = utils.normalize_address(address)
        acct = self.get_and_cache_account(address)
        if value == 0:
            self.set_and_journal(acct, "touched", True)
            return
        newbal = acct.token_balances.balance(token_id) + value
        self._set_token_balance_and_journal(acct, token_id, newbal)
        self.set_and_journal(acct, "touched", True)

    def increment_nonce(self, address):
        address = utils.normalize_address(address)
        acct = self.get_and_cache_account(address)
        newnonce = acct.nonce + 1
        self.set_and_journal(acct, "nonce", newnonce)
        self.set_and_journal(acct, "touched", True)

    def get_storage_data(self, address, key):
        return self.get_and_cache_account(
            utils.normalize_address(address)
        ).get_storage_data(key)

    def set_storage_data(self, address, key, value):
        acct = self.get_and_cache_account(utils.normalize_address(address))
        preval = acct.get_storage_data(key)
        acct.set_storage_data(key, value)
        self.journal.append(lambda: acct.set_storage_data(key, preval))
        self.set_and_journal(acct, "touched", True)

    def add_suicide(self, address):
        self.suicides.append(address)
        self.journal.append(lambda: self.suicides.pop())

    def add_log(self, log):
        for listener in self.log_listeners:
            listener(log)
        self.logs.append(log)
        self.journal.append(lambda: self.logs.pop())

    def add_receipt(self, receipt):
        self.receipts.append(receipt)
        self.journal.append(lambda: self.receipts.pop())

    def add_xshard_deposit_receipt(self, receipt):
        self.xshard_deposit_receipts.append(receipt)
        self.journal.append(lambda: self.xshard_deposit_receipts.pop())

    def add_refund(self, value):
        preval = self.refunds
        self.refunds += value
        self.journal.append(lambda: setattr(self.refunds, preval))

    def snapshot(self):
        return (
            self.trie.root_hash,
            len(self.journal),
            {k: copy.copy(getattr(self, k)) for k in STATE_DEFAULTS},
        )

    def revert(self, snapshot):
        h, L, auxvars = snapshot
        # Compatibility with weird geth+parity bug
        three_touched = self.cache[THREE].touched if THREE in self.cache else False
        while len(self.journal) > L:
            try:
                lastitem = self.journal.pop()
                lastitem()
            except Exception as e:
                print(e)
        if h != self.trie.root_hash:
            assert L == 0
            self.trie.root_hash = h
            self.cache = {}
        for k in STATE_DEFAULTS:
            setattr(self, k, copy.copy(auxvars[k]))
        if (
            three_touched and 2675000 < self.block_number < 2675200
        ):  # Compatibility with weird geth+parity bug
            self.delta_token_balance(THREE, self.shard_config.default_chain_token, 0)

    def set_param(self, k, v):
        preval = getattr(self, k)
        self.journal.append(lambda: setattr(self, k, preval))
        setattr(self, k, v)

    def account_exists(self, address):
        o = not self.get_and_cache_account(utils.normalize_address(address)).is_blank()
        return o

    def transfer_value(self, from_addr, to_addr, token_id, value):
        assert value >= 0
        if self.get_balance(from_addr, token_id=token_id) >= value:
            self.delta_token_balance(from_addr, token_id, -value)
            self.delta_token_balance(to_addr, token_id, value)
            return True
        return False

    def deduct_value(self, from_addr, token_id, value):
        assert value >= 0
        if self.get_balance(from_addr, token_id=token_id) >= value:
            self.delta_token_balance(from_addr, token_id, -value)
            return True
        return False

    def account_to_dict(self, address):
        return self.get_and_cache_account(utils.normalize_address(address)).to_dict()

    def commit(self, allow_empties=False):
        for addr, acct in self.cache.items():
            if acct.touched or acct.deleted:
                acct.commit()
                self.deletes.extend(acct.storage_trie.deletes)
                self.changed[addr] = True
                if self.account_exists(addr) or allow_empties:
                    if self.use_mock_evm_account:
                        assert len(acct.token_balances._balances) <= 1, "QKC only"
                        _acct = _MockAccount(
                            acct.nonce,
                            acct.token_balances.balance(token_id_encode("QKC")),
                            acct.storage,
                            acct.code_hash,
                        )
                    else:
                        _acct = _Account(
                            acct.nonce,
                            acct.token_balances.serialize(),
                            acct.storage,
                            acct.code_hash,
                            acct.full_shard_key,
                            b"",
                        )
                    self.trie.update(addr, rlp.encode(_acct))
                    if self.executing_on_head:
                        self.db.put(b"address:" + addr, rlp.encode(_acct))
                else:
                    self.trie.delete(addr)
                    if self.executing_on_head:
                        try:
                            self.db.remove(b"address:" + addr)
                        except KeyError:
                            pass
        self.deletes.extend(self.trie.deletes)
        self.trie.deletes = []
        self.cache = {}
        self.journal = []

    def to_dict(self):
        for addr in self.trie.to_dict().keys():
            self.get_and_cache_account(addr)
        return {encode_hex(addr): acct.to_dict() for addr, acct in self.cache.items()}

    def del_account(self, address):
        self.reset_balances(address)
        self.set_nonce(address, 0)
        self.set_code(address, b"")
        self.reset_storage(address)
        self.set_and_journal(
            self.get_and_cache_account(utils.normalize_address(address)),
            "deleted",
            True,
        )
        self.set_and_journal(
            self.get_and_cache_account(utils.normalize_address(address)),
            "touched",
            False,
        )
        # self.set_and_journal(self.get_and_cache_account(utils.normalize_address(address)), 'existent_at_start', False)

    def reset_storage(self, address):
        acct = self.get_and_cache_account(address)
        pre_cache = acct.storage_cache
        acct.storage_cache = {}
        self.journal.append(lambda: setattr(acct, "storage_cache", pre_cache))
        pre_root = acct.storage_trie.root_hash
        self.journal.append(lambda: setattr(acct.storage_trie, "root_hash", pre_root))
        acct.storage_trie.root_hash = BLANK_ROOT

    # Creates a snapshot from a state
    def to_snapshot(self, no_prevblocks=False):
        snapshot = dict()
        snapshot["state_root"] = "0x" + encode_hex(self.trie.root_hash)
        # Save non-state-root variables
        for k, default in STATE_DEFAULTS.items():
            default = copy.copy(default)
            v = getattr(self, k)
            if is_numeric(default):
                snapshot[k] = str(v)
            elif isinstance(default, (str, bytes)):
                snapshot[k] = "0x" + encode_hex(v)
            elif k == "prev_headers" and not no_prevblocks:
                snapshot[k] = [
                    prev_header_to_dict(h)
                    for h in v[: self.config["PREV_HEADER_DEPTH"]]
                ]
            elif k == "recent_uncles" and not no_prevblocks:
                snapshot[k] = {
                    str(n): ["0x" + encode_hex(h) for h in headers]
                    for n, headers in v.items()
                }
        return snapshot

    # Creates a state from a snapshot
    @classmethod
    def from_snapshot(cls, snapshot_data, env, executing_on_head=False):
        state = State(env=env)
        if "state_root" in snapshot_data:
            state.trie.root_hash = parse_as_bin(snapshot_data["state_root"])
        else:
            raise Exception("Must specify either alloc or state root parameter")
        for k, default in STATE_DEFAULTS.items():
            default = copy.copy(default)
            v = snapshot_data[k] if k in snapshot_data else None
            if is_numeric(default):
                setattr(state, k, parse_as_int(v) if k in snapshot_data else default)
            elif is_string(default):
                setattr(state, k, parse_as_bin(v) if k in snapshot_data else default)
            elif k == "prev_headers":
                if k in snapshot_data:
                    headers = [dict_to_prev_header(h) for h in v]
                else:
                    headers = default
                setattr(state, k, headers)
            elif k == "recent_uncles":
                if k in snapshot_data:
                    uncles = {}
                    for height, _uncles in v.items():
                        uncles[int(height)] = []
                        for uncle in _uncles:
                            uncles[int(height)].append(parse_as_bin(uncle))
                else:
                    uncles = default
                setattr(state, k, uncles)
        if executing_on_head:
            state.executing_on_head = True
        state.commit()
        state.changed = {}
        return state

    def ephemeral_clone(self):
        snapshot = self.to_snapshot(no_prevblocks=True)
        env2 = Env(OverlayDb(self.db), self.env.config)
        s = State.from_snapshot(snapshot, env2)
        for param in STATE_DEFAULTS:
            setattr(s, param, getattr(self, param))
        s.recent_uncles = self.recent_uncles
        s.prev_headers = self.prev_headers
        for acct in self.cache.values():
            assert not acct.touched or not acct.deleted
        s.journal = copy.copy(self.journal)
        s.cache = {}
        s.qkc_config = self.qkc_config
        s.sender_disallow_map = self.sender_disallow_map
        return s


def prev_header_to_dict(h):
    return {
        "hash": "0x" + encode_hex(h.hash),
        "number": str(h.number),
        "timestamp": str(h.timestamp),
        "difficulty": str(h.difficulty),
        "gas_used": str(h.gas_used),
        "gas_limit": str(h.gas_limit),
        "uncles_hash": "0x" + encode_hex(h.uncles_hash),
    }


BLANK_UNCLES_HASH = sha3(rlp.encode([]))


def dict_to_prev_header(h):
    return FakeHeader(
        hash=parse_as_bin(h["hash"]),
        number=parse_as_int(h["number"]),
        timestamp=parse_as_int(h["timestamp"]),
        difficulty=parse_as_int(h["difficulty"]),
        gas_used=parse_as_int(h.get("gas_used", "0")),
        gas_limit=parse_as_int(h["gas_limit"]),
        uncles_hash=parse_as_bin(
            h.get("uncles_hash", "0x" + encode_hex(BLANK_UNCLES_HASH))
        ),
    )
