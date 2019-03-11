# -*- coding: utf-8 -*-
# Modified based on pyethereum under MIT license
import rlp
from quarkchain.evm import utils
from quarkchain.evm.exceptions import InvalidTransaction
from quarkchain.evm.utils import (
    TT256,
    mk_contract_address,
    ecsign,
    ecrecover_to_pub,
    normalize_key,
)
from quarkchain.evm.utils import encode_hex
from rlp.sedes import big_endian_int, binary, BigEndianInt
from quarkchain.rlp.utils import str_to_bytes, ascii_chr

from quarkchain.evm import opcodes
from quarkchain.utils import sha3_256, check, TOKEN_ID_MAX, SHARD_KEY_MAX
from quarkchain.evm.solidity_abi_utils import tx_to_typed_data, typed_signature_hash

# in the yellow paper it is specified that s should be smaller than
# secpk1n (eq.205)
secpk1n = 115792089237316195423570985008687907852837564279074904382605163141518161494337
null_address = b"\xff" * 20


class Transaction(rlp.Serializable):

    """
    A transaction is stored as:
    [nonce, gasprice, startgas, to, value, data, v, r, s]

    nonce is the number of transactions already sent by that account, encoded
    in binary form (eg.  0 -> '', 7 -> '\x07', 1000 -> '\x03\xd8').

    (v,r,s) is the raw Electrum-style signature of the transaction without the
    signature made with the private key corresponding to the sending account,
    with 0 <= v <= 3. From an Electrum-style signature (65 bytes) it is
    possible to extract the public key, and thereby the address, directly.

    A valid transaction is one where:
    (i) the signature is well-formed (ie. 0 <= v <= 3, 0 <= r < P, 0 <= s < N,
        0 <= r < P - N if v >= 2), and
    (ii) the sending account has enough funds to pay the fee and the value.

    There are 3 types of transactions:
        1. Value transfer. In-shard transaction if from_full_shard_key and to_full_shard_key
        refer to the same shard, otherwise it is a cross-shard transaction.

        2. Contract creation. 'to' must be empty. from_full_shard_key and to_full_shard_key
        must refer to the same shard id. The contract address will have the same
        full shard id as to_full_shard_key. If the contract does not invoke other contract
        normally the to_full_shard_key should be the same as from_full_shard_key.

        3. Contract call. from_full_shard_key and to_full_shard_key must refer to the same
        shard id based on the current number of shards in the network. It is possible
        a reshard event would invalidate a tx that was valid before the reshard.
    """

    fields = [
        ("nonce", big_endian_int),
        ("gasprice", big_endian_int),
        ("startgas", big_endian_int),
        ("to", utils.address),
        ("value", big_endian_int),
        ("data", binary),
        ("network_id", big_endian_int),
        ("from_full_shard_key", BigEndianInt(4)),
        ("to_full_shard_key", BigEndianInt(4)),
        ("gas_token_id", big_endian_int),
        ("transfer_token_id", big_endian_int),
        ("version", big_endian_int),
        ("v", big_endian_int),
        ("r", big_endian_int),
        ("s", big_endian_int),
    ]

    _sender = None

    def __init__(
        self,
        nonce,
        gasprice,
        startgas,
        to,
        value,
        data,
        gas_token_id,
        transfer_token_id,
        v=0,
        r=0,
        s=0,
        from_full_shard_key=0,
        to_full_shard_key=0,
        network_id=1,
        version=0,
    ):
        self.quark_chain_config = None

        to = utils.normalize_address(to, allow_blank=True)

        super(Transaction, self).__init__(
            nonce,
            gasprice,
            startgas,
            to,
            value,
            data,
            network_id,
            from_full_shard_key,
            to_full_shard_key,
            gas_token_id,
            transfer_token_id,
            version,
            v,
            r,
            s,
        )

        if (
            self.gasprice >= TT256
            or self.startgas >= TT256
            or self.value >= TT256
            or self.nonce >= TT256
            or self.version >= TT256
            or self.gas_token_id > TOKEN_ID_MAX
            or self.transfer_token_id > TOKEN_ID_MAX
            or self.from_full_shard_key > SHARD_KEY_MAX
            or self.to_full_shard_key > SHARD_KEY_MAX
        ):
            raise InvalidTransaction("Values way too high!")

    @property
    def sender(self):
        if not self._sender:
            # Determine sender
            if self.r == 0 and self.s == 0:
                self._sender = null_address
            else:
                if self.r >= secpk1n or self.s >= secpk1n or self.r == 0 or self.s == 0:
                    raise InvalidTransaction("Invalid signature values!")
                if self.version == 0:
                    pub = ecrecover_to_pub(self.hash_unsigned, self.v, self.r, self.s)
                if self.version == 1:
                    pub = ecrecover_to_pub(self.hash_typed, self.v, self.r, self.s)
                if pub == b"\x00" * 64:
                    raise InvalidTransaction(
                        "Invalid signature (zero privkey cannot sign)"
                    )
                self._sender = sha3_256(pub)[-20:]
        return self._sender

    @sender.setter
    def sender(self, value):
        self._sender = value

    def sign(self, key, network_id=None):
        """Sign this transaction with a private key.

        A potentially already existing signature would be overridden.
        """
        if network_id is not None:
            self.network_id = network_id
        key = normalize_key(key)

        self._in_mutable_context = True
        self.v, self.r, self.s = ecsign(self.hash_unsigned, key)
        self.version = 0
        self._in_mutable_context = False

        self._sender = utils.privtoaddr(key)
        return self

    @property
    def hash(self):
        return sha3_256(rlp.encode(self))

    @property
    def hash_unsigned(self):
        return sha3_256(rlp.encode(unsigned_tx_from_tx(self), UnsignedTransaction))

    @property
    def hash_typed(self):
        return bytes.fromhex(typed_signature_hash(tx_to_typed_data(self))[2:])

    def to_dict(self):
        d = {}
        for name, _ in self.__class__._meta.fields:
            d[name] = getattr(self, name)
            if name in ("to", "data"):
                d[name] = "0x" + encode_hex(d[name])
        d["sender"] = "0x" + encode_hex(self.sender)
        d["hash"] = "0x" + encode_hex(self.hash)
        return d

    @property
    def intrinsic_gas_used(self):
        num_zero_bytes = str_to_bytes(self.data).count(ascii_chr(0))
        num_non_zero_bytes = len(self.data) - num_zero_bytes
        return (
            opcodes.GTXCOST
            + (opcodes.CREATE[3] if not self.to else 0)
            + opcodes.GTXDATAZERO * num_zero_bytes
            + opcodes.GTXDATANONZERO * num_non_zero_bytes
            + (opcodes.GTXXSHARDCOST if self.is_cross_shard else 0)
        )

    @property
    def creates(self):
        "returns the address of a contract created by this tx"
        if self.to in (b"", "\0" * 20):
            return mk_contract_address(self.sender, self.nonce)

    def set_quark_chain_config(self, quark_chain_config):
        self.quark_chain_config = quark_chain_config

    @property
    def from_chain_id(self):
        return self.from_full_shard_key >> 16

    @property
    def to_chain_id(self):
        return self.to_full_shard_key >> 16

    @property
    def from_shard_size(self):
        check(self.quark_chain_config)
        return self.quark_chain_config.get_shard_size_by_chain_id(self.from_chain_id)

    @property
    def to_shard_size(self):
        check(self.quark_chain_config)
        return self.quark_chain_config.get_shard_size_by_chain_id(self.to_chain_id)

    @property
    def from_shard_id(self):
        shard_mask = self.from_shard_size - 1
        return self.from_full_shard_key & shard_mask

    @property
    def to_shard_id(self):
        shard_mask = self.to_shard_size - 1
        return self.to_full_shard_key & shard_mask

    @property
    def from_full_shard_id(self):
        return self.from_chain_id << 16 | self.from_shard_size | self.from_shard_id

    @property
    def to_full_shard_id(self):
        return self.to_chain_id << 16 | self.to_shard_size | self.to_shard_id

    @property
    def is_cross_shard(self):
        return (
            self.from_chain_id != self.to_chain_id
            or self.from_shard_id != self.to_shard_id
        )

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.hash == other.hash

    def __lt__(self, other):
        return isinstance(other, self.__class__) and self.hash < other.hash

    def __hash__(self):
        return utils.big_endian_to_int(self.hash)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "<Transaction(%s)>" % encode_hex(self.hash)[:4]

    def __structlog__(self):
        return encode_hex(self.hash)


class UnsignedTransaction(rlp.Serializable):
    fields = [
        (field, sedes)
        for field, sedes in Transaction._meta.fields
        if field not in ["v", "r", "s", "version"]
    ]


def unsigned_tx_from_tx(tx):
    return UnsignedTransaction(
        nonce=tx.nonce,
        gasprice=tx.gasprice,
        startgas=tx.startgas,
        to=tx.to,
        value=tx.value,
        data=tx.data,
        from_full_shard_key=tx.from_full_shard_key,
        to_full_shard_key=tx.to_full_shard_key,
        network_id=tx.network_id,
        gas_token_id=tx.gas_token_id,
        transfer_token_id=tx.transfer_token_id,
    )
