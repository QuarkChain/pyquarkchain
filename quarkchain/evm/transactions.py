# -*- coding: utf-8 -*-
# Modified based on pyethereum under MIT license
import rlp
from rlp.sedes import big_endian_int, binary
from rlp.utils import str_to_bytes, ascii_chr
from ethereum.utils import encode_hex

from ethereum.exceptions import InvalidTransaction
from quarkchain.evm import opcodes
from ethereum import utils
from ethereum.utils import TT256, mk_contract_address, ecsign, ecrecover_to_pub, normalize_key
from quarkchain.utils import sha3_256


# in the yellow paper it is specified that s should be smaller than
# secpk1n (eq.205)
secpk1n = 115792089237316195423570985008687907852837564279074904382605163141518161494337
null_address = b'\xff' * 20


UINT32_MAX = (2 ** 32) - 1


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
    """

    fields = [
        ('nonce', big_endian_int),
        ('gasprice', big_endian_int),
        ('startgas', big_endian_int),
        ('to', utils.address),
        ('value', big_endian_int),
        ('data', binary),
        ('branchValue', big_endian_int),
        ('withdraw', big_endian_int),
        ('withdrawSign', big_endian_int),
        ('withdrawTo', binary),
        ('networkId', big_endian_int),
        ('v', big_endian_int),
        ('r', big_endian_int),
        ('s', big_endian_int),
    ]

    _sender = None

    def __init__(self, nonce, gasprice, startgas, to, value, data,
                 v=0, r=0, s=0, branchValue=1, withdraw=0, withdrawSign=1, withdrawTo=b'', networkId=1):
        self.data = None

        to = utils.normalize_address(to, allow_blank=True)

        super(
            Transaction,
            self).__init__(
            nonce,
            gasprice,
            startgas,
            to,
            value,
            data,
            branchValue,
            withdraw,
            withdrawSign,
            withdrawTo,
            networkId,
            v,
            r,
            s)

        if self.gasprice >= TT256 or self.startgas >= TT256 or \
                self.value >= TT256 or self.nonce >= TT256 or \
                self.withdraw >= TT256 or self.withdrawSign < 0 or \
                self.withdrawSign >= 2 or self.branchValue > UINT32_MAX:
            raise InvalidTransaction("Values way too high!")

    @property
    def sender(self):
        if not self._sender:
            # Determine sender
            if self.r == 0 and self.s == 0:
                self._sender = null_address
            else:
                sighash = sha3_256(rlp.encode(self, UnsignedTransaction))
                if self.r >= secpk1n or self.s >= secpk1n or self.r == 0 or self.s == 0:
                    raise InvalidTransaction("Invalid signature values!")
                pub = ecrecover_to_pub(sighash, self.v, self.r, self.s)
                if pub == b'\x00' * 64:
                    raise InvalidTransaction(
                        "Invalid signature (zero privkey cannot sign)")
                self._sender = sha3_256(pub)[-20:]
        return self._sender

    @property
    def network_id(self):
        return self.networkId

    @sender.setter
    def sender(self, value):
        self._sender = value

    def getWithdraw(self):
        if self.withdrawSign == 0:
            return -self.withdraw
        else:
            return self.withdraw

    def setWithdraw(self, value):
        if value < 0:
            self.withdrawSign = 0
            self.withdraw = -value
        else:
            self.withdraw = value

    def sign(self, key, network_id=None):
        """Sign this transaction with a private key.

        A potentially already existing signature would be overridden.
        """
        if network_id is not None:
            self.networkId = network_id
        rawhash = sha3_256(rlp.encode(self, UnsignedTransaction))
        key = normalize_key(key)

        self.v, self.r, self.s = ecsign(rawhash, key)

        self._sender = utils.privtoaddr(key)
        return self

    @property
    def hash(self):
        return sha3_256(rlp.encode(self))

    def to_dict(self):
        d = {}
        for name, _ in self.__class__.fields:
            d[name] = getattr(self, name)
            if name in ('to', 'data'):
                d[name] = '0x' + encode_hex(d[name])
        d['sender'] = '0x' + encode_hex(self.sender)
        d['hash'] = '0x' + encode_hex(self.hash)
        return d

    @property
    def intrinsic_gas_used(self):
        num_zero_bytes = str_to_bytes(self.data).count(ascii_chr(0))
        num_non_zero_bytes = len(self.data) - num_zero_bytes
        return (opcodes.GTXCOST +
                #         + (0 if self.to else opcodes.CREATE[3])
                opcodes.GTXDATAZERO * num_zero_bytes +
                opcodes.GTXDATANONZERO * num_non_zero_bytes)

    @property
    def creates(self):
        "returns the address of a contract created by this tx"
        if self.to in (b'', '\0' * 20):
            return mk_contract_address(self.sender, self.nonce)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.hash == other.hash

    def __lt__(self, other):
        return isinstance(other, self.__class__) and self.hash < other.hash

    def __hash__(self):
        return utils.big_endian_to_int(self.hash)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '<Transaction(%s)>' % encode_hex(self.hash)[:4]

    def __structlog__(self):
        return encode_hex(self.hash)

    # This method should be called for block numbers >= HOMESTEAD_FORK_BLKNUM only.
    # The >= operator is replaced by > because the integer division N/2 always produces the value
    # which is by 0.5 less than the real N/2
    def check_low_s_metropolis(self):
        if self.s > secpk1n // 2:
            raise InvalidTransaction("Invalid signature S value!")

    def check_low_s_homestead(self):
        if self.s > secpk1n // 2 or self.s == 0:
            raise InvalidTransaction("Invalid signature S value!")


UnsignedTransaction = Transaction.exclude(['v', 'r', 's'])
