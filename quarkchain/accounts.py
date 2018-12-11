from builtins import hex
from builtins import object
from functools import total_ordering
import json
import os
import pbkdf2
from random import SystemRandom
import shutil
from uuid import UUID, uuid4
from Crypto.Cipher import (
    AES,
)  # Crypto imports necessary for AES encryption of the private key
from Crypto.Hash import SHA256
from Crypto.Util import Counter
from quarkchain.core import Address, Identity
from quarkchain.evm.slogging import get_logger
from quarkchain.evm.utils import decode_hex, encode_hex, is_string, sha3, to_string


log = get_logger("accounts")

DEFAULT_COINBASE = decode_hex("de0b295669a9fd93d5f28d9ec85e40f4cb697bae")

random = SystemRandom()


@total_ordering
class MinType(object):
    """ Return Min value for sorting comparison

    This class is used for comparing unorderded types. e.g., NoneType
    """

    def __le__(self, other):
        return True

    def __eq__(self, other):
        return self is other


class Account(object):
    """
    An account that represents a (privatekey, address) pair.
    Uses quarkchain.core's Identity and Address classes.
    """

    def __init__(self, identity, address):
        self.id = uuid4()  # generates an 128-bit uuid using urandom
        self.identity = identity
        self.qkc_address = address

    @staticmethod
    def new(key=None):
        """
        Create a new account.
        :param key: the private key to import, or None to generate a random one
        """
        if key is None:
            identity = Identity.create_random_identity()
        else:
            if not isinstance(key, str):
                raise Exception("Imported key must be a hexadecimal string")

            identity = Identity.create_from_key(bytes.fromhex(key))
        address = Address.create_from_identity(identity)
        return Account(identity, address)

    @staticmethod
    def load(path, password):
        """Load an account from a keystore file.

        :param path: full path to the keyfile
        :param password: the password to decrypt the key file or `None` to leave it encrypted
        """
        with open(path) as f:
            keystore_jsondata = json.load(f)
        privkey = Account._decode_keystore_json(keystore_jsondata, password).hex()
        account = Account.new(key=privkey)
        if "id" in keystore_jsondata:
            account.id = keystore_jsondata["id"]
        return account

    def dump(self, password, include_address=True, write=False, directory="~/keystore"):
        """
        Dump the keystore for disk storage.
        :param password: used to encrypt your private key
        :param include_address: flag denoting if the address should be included or not
        """
        if not is_string(password):
            password = to_string(password)

        keystore_json = self._make_keystore_json(password)
        if include_address:
            keystore_json["address"] = self.address

        json_str = json.dumps(keystore_json, indent=4)
        if write:
            path = "{0}/{1}.json".format(directory, str(self.id))
            with open(path, "w") as f:
                f.write(json_str)
        return json_str

    def _make_keystore_json(self, password):
        """
        Generate the keystore json that follows the Version 3 specification:
        https://github.com/ethereum/wiki/wiki/Web3-Secret-Storage-Definition#definition

        Uses pbkdf2 for password encryption, and AES-128-CTR as the cipher.
        """
        # Get the hash function and default parameters
        kdfparams = {
            "prf": "hmac-sha256",
            "dklen": 32,
            "c": 262144,
            "salt": encode_hex(os.urandom(16)),
        }

        # Compute derived key
        derivedkey = pbkdf2.PBKDF2(
            password, decode_hex(kdfparams["salt"]), kdfparams["c"], SHA256
        ).read(kdfparams["dklen"])

        # Produce the encryption key and encrypt using AES
        enckey = derivedkey[:16]
        cipherparams = {"iv": encode_hex(os.urandom(16))}
        iv = int.from_bytes(decode_hex(cipherparams["iv"]), byteorder="big")
        ctr = Counter.new(128, initial_value=iv, allow_wraparound=True)
        encryptor = AES.new(enckey, AES.MODE_CTR, counter=ctr)
        c = encryptor.encrypt(self.identity.key)

        # Compute the MAC
        mac = sha3(derivedkey[16:32] + c)

        # Return the keystore json
        return {
            "crypto": {
                "cipher": "aes-128-ctr",
                "ciphertext": encode_hex(c),
                "cipherparams": cipherparams,
                "kdf": "pbkdf2",
                "kdfparams": kdfparams,
                "mac": encode_hex(mac),
                "version": 1,
            },
            "id": self.uuid,
            "version": 3,
        }

    @staticmethod
    def _decode_keystore_json(jsondata, password):
        # Get key derivation function (kdf) and parameters
        kdfparams = jsondata["crypto"]["kdfparams"]

        # Compute derived key
        derivedkey = pbkdf2.PBKDF2(
            password, decode_hex(kdfparams["salt"]), kdfparams["c"], SHA256
        ).read(kdfparams["dklen"])

        assert len(derivedkey) >= 32, "Derived key must be at least 32 bytes long"

        # Get cipher and parameters and decrypt using AES
        cipherparams = jsondata["crypto"]["cipherparams"]
        enckey = derivedkey[:16]
        iv = int.from_bytes(decode_hex(cipherparams["iv"]), byteorder="big")
        ctr = Counter.new(128, initial_value=iv, allow_wraparound=True)
        encryptor = AES.new(enckey, AES.MODE_CTR, counter=ctr)
        ctext = decode_hex(jsondata["crypto"]["ciphertext"])
        o = encryptor.decrypt(ctext)

        # Compare the provided MAC with a locally computed MAC
        mac1 = sha3(derivedkey[16:32] + ctext)
        mac2 = decode_hex(jsondata["crypto"]["mac"])
        if mac1 != mac2:
            raise ValueError("MAC mismatch. Password incorrect?")
        return o

    @property
    def address(self):
        return self.qkc_address.to_hex()

    @property
    def privkey(self):
        return self.identity.key.hex()

    @property
    def uuid(self):
        return str(self.id)

    def __repr__(self):
        return "<Account(address={address}, id={id})>".format(
            address=self.address, id=self.uuid
        )


"""
--import-key = key.json
--unlock <password dialog>
--password  passwordfile
--newkey    <password dialog>


"""
