import unittest
from quarkchain.evm.solidity_abi_utils import (
    tx_to_typed_data,
    solidity_pack,
    typed_signature_hash,
)
from quarkchain.evm.transactions import Transaction


class TestTypedSignature(unittest.TestCase):

    raw_tx = Transaction(
        nonce=0x0d,
        gasprice=0x02540be400,
        startgas=0x7530,
        to=bytes.fromhex("314b2cd22c6d26618ce051a58c65af1253aecbb8"),
        value=0x056bc75e2d63100000,
        data=b"",
        from_full_shard_key=0xc47decfd,
        to_full_shard_key=0xc49c1950,
        network_id=0x03,
    )

    tx = [
        {"type": "uint256", "name": "nonce", "value": "0x0d"},
        {"type": "uint256", "name": "gasPrice", "value": "0x02540be400"},
        {"type": "uint256", "name": "gasLimit", "value": "0x7530"},
        {
            "type": "uint160",
            "name": "to",
            "value": "0x314b2cd22c6d26618ce051a58c65af1253aecbb8",
        },
        {"type": "uint256", "name": "value", "value": "0x056bc75e2d63100000"},
        {"type": "bytes", "name": "data", "value": "0x"},
        {"type": "uint32", "name": "fromFullShardId", "value": "0xc47decfd"},
        {"type": "uint32", "name": "toFullShardId", "value": "0xc49c1950"},
        {"type": "uint256", "name": "networkId", "value": "0x03"},
        {"type": "string", "name": "qkcDomain", "value": "bottom-quark"},
    ]

    def test_typed(self):
        assert tx_to_typed_data(self.raw_tx) == self.tx

    def test_solidity_pack(self):
        schema = list(map(lambda x: "{} {}".format(x["type"], x["name"]), self.tx))
        types = list(map(lambda x: x["type"], self.tx))
        data = list(
            map(
                lambda x: bytes.fromhex(x["value"][2:])
                if x["type"] == "bytes"
                else x["value"],
                self.tx,
            )
        )
        h1 = solidity_pack(["string"] * len(self.tx), schema)
        h2 = solidity_pack(types, data)
        assert (
            h1.hex()
            == "75696e74323536206e6f6e636575696e7432353620676173507269636575696e74323536206761734c696d697475696e7431363020746f75696e743235362076616c75656279746573206461746175696e7433322066726f6d46756c6c5368617264496475696e74333220746f46756c6c5368617264496475696e74323536206e6574776f726b4964737472696e6720716b63446f6d61696e"
        )
        assert (
            h2.hex()
            == "000000000000000000000000000000000000000000000000000000000000000d00000000000000000000000000000000000000000000000000000002540be4000000000000000000000000000000000000000000000000000000000000007530314b2cd22c6d26618ce051a58c65af1253aecbb80000000000000000000000000000000000000000000000056bc75e2d63100000c47decfdc49c19500000000000000000000000000000000000000000000000000000000000000003626f74746f6d2d717561726b"
        )

    def test_typed_signature_hash(self):
        h = typed_signature_hash(self.tx)
        assert h == "0x57dfcc7be8e4249fb6e75a45dc5ecdfed0309ed951b6adc69b8a659c7eca33bf"

    def test_recover(self):
        """
        """
        self.raw_tx._in_mutable_context = True
        self.raw_tx.version = 1
        self.raw_tx.r = (
            0xb5145678e43df2b7ea8e0e969e51dbf72c956dd52e234c95393ad68744394855
        )
        self.raw_tx.s = (
            0x44515b465dbbf746a484239c11adb98f967e35347e17e71b84d850d8e5c38a6a
        )
        self.raw_tx.v = 0x1b
        self.raw_tx._in_mutable_context = False
        assert self.raw_tx.sender == bytes.fromhex(
            "8b74a79290a437aa9589be3227d9bb81b22beff1"
        )
