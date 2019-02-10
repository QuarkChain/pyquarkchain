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
        network_id=0x03,
        from_full_shard_key=0xc47decfd,
        to_full_shard_key=0xc49c1950,
        gas_token_id=0x0111,
        transfer_token_id=0x0222,
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
        {"type": "uint256", "name": "networkId", "value": "0x03"},
        {"type": "uint32", "name": "fromFullShardKey", "value": "0xc47decfd"},
        {"type": "uint32", "name": "toFullShardKey", "value": "0xc49c1950"},
        {"type": "uint64", "name": "gasTokenId", "value": "0x0111"},
        {"type": "uint64", "name": "transferTokenId", "value": "0x0222"},
        {"type": "string", "name": "qkcDomain", "value": "bottom-quark"},
    ]

    def test_typed(self):
        self.assertEqual(tx_to_typed_data(self.raw_tx), self.tx)

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

        self.assertEqual(
            h1.hex(),
            "75696e74323536206e6f6e636575696e7432353620676173507269636575696e74323536206761734c696d697475696e7431363020746f75696e743235362076616c75656279746573206461746175696e74323536206e6574776f726b496475696e7433322066726f6d46756c6c53686172644b657975696e74333220746f46756c6c53686172644b657975696e74363420676173546f6b656e496475696e743634207472616e73666572546f6b656e4964737472696e6720716b63446f6d61696e",
        )
        self.assertEqual(
            h2.hex(),
            "000000000000000000000000000000000000000000000000000000000000000d00000000000000000000000000000000000000000000000000000002540be4000000000000000000000000000000000000000000000000000000000000007530314b2cd22c6d26618ce051a58c65af1253aecbb80000000000000000000000000000000000000000000000056bc75e2d631000000000000000000000000000000000000000000000000000000000000000000003c47decfdc49c195000000000000001110000000000000222626f74746f6d2d717561726b",
        )

    def test_typed_signature_hash(self):
        h = typed_signature_hash(self.tx)
        self.assertEqual(
            h, "0xe768719d0a211ffb0b7f9c7bc6af9286136b3dd8b6be634a57dc9d6bee35b492"
        )

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
        self.assertEqual(
            self.raw_tx.sender.hex(), "2e6144d0a4786e6f62892eee59c24d1e81e33272"
        )
