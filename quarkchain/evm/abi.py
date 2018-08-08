import re
from typing import Iterable
from quarkchain.utils import sha3_256


def intToBytes(n: int):
    """
    similar to hex(n), but align to bytes
    """
    return n.to_bytes((n.bit_length() + 7) // 8, 'big')


def txToTypedData(rawTx):
    """
    see UnsignedTransaction, exludes ['v', 'r', 's', 'version']
    """
    return [
      {
        "type": "uint256",
        "name": "nonce",
        "value": "0x{}".format(intToBytes(rawTx.nonce).hex())
      },
      {
        "type": "uint256",
        "name": "gasPrice",
        "value": "0x{}".format(intToBytes(rawTx.gasprice).hex())
      },
      {
        "type": "uint256",
        "name": "gasLimit",
        "value": "0x{}".format(intToBytes(rawTx.startgas).hex())
      },
      {
        "type": "uint160",
        "name": "to",
        "value": "0x{}".format(rawTx.to.hex())
      },
      {
        "type": "uint256",
        "name": "value",
        "value": "0x{}".format(intToBytes(rawTx.value).hex())
      },
      {
        "type": "bytes",
        "name": "data",
        "value": "0x{}".format(rawTx.data.hex())
      },
      {
        "type": "uint32",
        "name": "fromFullShardId",
        "value": "0x{}".format(intToBytes(rawTx.fromFullShardId).hex())
      },
      {
        "type": "uint32",
        "name": "toFullShardId",
        "value": "0x{}".format(intToBytes(rawTx.toFullShardId).hex())
      },
      {
        "type": "uint256",
        "name": "networkId",
        "value": "0x{}".format(intToBytes(rawTx.networkId).hex())
      },
      {
        "type": "string",
        "name": "qkcDomain",
        "value": "bottom-quark"
      }
    ]


def solidityPack(types: Iterable, values: Iterable):
    """
    Port of ABI.solidityPack
    https://github.com/ethereumjs/ethereumjs-abi/blob/00ba8463a7f7a67fcad737ff9c2ebd95643427f7/lib/index.js#L441
    Serialize values according to types
    returns bytes
    """
    if len(types) != len(values):
        raise ValueError("Number of types are not matching the values")
    retv = bytes()
    for t, v in zip(types, values):
        if t == "bytes":
            retv += v
        elif t == "string":
            retv += v.encode(encoding="utf-8")
        elif t in ("bool", "address"):
            raise ValueError("unsupported type for now")
        elif t.startswith("bytes"):
            size = int(re.search(r'\d+', t).group())
            if size < 1 or size > 32:
                raise ValueError("unsupported byte size")
            value = bytes.fromhex(v[2:])
            if len(value) > size:
                raise ValueError("data is larger than size")
            retv += value.rjust(size, b'\x00')
        elif t.startswith("int") or t.startswith("uint"):
            size = int(re.search(r'\d+', t).group())
            if size % 8 != 0 or size < 8 or size > 256:
                raise ValueError("unsupported int size")
            value = bytes.fromhex(v[2:])
            if len(value) > size // 8:
                raise ValueError("data is larger than size")
            retv += value.rjust(size // 8, b'\x00')
        else:
            raise ValueError("Unsupported or invalid type: {}".format(t))
    return retv


def soliditySHA3(types: Iterable, values: Iterable):
    """
    returns 0x hex str
    """
    return "0x{}".format(sha3_256(solidityPack(types, values)).hex())


def typedSignatureHash(tx):
    schema = list(map(lambda x: "{} {}".format(x["type"], x["name"]), tx))
    types = list(map(lambda x: x["type"], tx))
    data = list(map(lambda x: bytes.fromhex(x['value'][2:]) if x['type'] == "bytes" else x['value'], tx))
    return soliditySHA3(
        ['bytes32', 'bytes32'],
        [
            soliditySHA3(['string'] * len(tx), schema),
            soliditySHA3(types, data)
        ]
    )
