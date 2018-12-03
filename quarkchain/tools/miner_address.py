import re
import ecdsa
import os
from utils import underline, colorify
from quarkchain.utils import sha3_256

FILE = "../../testnet/2/cluster_config_template.json"


def gen_address():
    sk = ecdsa.SigningKey.generate(curve=ecdsa.SECP256k1)
    print(
        colorify(
            "A new address has been generated for you, your private key is: ", "grey"
        )
    )
    print(sk.to_string().hex())
    address = sha3_256(sk.verifying_key.to_string())[-20:].hex()
    print(colorify("Address is:", "grey") + " 0x{}".format(address))
    return address


def touch_file(address: str):
    # sanitize the input address first
    with open(FILE, "r+") as f:
        content = f.read()
        content = re.sub(
            r"\"COINBASE_ADDRESS\": \"[0-9a-fA-F]{40}",
            '"COINBASE_ADDRESS": "{}'.format(address),
            content,
        )
        f.seek(0)
        f.truncate()
        f.write(content)

    print(
        colorify(
            "{} has been updated with {}, run `git diff` to double check".format(
                FILE, address
            ),
            "green",
        )
    )


def main():
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    print(
        "This tool will edit {} with your address; if you don't have one, one will be generated for you".format(
            FILE
        )
    )
    print(colorify("--------------", "green"))
    address = input(
        underline(
            "Please paste your QKC address (make sure you have the private key for it)"
        )
        + ": "
    )
    if not address:
        print(
            "your input is empty, so we will generate one for you; "
            + underline("be sure to keep the private key in a safe place")
        )
        address = gen_address()
        touch_file(address)
        return

    if address.startswith("0x"):
        address = address[2:]
    if len(address) == 40:
        print(
            "your input is ETH address, but it's OK, we actually just need the 20-byte address"
        )
        touch_file(address)
    elif len(address) == 48:
        touch_file(address[:40])
    else:
        print(
            colorify(
                "Wrong address length, please provide either 20-byte ETH address or 24-byte QKC address",
                "red",
            )
        )


if __name__ == "__main__":
    main()
