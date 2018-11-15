"""
This is a command line tool for generating private key used in QuarkChain Network
"""
from quarkchain.p2p import ecies


def main():
    privkey = ecies.generate_privkey()
    print("Here is a new SECP256K1 private key, please keep it in a safe place:")
    print("*" * 50)
    print(privkey.to_bytes().hex())
    print("*" * 50)
    print("You can pass it to --privkey when running cluster.py")
    print(
        "If you want to use the key for bootnode, here is what you want others to use (replace IP/PORT):"
    )
    print("*" * 50)
    print("enode://{}@IP:PORT".format(privkey.public_key.to_bytes().hex()))
    print("*" * 50)


if __name__ == "__main__":
    main()
