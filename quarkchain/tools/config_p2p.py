"""
    python config_p2p.py <64 character hex>

will update PRIV_KEY field in P2P section.
"""
import argparse
import json
import os

FILE = "../../testnet/2/cluster_config_template.json"
if "QKC_CONFIG" in os.environ:
    FILE = os.environ["QKC_CONFIG"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("privkey", metavar="privkey", help="Private key for P2P config")
    args = parser.parse_args()

    privkey = args.privkey
    if not privkey or not privkey.isalnum() or len(privkey) != 64:
        raise ValueError("Invalid private key")

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    with open(FILE, "r+") as f:
        parsed_config = json.load(f)
        if "P2P" not in parsed_config:
            raise ValueError("P2P not found in config")
        parsed_config["P2P"]["PRIV_KEY"] = privkey
        f.seek(0)
        f.truncate()
        f.write(json.dumps(parsed_config, indent=4))


if __name__ == "__main__":
    main()
