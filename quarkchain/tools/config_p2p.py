"""
    python config_p2p.py \
            --privkey <64 character hex> \
            --bootnodes enode://xxx@yyy:zzz,...

will update PRIV_KEY and BOOT_NODES fields in P2P section.
"""
import argparse
import json
import os
import re
import socket

FILE = "../../testnet/2/cluster_config_template.json"
if "QKC_CONFIG" in os.environ:
    FILE = os.environ["QKC_CONFIG"]


def validate_bootnodes(bootnodes_str):
    """Expect comma-separated string like 'enode://<128 char pub key>@<ip>:<port>'."""
    if not bootnodes_str:
        return
    p = re.compile("^enode://([a-z0-9]+)@(.+):([0-9]+)$")
    for bootnode in bootnodes_str.split(","):
        res = p.match(bootnode)
        if not res:
            raise ValueError("Invalid boot nodes")
        pubkey, ip, port = res.group(1, 2, 3)
        if len(pubkey) != 128:
            raise ValueError("Invalid public key in boot nodes")
        try:
            socket.inet_aton(ip)
        except socket.error:
            raise ValueError("Invalid IP in boot nodes")
        if not port.isnumeric():
            raise ValueError("Invalid port in boot nodes")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--privkey", help="Private key for P2P config")
    parser.add_argument("--bootnodes", help="List of bootnodes")
    args = parser.parse_args()

    privkey = args.privkey
    if privkey and (not privkey.isalnum() or len(privkey) != 64):
        raise ValueError("Invalid private key")

    validate_bootnodes(args.bootnodes)

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    with open(FILE, "r+") as f:
        parsed_config = json.load(f)
        if "P2P" not in parsed_config:
            raise ValueError("P2P not found in config")
        p2p = parsed_config["P2P"]
        p2p["PRIV_KEY"] = privkey or ""
        p2p["BOOT_NODES"] = args.bootnodes or ""
        f.seek(0)
        f.truncate()
        f.write(json.dumps(parsed_config, indent=4))


if __name__ == "__main__":
    main()
