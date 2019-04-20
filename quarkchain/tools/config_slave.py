"""
    python config_slave.py 127.0.0.1 38000 38006 127.0.0.2 18999 18002

will generate 4 slave server configs accordingly. will be used in deployment automation to configure a cluster.
usage: python config_slave.py <host1> <port1> <port2> <host2> <port3> ...
"""
import argparse
import collections
import json
import os

FILE = "../../testnet/2/cluster_config_template.json"
if "config" in os.environ:
    FILE = os.environ["config"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "hostports",
        nargs="+",
        metavar="hostports",
        help="Host and ports for slave config",
    )
    args = parser.parse_args()

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    ###############
    # parse hosts and ports to form a slave list
    ###############

    host_port_mapping = collections.defaultdict(list)
    last_host = None
    for host_or_port in args.hostports:  # type: str
        if not host_or_port.isdigit():  # host
            last_host = host_or_port
        else:  # port
            host_port_mapping[last_host].append(host_or_port)

    assert None not in host_port_mapping
    slave_num = sum(len(port_list) for port_list in host_port_mapping.values())
    # make sure number of slaves is power of 2
    assert slave_num > 0 and (slave_num & (slave_num - 1) == 0)

    slave_servers, i = [], 0
    for host, port_list in host_port_mapping.items():
        for port in port_list:
            s = {
                "HOST": host,
                "PORT": int(port),
                "ID": "S%d" % i,
                "CHAIN_MASK_LIST": [i | slave_num],
            }
            slave_servers.append(s)
            i += 1

    ###############
    # read config file and substitute with updated slave config
    ###############

    with open(FILE, "r+") as f:
        parsed_config = json.load(f)
        parsed_config["SLAVE_LIST"] = slave_servers
        f.seek(0)
        f.truncate()
        f.write(json.dumps(parsed_config, indent=4))


if __name__ == "__main__":
    main()
