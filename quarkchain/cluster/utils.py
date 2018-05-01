from quarkchain.utils import is_p2


def create_cluster_config(args):
    num_slaves = args.num_slaves
    if num_slaves <= 0 or not is_p2(num_slaves):
        print("Number of slaves must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": args.ip,
        "port": args.port_start,
        "db_path": args.db_prefix + "m",
        "server_port": args.p2p_port
    }
    config["slaves"] = []
    for i in range(num_slaves):
        mask = i | num_slaves
        config["slaves"].append({
            "id": "S{}".format(i),
            "ip": args.ip,
            "port": args.port_start + i + 1,
            "shard_masks": [mask],
            "db_path": args.db_prefix + str(i)
        })

    return config
