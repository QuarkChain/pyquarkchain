from quarkchain.utils import is_p2


def create_cluster_config(slaveCount, ip, p2pPort, clusterPortStart, jsonRpcPort, seedHost, seedPort, dbPrefix=""):
    if slaveCount <= 0 or not is_p2(slaveCount):
        print("Slave count must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": ip,
        "port": clusterPortStart,
        "db_path": dbPrefix + "m",
        "server_port": p2pPort,
        "json_rpc_port": jsonRpcPort,
        "seed_host": seedHost,
        "seed_port": seedPort,
    }
    config["slaves"] = []
    for i in range(slaveCount):
        mask = i | slaveCount
        config["slaves"].append({
            "id": "S{}".format(i),
            "ip": ip,
            "port": clusterPortStart + i + 1,
            "shard_masks": [mask],
            "db_path": dbPrefix + str(i)
        })

    return config
