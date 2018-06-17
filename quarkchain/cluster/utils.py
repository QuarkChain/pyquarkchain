from quarkchain.utils import is_p2


def create_cluster_config(slaveCount, ip, p2pPort, clusterPortStart, jsonRpcPort,
                          seedHost, seedPort, dbPrefix, **kwargs):
    if slaveCount <= 0 or not is_p2(slaveCount):
        print("Slave count must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": ip,
        "db_path": dbPrefix + "m",
        "server_port": p2pPort,
        "json_rpc_port": jsonRpcPort,
        "seed_host": seedHost,
        "seed_port": seedPort,
        "devp2p": kwargs['devp2p'],
        "devp2p_port": kwargs['devp2p_port'],
        "devp2p_bootstrap_host": kwargs['devp2p_bootstrap_host'],
        "devp2p_bootstrap_port": kwargs['devp2p_bootstrap_port'],
        "devp2p_min_peers": kwargs['devp2p_min_peers'],
        "devp2p_max_peers": kwargs['devp2p_max_peers'],
    }
    config["slaves"] = []
    for i in range(slaveCount):
        mask = i | slaveCount
        config["slaves"].append({
            "id": "S{}".format(i),
            "ip": ip,
            "port": clusterPortStart + i,
            "shard_masks": [mask],
            "db_path": dbPrefix + str(i)
        })

    return config
