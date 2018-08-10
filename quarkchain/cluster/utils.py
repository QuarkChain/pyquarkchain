from quarkchain.utils import is_p2


def create_cluster_config(
    slave_count,
    ip,
    p2p_port,
    cluster_port_start,
    json_rpc_port,
    json_rpc_private_port,
    seed_host,
    seed_port,
    db_path_root="",
    **kwargs
):
    if slave_count <= 0 or not is_p2(slave_count):
        print("Slave count must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": ip,
        "db_path_root": db_path_root,
        "server_port": p2p_port,
        "json_rpc_port": json_rpc_port,
        "json_rpc_private_port": json_rpc_private_port,
        "seed_host": seed_host,
        "seed_port": seed_port,
        "devp2p": kwargs["devp2p"],
        "devp2p_ip": kwargs["devp2p_ip"],
        "devp2p_port": kwargs["devp2p_port"],
        "devp2p_bootstrap_host": kwargs["devp2p_bootstrap_host"],
        "devp2p_bootstrap_port": kwargs["devp2p_bootstrap_port"],
        "devp2p_min_peers": kwargs["devp2p_min_peers"],
        "devp2p_max_peers": kwargs["devp2p_max_peers"],
        "devp2p_additional_bootstraps": kwargs["devp2p_additional_bootstraps"],
    }
    config["slaves"] = []
    for i in range(slave_count):
        mask = i | slave_count
        config["slaves"].append(
            {
                "id": "S{}".format(i),
                "ip": ip,
                "port": cluster_port_start + i,
                "shard_masks": [mask],
                "db_path_root": db_path_root,
            }
        )

    return config
