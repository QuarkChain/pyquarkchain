"""
    following environment variables are required to run this script.
            EMAIL_FROM_ADDRESS=X PASSWORD=Y EMAIL_TO_ADDRESS=Z \
            python bootnode_health_check.py \
            --cluster_config ../../mainnet/singularity/cluster_config_template.json

will update BOOT_NODES fields in P2P section and check the working status of boostrap nodes.
"""
import argparse
import asyncio
import os
import smtplib
import tempfile
import time
from datetime import datetime
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.cluster import Cluster
from quarkchain.jsonrpc_client import JsonRpcClient

TIMEOUT = 10
PRIVATE_ENDPOINT = "http://{}:38491".format("localhost")
PRIVATE_CLIENT = JsonRpcClient(PRIVATE_ENDPOINT, TIMEOUT)


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class HealthCheckCluster(Cluster):
    async def run(self):
        await self.run_master()
        await asyncio.sleep(20)
        check_routing_table(10)
        await self.shutdown()


def check_routing_table(timeout=TIMEOUT):
    result = PRIVATE_CLIENT.call("getKadRoutingTable")
    if len(result) == 0:
        print("Bootstrap node can not provide the routing table for a while!")
        subject = "Boostrap Node Alert!"
        msg = "Bootstrap node can not provide the routing table for a while!" + now()
        send_email(subject, msg)
    print(len(result))


def send_email(subject, msg):
    try:
        server = smtplib.SMTP("smtp.gmail.com:587")
        server.ehlo()
        server.starttls()
        email_from_address = os.environ.get("EMAIL_FROM_ADDRESS")
        password = os.environ.get("PASSWORD")
        email_to_address = os.environ.get("EMAIL_TO_ADDRESS")
        server.login(email_from_address, password)
        message = "Subject: {}\n\n{}".format(subject, msg)
        server.sendmail(email_from_address, email_to_address, message)
        server.quit()
        print("Success: Email sent!")
    except:
        print("Email failed to send.")


def main():
    if "EMAIL_FROM_ADDRESS" not in os.environ:
        raise ValueError("EMAIL_FROM_ADDRESS not found in environment variables")
    if "PASSWORD" not in os.environ:
        raise ValueError("PASSWORD not found in environment variables")
    if "EMAIL_TO_ADDRESS" not in os.environ:
        raise ValueError("EMAIL_TO_ADDRESS not found in environment variables")

    os.chdir(os.path.dirname("../../quarkchain/cluster/"))
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()
    config = ClusterConfig.create_from_args(args)

    # creat a config tempfile for health check, which is a copy of the original config file
    tempfile.tempdir = "../../mainnet/singularity/"
    fd, config.json_filepath = tempfile.mkstemp()
    with os.fdopen(fd, "w") as tmp:
        tmp.write(config.to_json())
    print("Cluster config file: {}".format(config.json_filepath))
    print(config.to_json())

    cluster = HealthCheckCluster(config)
    bootstrap_nodes = config.P2P.BOOT_NODES.split(",")
    count = 0

    while True:
        bash_command_revised_config = (
            "QKC_CONFIG="
            + config.json_filepath
            + " python3 ../../quarkchain/tools/config_p2p.py --bootnodes "
            + bootstrap_nodes[count]
        )
        os.system(bash_command_revised_config)
        print("Start Bootstrap With " + bootstrap_nodes[count])
        cluster.start_and_loop()
        time.sleep(100)
        count = (count + 1) % len(bootstrap_nodes)


if __name__ == "__main__":
    main()
