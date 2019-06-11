"""
    python check_health_bootstrap.py \
            --cluster_config ../../mainnet/singularity/cluster_config_template.json

will update BOOT_NODES fields in P2P section and check the working status of boostrap nodes.
"""
import argparse
import asyncio
import logging
import time
from datetime import datetime
import jsonrpcclient
import psutil
import numpy
from decimal import Decimal
import smtplib
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.master import MasterServer
from quarkchain.cluster.cluster import Cluster
import jsonrpcclient
import logging
import time
from datetime import datetime
import smtplib
import os

TIMEOUT = 10
PRIVATE_ENDPOINT = "http://{}:38491".format("localhost")
PRIVATE_CLIENT = jsonrpcclient.HTTPClient(PRIVATE_ENDPOINT)

# Please export your email information first.
# export EMAIL_FROM_ADDRESS=""
# export PASSWORD=""
# export EMAIL_TO_ADDRESS=""


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Health_Check_Cluster(Cluster):
    async def run(self):
        await self.run_master()
        # p2p discovery mode will disable slaves
        # if not self.config.P2P.DISCOVERY_ONLY:
        #    await self.run_slaves()

        await asyncio.sleep(20)
        checkRoutingTable(10)
        await self.shutdown()


def checkRoutingTable(timeout=TIMEOUT):
    result = PRIVATE_CLIENT.send(
        jsonrpcclient.Request("getKadRoutingTable"), timeout=timeout
    )
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
        if "EMAIL_FROM_ADDRESS" in os.environ:
            EMAIL_FROM_ADDRESS = os.environ.get("EMAIL_FROM_ADDRESS")
        if "PASSWORD" in os.environ:
            PASSWORD = os.environ.get("PASSWORD")
        if "EMAIL_TO_ADDRESS" in os.environ:
            EMAIL_TO_ADDRESS = os.environ.get("EMAIL_TO_ADDRESS")
        server.login(EMAIL_FROM_ADDRESS, PASSWORD)
        message = "Subject: {}\n\n{}".format(subject, msg)
        server.sendmail(EMAIL_FROM_ADDRESS, EMAIL_TO_ADDRESS, message)
        server.quit()
        print("Success: Email sent!")
    except:
        print("Email failed to send.")


def main():
    # os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(os.path.dirname("../../quarkchain/cluster/"))
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()

    config = ClusterConfig.create_from_args(args)
    print("Cluster config file: {}".format(config.json_filepath))
    print(config.to_json())

    cluster = Health_Check_Cluster(config)
    bootnodes = []
    # GCP Bootstrap
    bootnodes.append(
        "enode://48e1af232c290add043118edca45608589ef305f19a2d6d8a6126677ba573c1d5984c15962e8593b32e6ae2f1a89237f9691178e527cba0b07818ad5b01a13dc@52.34.48.64:38291"
    )
    bootnodes.append(
        "enode://69a887846c4f6540958c20d654b191b08c39e5624b93d8e94ec8e37da2ae7c0572c0741775e34cd17affa7e68532910e152e16361d22198f04aae2cceb105a03@13.124.15.123:38291"
    )
    # AWS Bootstrap
    bootnodes.append(
        "enode://438d9a2349037e231ae7975f646a32c5b3d2032190a067762b35b8a039568fbb81981e4e2e43f1923a113834a4675919ed27fad68ca48203b4001fee049a9276@35.243.210.122:38291"
    )
    bootnodes.append(
        "enode://c093dee29400c0d114c3af600df80a7ad285e8b430f6768749600d55726d4b1562f624526c596b968e4520eaeadaa0d8be98940da065ac710a76d8fac58d5c00@35.246.213.180:38291"
    )
    # DO  Bootstrap
    bootnodes.append(
        "enode://5f81aac576814cac04701d418d9f127903cf75ed26c0433b9b1b35774efe7e2630e377baae156023d2448055e32678a472f6a25f5ead8a690f8cec1e7c9176e6@68.183.247.182:38291"
    )
    bootnodes.append(
        "enode://80a7f0960732dae69fa470cf950be636352166b9f016605b79bae4286f06a3fb0361f1293d6ea43df9b87f6e6397498e82c23d913464e2724e5c3adcce796e0d@165.227.240.113:38291"
    )
    count = 0

    while True:
        bashCommandRevisedConfig = (
            "python3 ../../quarkchain/tools/config_p2p.py --bootnodes "
            + bootnodes[(count % 6)]
        )
        os.system(bashCommandRevisedConfig)
        print("Start Bootstrap With" + bootnodes[(count % 6)])
        cluster.start_and_loop()
        time.sleep(100)
        count += 1
        count = count % 10000


if __name__ == "__main__":
    main()
