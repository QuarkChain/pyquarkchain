import jsonrpcclient
import time
import logging
import argparse
import smtplib
from email.message import EmailMessage


HOST = "http://jrpc.mainnet.quarkchain.io"
PORT = "38391"

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT)
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def query(endpoint, *args):
    retry, resp = 0, None
    while retry <= 5:
        try:
            resp = jsonrpcclient.request(HOST + ":" + PORT, endpoint, *args)
            break
        except Exception:
            retry += 1
            time.sleep(0.5)
    return resp


def query_tip():
    resp = query("getRootBlockByHeight", None)
    return int(resp.data.result["height"], 16), resp.data.result["hash"]


def query_rblock_with_height(height):
    if isinstance(height, int):
        height = hex(height)
    resp = query("getRootBlockByHeight", height)
    return int(resp.data.result["height"], 16), resp.data.result["hash"]


def main():
    global HOST, PORT
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="recipient to query")
    parser.add_argument("--port", type=str, default=PORT, help="recipient to query")
    parser.add_argument(
        "--check_interval", type=int, default=15 * 60, help="recipient to query"
    )
    parser.add_argument(
        "--rblock_height_diff",
        type=int,
        default=5,
        help="height to check = current tip height - diff",
    )
    parser.add_argument("--from_addr", type=str, default="", help="from address")
    parser.add_argument("--to_addr", type=str, default="", help="to address")
    parser.add_argument("--username", type=str, default="", help="email username")
    parser.add_argument("--password", type=str, default="", help="email password")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    prev_block_height = None
    prev_block_hash = None

    while True:
        if prev_block_height is not None:
            logger.info("Checking height {0}".format(prev_block_height))
            _, block_hash = query_rblock_with_height(prev_block_height)
            if block_hash == prev_block_hash:
                logger.info("Hash checked OK")
            else:
                msg = "Reorg detect: height {0}, previous hash {1}, now hash {2}".format(
                    prev_block_height, prev_block_hash, block_hash
                )
                logger.info(msg)
                emsg = EmailMessage()
                emsg["Subject"] = msg
                emsg["From"] = args.from_addr
                emsg["To"] = args.to_addr
                with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465) as s:
                    s.login(args.username, args.password)
                    s.send_message(emsg)

        logger.info("Checking tip height")
        tip_block_height, tip_block_hash = query_tip()
        logger.info("Tip height {0}".format(tip_block_height))

        logger.info(
            "Checking rblock with height {0}".format(
                tip_block_height - args.rblock_height_diff
            )
        )
        prev_block_height, prev_block_hash = query_rblock_with_height(
            tip_block_height - args.rblock_height_diff
        )
        logger.info(
            "Block to check: height {0}, hash {1}".format(
                prev_block_height, prev_block_hash
            )
        )

        time.sleep(args.check_interval)


if __name__ == "__main__":
    main()
