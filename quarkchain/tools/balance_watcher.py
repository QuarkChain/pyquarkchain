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


def query_balance(recipient, chain_id, token_str):
    resp = query(
        "getBalances",
        recipient.lower() + chain_id.to_bytes(2, byteorder="big").hex() + "0000",
    )
    for balance in resp.data.result["balances"]:
        if balance["tokenStr"] == token_str:
            return int(balance["balance"], 16)
    return 0


def main():
    global HOST, PORT
    parser = argparse.ArgumentParser()
    parser.add_argument("--recipient", type=str, help="recipient to query")
    parser.add_argument("--host", type=str, default=HOST, help="recipient to query")
    parser.add_argument("--port", type=str, default=PORT, help="recipient to query")
    parser.add_argument(
        "--check_interval", type=int, default=15 * 60, help="recipient to query"
    )
    parser.add_argument("--from_addr", type=str, default="", help="from address")
    parser.add_argument("--to_addr", type=str, default="", help="to address")
    parser.add_argument("--username", type=str, default="", help="email username")
    parser.add_argument("--password", type=str, default="", help="email password")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    prev_balance = None

    while True:
        total_balance = 0
        logger.info("Checking balance")
        for chain_id in range(8):
            balance = query_balance(args.recipient, chain_id, "QKC")
            total_balance += balance
        logger.info(
            "Balance query done, recipient: {0}, total_balance: {1} ({2:,.2f} QKC))".format(
                args.recipient, total_balance, total_balance / (10 ** 18)
            )
        )

        if prev_balance is not None and prev_balance != total_balance:
            msg = "Recipient {3}, balance changed: previous {0:,.2f}, now {1:,.2f}, diff {2:,.2f}".format(
                prev_balance / (10 ** 18),
                total_balance / (10 ** 18),
                (total_balance - prev_balance) / (10 ** 18),
                args.recipient,
            )
            logger.info(msg)
            emsg = EmailMessage()
            emsg["Subject"] = msg
            emsg["From"] = args.from_addr
            emsg["To"] = args.to_addr
            with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465) as s:
                s.login(args.username, args.password)
                s.send_message(emsg)

        prev_balance = total_balance
        time.sleep(args.check_interval)


if __name__ == "__main__":
    main()
