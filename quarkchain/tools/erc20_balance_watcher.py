# jsonrpcclient==2.5.2
# requests==2.20.0
import jsonrpcclient
import time
import logging
import argparse
import smtplib
from email.message import EmailMessage


HOST = "https://eth.llamarpc.com"
PORT = "443"

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


def query_balance(recipient):
    resp = query(
        "eth_call",
        [{"from": None, "to": "0xea26c4ac16d4a5a106820bc8aee85fd0b7b2b664", "data": "0x70a08231"+int(recipient, 0).to_bytes(32, byteorder="big").hex()}, "latest"]
    )
    return int(resp, 0)


def main():
    global HOST, PORT
    parser = argparse.ArgumentParser()
    parser.add_argument("--recipient", type=str, help="recipient to query")
    parser.add_argument("--host", type=str, default=HOST, help="recipient to query")
    parser.add_argument("--port", type=str, default=PORT, help="recipient to query")
    parser.add_argument(
        "--check_interval", type=int, default=15 * 60, help="recipient to query"
    )
    parser.add_argument(
        "--force_interval", type=int, default=None, help="interval to forcibly send update"
    )
    parser.add_argument("--from_addr", type=str, default="", help="from address")
    parser.add_argument("--to_addr", type=str, default="", help="to address")
    parser.add_argument("--username", type=str, default="", help="email username")
    parser.add_argument("--password", type=str, default="", help="email password")
    parser.add_argument("--test_email", type=bool, default=False, help="send a test email when start")


    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    prev_balance = None
    prev_send = time.monotonic()

    if args.test_email:
        msg = "test email for {}".format(args.recipient)
        emsg = EmailMessage()
        emsg["Subject"] = msg
        emsg["From"] = args.from_addr
        emsg["To"] = args.to_addr
        logger.info(msg)
        with smtplib.SMTP(host="smtp.gmail.com", port=587) as s:
            s.ehlo()
            s.starttls()
            s.login(args.username, args.password)
            s.send_message(emsg)

    while True:
        logger.info("Checking balance")
        balance = query_balance(args.recipient)
        logger.info(
            "Balance query done, recipient: {0}, balance: {1} ({2:,.2f} QKC))".format(
                args.recipient, balance, balance / (10 ** 18)
            )
        )

        if (prev_balance is not None and prev_balance != balance) or (args.force_interval is not None and time.monotonic() - prev_send >= args.force_interval):
            msg = "Recipient {3}, balance changed: previous {0:,.2f}, now {1:,.2f}, diff {2:,.2f}".format(
                prev_balance / (10 ** 18),
                balance / (10 ** 18),
                (balance - prev_balance) / (10 ** 18),
                args.recipient,
            )
            logger.info(msg)
            emsg = EmailMessage()
            emsg["Subject"] = msg
            emsg["From"] = args.from_addr
            emsg["To"] = args.to_addr
            with smtplib.SMTP(host="smtp.gmail.com", port=587) as s:
                s.ehlo()
                s.starttls()
                s.login(args.username, args.password)
                s.send_message(emsg)
            prev_send = time.monotonic()

        prev_balance = balance
        time.sleep(args.check_interval)


if __name__ == "__main__":
    main()
