import asyncio
import json
from typing import List, Dict, Tuple, Optional

from jsonrpcserver.exceptions import InvalidParams
from websockets import WebSocketServerProtocol

from quarkchain.cluster.log_filter import LogFilter as EvmLogFilter
from quarkchain.core import MinorBlockHeader

SUB_NEW_HEADS = "newHeads"
SUB_NEW_PENDING_TX = "newPendingTransactions"
SUB_LOGS = "logs"
SUB_SYNC = "syncing"


class SubscriptionManager:
    def __init__(self):
        self.subscribers = {
            SUB_NEW_HEADS: {},
            SUB_NEW_PENDING_TX: {},
            SUB_LOGS: {},
            SUB_SYNC: {},
        }  # type: Dict[str, Dict[str, WebSocketServerProtocol]]
        self.log_filters = {}  # type: Dict[str, EvmLogFilter]

    def add_subscriber(self, sub_type, sub_id, conn, extra=None):
        if sub_type not in self.subscribers:
            raise InvalidParams("Invalid subscription")
        self.subscribers[sub_type][sub_id] = conn
        if sub_type == SUB_LOGS:
            assert extra and isinstance(extra, EvmLogFilter)
            self.log_filters[sub_id] = extra

    def remove_subscriber(self, sub_id):
        for sub_type, subscriber_dict in self.subscribers.items():
            if sub_id in subscriber_dict:
                del subscriber_dict[sub_id]
                if sub_type == SUB_LOGS:
                    del self.log_filters[sub_id]
                return
        raise InvalidParams("subscription not found")

    async def notify_new_heads(self, header: MinorBlockHeader):
        # TODO
        data = None
        await self.__notify(SUB_NEW_HEADS, data)

    async def notify_new_pending_tx(self, tx_hashes: List[bytes]):
        tasks = []
        for sub_id, websocket in self.subscribers[SUB_NEW_PENDING_TX].items():
            for tx_hash in tx_hashes:
                tx_hash = "0x" + tx_hash.hex()
                response = self.response_encoder(sub_id, tx_hash)
                tasks.append(websocket.send(json.dumps(response)))
        await asyncio.gather(*tasks)

    async def notify_log(self, height: int):
        # TODO
        data = None
        await self.__notify(SUB_LOGS, data)

    async def notify_sync(self, data: Optional[Tuple[int, ...]] = None):
        await self.__notify(SUB_SYNC, data)

    async def __notify(self, sub_type, data):
        assert sub_type in self.subscribers
        encoder = (
            self.sync_status_encoder if sub_type == SUB_SYNC else self.response_encoder
        )
        for sub_id, websocket in self.subscribers[sub_type].items():
            response = encoder(sub_id, data)
            asyncio.ensure_future(websocket.send(json.dumps(response)))

    @staticmethod
    def response_encoder(sub_id, result):
        return {
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {"subscription": sub_id, "result": result},
        }

    @staticmethod
    def sync_status_encoder(sub_id, data):
        ret = {
            "jsonrpc": "2.0",
            "subscription": sub_id,
            "result": {"syncing": bool(data)},
        }
        if data:
            tip_height, highest_block = data
            ret["result"]["status"] = {
                "currentBlock": tip_height,
                "highestBlock": highest_block,
            }
        return ret
