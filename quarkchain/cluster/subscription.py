import asyncio
import json
from typing import List, Dict

from jsonrpcserver.exceptions import InvalidParams
from websockets import WebSocketServerProtocol

from quarkchain.cluster.filter import Filter as EvmLogFilter
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
        from quarkchain.cluster.jsonrpc import minor_block_header_encoder

        data = minor_block_header_encoder(header)
        await self.__notify(SUB_NEW_HEADS, data)

    async def notify_new_pending_tx(self, tx_hash: bytes):
        await self.__notify(SUB_NEW_PENDING_TX, "0x" + tx_hash.hex())

    async def notify_log(self, height: int):
        from quarkchain.cluster.jsonrpc import loglist_encoder

        for sub_id, websocket in self.subscribers[SUB_LOGS].items():
            log_filter = self.log_filters[sub_id]
            log_filter.start_block = height
            log_filter.end_block = height
            logs = log_filter.run()
            log_list = loglist_encoder(logs)
            tasks = []
            for log in log_list:
                response = self.response_encoder(sub_id, log)
                tasks.append(websocket.send(json.dumps(response)))
            await asyncio.gather(*tasks)

    async def notify_sync(
        self, running: bool, tip: MinorBlockHeader, queue: List[MinorBlockHeader]
    ):
        # TODO
        data = None
        await self.__notify(SUB_SYNC, data)

    async def __notify(self, sub_type, data):
        assert sub_type in self.subscribers
        tasks = []
        for sub_id, websocket in self.subscribers[sub_type].items():
            response = self.response_encoder(sub_id, data)
            tasks.append(websocket.send(json.dumps(response)))
        await asyncio.gather(*tasks)

    @staticmethod
    def response_encoder(sub_id, result):
        return {
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {"subscription": sub_id, "result": result},
        }
