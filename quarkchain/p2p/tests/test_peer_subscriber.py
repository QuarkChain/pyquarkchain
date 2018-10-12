import asyncio
import logging

import pytest

from quarkchain.utils import Logger

from quarkchain.p2p.peer import PeerSubscriber
from quarkchain.p2p.protocol import Command

from quarkchain.p2p.tools.paragon import GetSum
from quarkchain.p2p.tools.paragon.helpers import get_directly_linked_peers


class GetSumSubscriber(PeerSubscriber):
    logger = Logger
    msg_queue_maxsize = 10
    subscription_msg_types = {GetSum}


class AllSubscriber(PeerSubscriber):
    logger = Logger
    msg_queue_maxsize = 10
    subscription_msg_types = {Command}


@pytest.mark.asyncio
async def test_peer_subscriber_filters_messages(request, event_loop):
    peer, remote = await get_directly_linked_peers(request, event_loop)

    get_sum_subscriber = GetSumSubscriber()
    all_subscriber = AllSubscriber()

    peer.add_subscriber(get_sum_subscriber)
    peer.add_subscriber(all_subscriber)

    remote.sub_proto.send_broadcast_data(b"value-a")
    remote.sub_proto.send_broadcast_data(b"value-b")
    remote.sub_proto.send_get_sum(7, 8)
    remote.sub_proto.send_get_sum(1234, 4321)
    remote.sub_proto.send_broadcast_data(b"value-b")

    # yeild to let remote and peer transmit.
    await asyncio.sleep(0.2)

    assert get_sum_subscriber.queue_size == 2
    assert all_subscriber.queue_size == 5
