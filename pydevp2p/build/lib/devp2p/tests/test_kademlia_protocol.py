# -*- coding: utf-8 -*-
import random
import time
from devp2p.utils import int_to_big_endian
from devp2p import kademlia
import pytest
import gevent

random.seed(42)


class WireMock(kademlia.WireInterface):

    messages = []  # global messages

    def __init__(self, sender):
        assert isinstance(sender, kademlia.Node)
        self.sender = sender
        assert not self.messages

    @classmethod
    def empty(cls):
        while cls.messages:
            cls.messages.pop()

    def send_ping(self, node):
        echo = hex(random.randint(0, 2**256))[-32:]
        self.messages.append((node, 'ping', self.sender, echo))
        return echo

    def send_pong(self, node, echo):
        self.messages.append((node, 'pong', self.sender, echo))

    def send_find_node(self,  node, nodeid):
        self.messages.append((node, 'find_node', self.sender, nodeid))

    def send_neighbours(self, node, neighbours):
        self.messages.append((node, 'neighbours', self.sender, neighbours))

    def poll(self, node):
        for i, x in enumerate(self.messages):
            if x[0] == node:
                del self.messages[i]
                return x[1:]

    def process(self, kademlia_protocols, steps=0):
        """
        process messages until none are left
        or if process steps messages if steps >0
        """
        i = 0
        proto_by_node = dict((p.this_node, p) for p in kademlia_protocols)
        while self.messages:
            msg = self.messages.pop(0)
            assert isinstance(msg[2], kademlia.Node)
            target = proto_by_node[msg[0]]
            cmd = 'recv_' + msg[1]
            getattr(target, cmd)(*msg[2:])
            i += 1
            if steps and i == steps:
                return  # messages may be left
        assert not self.messages


def random_pubkey():
    pk = int_to_big_endian(random.getrandbits(kademlia.k_pubkey_size))
    return b'\x00' * (kademlia.k_pubkey_size // 8 - len(pk)) + pk


def random_node():
    return kademlia.Node(random_pubkey())


def routing_table(num_nodes=1000):
    node = random_node()
    routing = kademlia.RoutingTable(node)
    for i in range(num_nodes):
        routing.add_node(random_node())
        assert len(routing.buckets) <= i + 2
    assert len(routing.buckets) <= 512
    assert i == num_nodes - 1
    return routing


def get_wired_protocol():
    this_node = random_node()
    return kademlia.KademliaProtocol(this_node, WireMock(this_node))


def test_bootstrap():
    proto = get_wired_protocol()
    wire = proto.wire
    other = routing_table()
    # lookup self
    proto.bootstrap(nodes=[other.this_node])
    msg = wire.poll(other.this_node)
    assert msg == ('find_node', proto.routing.this_node, proto.routing.this_node.id)
    assert wire.poll(other.this_node) is None
    assert wire.messages == []


def test_setup():
    """
    nodes connect to any peer and do a lookup for them selfs
    """

    proto = get_wired_protocol()
    wire = proto.wire
    other = routing_table()

    # lookup self
    proto.bootstrap(nodes=[other.this_node])
    msg = wire.poll(other.this_node)
    assert msg == ('find_node', proto.routing.this_node, proto.routing.this_node.id)
    assert wire.poll(other.this_node) is None
    assert wire.messages == []

    # respond with neighbours
    closest = other.neighbours(msg[2])
    assert len(closest) == kademlia.k_bucket_size
    proto.recv_neighbours(random_node(), closest)

    # expect 3 lookups
    for i in range(kademlia.k_find_concurrency):
        msg = wire.poll(closest[i])
        assert msg == ('find_node', proto.routing.this_node, proto.routing.this_node.id)

    # and pings for all nodes
    for node in closest:
        msg = wire.poll(node)
        assert msg[0] == 'ping'

    # nothing else
    assert wire.messages == []


@pytest.mark.timeout(5)
def test_find_node_timeout():
    proto = get_wired_protocol()
    other = routing_table()
    wire = proto.wire

    # lookup self
    proto.bootstrap(nodes=[other.this_node])
    msg = wire.poll(other.this_node)
    assert msg == ('find_node', proto.routing.this_node, proto.routing.this_node.id)
    assert wire.poll(other.this_node) is None
    assert wire.messages == []

    # do timeout
    gevent.sleep(kademlia.k_request_timeout)

    # respond with neighbours
    closest = other.neighbours(msg[2])
    assert len(closest) == kademlia.k_bucket_size
    proto.recv_neighbours(random_node(), closest)

    # expect pings, but no other lookup
    msg = wire.poll(closest[0])
    assert msg[0] == 'ping'
    assert wire.poll(closest[0]) is None
    wire.empty()
    assert wire.messages == []


def test_eviction():
    proto = get_wired_protocol()
    proto.routing = routing_table(1000)
    wire = proto.wire

    # trigger node ping
    node = proto.routing.neighbours(random_node())[0]
    proto.ping(node)
    msg = wire.poll(node)
    assert msg[0] == 'ping'
    assert wire.messages == []
    proto.recv_pong(node, msg[2])

    # expect no message and that node is still there
    assert wire.messages == []
    assert node in proto.routing

    # expect node to be on the tail
    assert proto.routing.bucket_by_node(node).tail == node


@pytest.mark.timeout(5)
def test_eviction_timeout():
    proto = get_wired_protocol()
    proto.routing = routing_table(1000)
    wire = proto.wire

    # trigger node ping
    node = proto.routing.neighbours(random_node())[0]
    proto.ping(node)
    msg = wire.poll(node)
    assert msg[0] == 'ping'
    assert wire.messages == []

    gevent.sleep(kademlia.k_request_timeout)
    proto.recv_pong(node, msg[2])
    # expect no message and that is not there anymore
    assert wire.messages == []
    assert node not in proto.routing

    # expect node not to be in the replacement_cache
    assert node not in proto.routing.bucket_by_node(node).replacement_cache


@pytest.mark.timeout(15)
def test_eviction_node_active():
    """
    active nodes (replying in time) should not be evicted
    """
    proto = get_wired_protocol()
    proto.routing = routing_table(10000)  # set high, so add won't split
    wire = proto.wire

    # get a full bucket
    full_buckets = [b for b in proto.routing.buckets if b.is_full and not b.should_split]
    assert full_buckets
    bucket = full_buckets[0]
    assert not bucket.should_split
    assert len(bucket) == kademlia.k_bucket_size
    bucket_nodes = bucket.nodes[:]
    eviction_candidate = bucket.head

    # create node to insert
    node = random_node()
    node.id = bucket.start + 1  # should not split
    assert bucket.in_range(node)
    assert bucket == proto.routing.bucket_by_node(node)

    # insert node
    proto.update(node)

    # expect bucket was not split
    assert len(bucket) == kademlia.k_bucket_size

    # expect bucket to be unchanged
    assert bucket_nodes == bucket.nodes
    assert eviction_candidate == bucket.head

    # expect node not to be in bucket yet
    assert node not in bucket
    assert node not in proto.routing

    # expect a ping to bucket.head
    msg = wire.poll(eviction_candidate)
    assert msg[0] == 'ping'
    assert msg[1] == proto.this_node
    assert len(proto._expected_pongs) == 1
    expected_pingid = tuple(proto._expected_pongs.keys())[0]
    assert len(expected_pingid) == 96
    echo = expected_pingid[:32]
    assert len(echo) == 32

    assert wire.messages == []

    # reply in time
    # can not check w/o mcd
    print('sending pong')
    proto.recv_pong(eviction_candidate, echo)

    # expect no other messages
    assert wire.messages == []

    # expect node was not added
    assert node not in proto.routing
    # eviction_candidate is around and was promoted to bucket.tail
    assert eviction_candidate in proto.routing
    assert eviction_candidate == bucket.tail
    # expect node to be in the replacement_cache
    assert node in bucket.replacement_cache


@pytest.mark.timeout(5)
def test_eviction_node_inactive():
    """
    active nodes (replying in time) should not be evicted
    """
    proto = get_wired_protocol()
    proto.routing = routing_table(10000)  # set high, so add won't split
    wire = proto.wire

    # get a full bucket
    full_buckets = [b for b in proto.routing.buckets if b.is_full and not b.should_split]
    assert full_buckets
    bucket = full_buckets[0]
    assert not bucket.should_split
    assert len(bucket) == kademlia.k_bucket_size
    bucket_nodes = bucket.nodes[:]
    eviction_candidate = bucket.head

    # create node to insert
    node = random_node()
    node.id = bucket.start + 1  # should not split
    assert bucket.in_range(node)
    assert bucket == proto.routing.bucket_by_node(node)

    # insert node
    proto.update(node)

    # expect bucket was not split
    assert len(bucket) == kademlia.k_bucket_size

    # expect bucket to be unchanged
    assert bucket_nodes == bucket.nodes
    assert eviction_candidate == bucket.head

    # expect node not to be in bucket yet
    assert node not in bucket
    assert node not in proto.routing

    # expect a ping to bucket.head
    msg = wire.poll(eviction_candidate)
    assert msg[0] == 'ping'
    assert msg[1] == proto.this_node
    assert len(proto._expected_pongs) == 1
    expected_pingid = list(proto._expected_pongs.keys())[0]
    assert len(expected_pingid) == 96
    echo = expected_pingid[:32]
    assert len(echo) == 32
    assert wire.messages == []

    # reply late
    gevent.sleep(kademlia.k_request_timeout)
    proto.recv_pong(eviction_candidate, echo)

    # expect no other messages
    assert wire.messages == []

    # expect node was not added
    assert node in proto.routing
    # eviction_candidate is around and was promoted to bucket.tail
    assert eviction_candidate not in proto.routing
    assert node == bucket.tail
    # expect node to be in the replacement_cache
    assert eviction_candidate not in bucket.replacement_cache


def test_eviction_node_split():
    """
    active nodes (replying in time) should not be evicted
    """
    proto = get_wired_protocol()
    proto.routing = routing_table(1000)  # set lpw, so we'll split
    wire = proto.wire

    # get a full bucket
    full_buckets = [b for b in proto.routing.buckets if b.is_full and b.should_split]
    assert full_buckets
    bucket = full_buckets[0]
    assert bucket.should_split
    assert len(bucket) == kademlia.k_bucket_size
    bucket_nodes = bucket.nodes[:]
    eviction_candidate = bucket.head

    # create node to insert
    node = random_node()
    node.id = bucket.start + 1  # should not split
    assert bucket.in_range(node)
    assert bucket == proto.routing.bucket_by_node(node)

    # insert node
    proto.update(node)

    # expect bucket to be unchanged
    assert bucket_nodes == bucket.nodes
    assert eviction_candidate == bucket.head

    # expect node not to be in bucket yet
    assert node not in bucket
    assert node in proto.routing

    # expect no ping to bucket.head
    assert not wire.poll(eviction_candidate)
    assert wire.messages == []

    # expect node was not added
    assert node in proto.routing

    # eviction_candidate is around and was unchanged
    assert eviction_candidate == bucket.head


def test_ping_adds_sender():
    p = get_wired_protocol()
    assert len(p.routing) == 0
    for i in range(10):
        n = random_node()
        p.recv_ping(n, 'some id %d' % i)
        assert len(p.routing) == i + 1
    p.wire.empty()


def test_two():
    print("")
    one = get_wired_protocol()
    one.routing = routing_table(100)
    two = get_wired_protocol()
    wire = one.wire
    assert one.this_node != two.this_node
    two.ping(one.this_node)
    # print('messages', wire.messages)
    wire.process([one, two])
    two.find_node(two.this_node.id)
    # print('messages', wire.messages)
    msg = wire.process([one, two], steps=2)
    # print('messages', wire.messages)
    assert len(wire.messages) >= kademlia.k_bucket_size
    msg = wire.messages.pop(0)
    assert msg[1] == 'find_node'
    for m in wire.messages[kademlia.k_find_concurrency:]:
        assert m[1] == 'ping'
    wire.empty()


def test_many(num_nodes=17):
    WireMock.empty()
    assert num_nodes >= kademlia.k_bucket_size + 1
    protos = []
    for i in range(num_nodes):
        protos.append(get_wired_protocol())
    bootstrap = protos[0]
    wire = bootstrap.wire

    # bootstrap
    for p in protos[1:]:
        p.bootstrap([bootstrap.this_node])
        wire.process(protos)  # successively add nodes

    # now everbody does a find node to fill the buckets
    for p in protos[1:]:
        p.find_node(p.this_node.id)
        wire.process(protos)  # can all send in parallel

    for i, p in enumerate(protos):
        # print(i, len(p.routing))
        assert len(p.routing) >= kademlia.k_bucket_size

    return protos


def test_find_closest(num_nodes=50):
    """
    assert, that nodes find really the closest of all nodes
    """
    num_tests = 10
    protos = test_many(num_nodes)
    all_nodes = [p.this_node for p in protos]
    for i, p in enumerate(protos[:num_tests]):
        for j, node in enumerate(all_nodes):
            if p.this_node == node:
                continue
            p.find_node(node.id)
            p.wire.process(protos)
            assert p.routing.neighbours(node)[0] == node


if __name__ == '__main__':
    import ethereum.slogging
    ethereum.slogging.configure(config_string=':debug')
    test_many()
