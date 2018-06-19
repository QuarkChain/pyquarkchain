# -*- coding: utf-8 -*-
from devp2p import kademlia
import random
from devp2p.utils import int_to_big_endian
import math
import json

random.seed(42)


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


def test_routing_table():
    routing_table(1000)


def test_node():
    node = random_node()
    l = [node]
    assert node in l
    l.remove(node)
    assert node not in l
    assert not l


def fake_node_from_id(id):
    "warning, pubkey is not the hashed source for the id"
    node = random_node()
    node.id = id
    return node


def test_split():
    node = random_node()
    routing = kademlia.RoutingTable(node)
    assert len(routing.buckets) == 1

    # create very close node
    for i in range(kademlia.k_bucket_size):
        # node = kademlia.Node(int_to_big_endian(node.id + 1))
        node = fake_node_from_id(node.id + 1)
        assert routing.buckets[0].in_range(node)
        routing.add_node(node)
        assert len(routing.buckets) == 1

    assert len(routing.buckets[0]) == kademlia.k_bucket_size

    # node = kademlia.Node(int_to_big_endian(node.id + 1))
    node = fake_node_from_id(node.id + 1)
    assert routing.buckets[0].in_range(node)
    routing.add_node(node)
    assert len(routing.buckets[0]) <= kademlia.k_bucket_size
    assert len(routing.buckets) <= 512


def test_split2():
    routing = routing_table(10000)
    # get a full bucket
    full_buckets = [b for b in routing.buckets if b.is_full]
    split_buckets = [b for b in routing.buckets if b.should_split]

    # not every full bucket needs to be split
    assert len(split_buckets) < len(full_buckets)
    # print('s/f/buckets', len(split_buckets), len(full_buckets))

    assert set(split_buckets).issubset(set(full_buckets))

    assert full_buckets
    bucket = full_buckets[0]
    assert not bucket.should_split
    assert len(bucket) == kademlia.k_bucket_size
    # node = kademlia.Node.from_id(bucket.start + 1)  # should not split
    node = fake_node_from_id(bucket.start + 1)
    assert node not in bucket
    assert bucket.in_range(node)
    assert bucket == routing.bucket_by_node(node)

    r = bucket.add_node(node)
    assert r


def test_non_overlap():
    routing = routing_table(1000)
    # buckets must not overlap
    max_id = 0
    for i, b in enumerate(routing.buckets):
        if i > 0:
            assert b.start > max_id
        assert b.end > max_id
        max_id = b.end
    assert b.end < 2 ** kademlia.k_id_size


def test_full_range():
    # buckets must cover whole range
    def t(routing):
        max_id = 0
        for i, b in enumerate(routing.buckets):
            assert b.start == max_id
            assert b.end > max_id
            max_id = b.end + 1
        assert b.end == 2 ** kademlia.k_id_size - 1
    for num_nodes in (1, 16, 17, 1000):
        t(routing_table(num_nodes))


def test_neighbours():
    routing = routing_table(1000)

    for i in range(100):  # also passed w/ 10k
        node = random_node()
        assert isinstance(node, kademlia.Node)
        nearest_bucket = routing.buckets_by_distance(node)[0]
        if not nearest_bucket.nodes:
            continue
        # change nodeid, to something in this bucket.
        node_a = nearest_bucket.nodes[0]
        node_b = fake_node_from_id(node_a.id + 1)
        assert node_a == routing.neighbours(node_b)[0]
        node_b.id = node_a.id - 1
        assert node_a == routing.neighbours(node_b)[0]


def test_wellformedness():
    """
    fixme: come up with a definition for RLPx
    """
    pass


#  ###################


def show_buckets():
    routing = routing_table(10000)
    for i, b in enumerate(routing.buckets):
        d = b.depth
        print('%sbucket:%d, num nodes:%d depth:%d' % \
            (' ' * d, i, len(b), kademlia.k_id_size - int(math.log(b.start ^ routing.this_node.id, 2))))
    print('routing.node is in bucket', \
        routing.buckets.index(routing.bucket_by_node(routing.this_node)))


def create_json_bucket_test():
    doc = \
        """
    'node_ids': hex encoded ids in order in which they were added to the routing table
    'buckets' : buckets sorted asc by range
    """
    num_nodes = 100
    data = dict(node_ids=[], buckets=[], comment=doc)
    node = random_node()
    routing = kademlia.RoutingTable(node)
    data['local_node_id'] = hex(node.id)
    for r in [random_node() for i in range(num_nodes)]:
        routing.add_node(r)
        data['node_ids'].append(hex(r.id))

    for b in routing.buckets:
        jb = dict(start=hex(b.start), end=hex(b.end), node_ids=[hex(n.id) for n in b.nodes])
        data['buckets'].append(jb)

    return json.dumps(data, indent=2)


if __name__ == '__main__':
    # print(create_json_bucket_test())
    print(show_buckets())
