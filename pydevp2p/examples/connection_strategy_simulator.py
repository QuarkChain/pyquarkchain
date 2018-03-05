"""
installation:

    git clone git@github.com:heikoheiko/pydevp2p.git
    git co sha3id
    python setup.py develop
    pip install statistics networkx matplotlib

usage:
    cd devp2p/tests/
    python test_connections_stratgy.py <num_nodes>

    you probably want to adjust  `min_peer_options` and `klasses`
    in the `main` function (end of file)

implementing strategies:
    inherit form CNodeBase and implement .setup_targets()
    add your class to `klasses` in the `main` function.
"""

import time
import operator
import devp2p.kademlia
from devp2p.tests.test_kademlia_protocol import test_many, get_wired_protocol
from collections import OrderedDict
import random
import statistics
import networkx as nx
from functools import total_ordering

random.seed(42)

@total_ordering
class CNodeBase(object):

    """
    to implement your conenction strategy override
        .select_targets (must)
        .connect_peers
    """
    k_max_node_id = devp2p.kademlia.k_max_node_id

    def __init__(self, proto, network, min_peers=5, max_peers=10):
        self.proto = proto
        self.network = network
        self.min_peers = min_peers
        self.max_peers = max_peers
        self.connections = []
        self.id = proto.this_node.id
        # list of dict(address=long, tolerance=long, connected=(None or Node))
        # address is the id : long
        self.targets = list()

    @classmethod
    def prepare_dht(cls, num_nodes):
        return test_many(num_nodes)

    def distance(self, other):
        return self.id ^ other.id

    def outbound(self):
        "peers this node connected to"
        return [n['connected'] for n in self.targets if n['connected']]

    def inbound(self):
        "peers that connected this node"
        ob = self.outbound()
        return [n for n in self.connections if n not in ob]

    def receive_connect(self, other):
        if len(self.connections) >= self.max_peers:
            return False
        else:
            assert other not in self.connections
            self.connections.append(other)
            assert len(self.connections) <= self.max_peers
            return True

    def receive_disconnect(self, other):
        assert other in self.connections
        self.connections.remove(other)
        for t in self.targets:
            if t['connected'] == other:
                t['connected'] = None

    def _find_targets(self):
        "call find node to fill buckets with addresses close to the target"
        for t in self.targets:
            self.proto.find_node(t['address'])
            self.network.process()

    def find_targets(self):
        "call find node to fill buckets with addresses close to the target"
        all = [n.proto for n in self.network.values()]
        for t in self.targets:
            self.proto.find_node(t['address'])
            self.network.process()
            print('find_targets:{}'.format(t))

    def connect_peers(self, max_connects=0, random_within_distance=False, candidates=[]):
        """
        random_within_distance: collect random node within distance form local buckets
        candidates: directly specify a list of candidates

        override to deal with situations where
            - you enter the method and have not enough slots to conenct your targets
            - your targets don't want to connect you
            - targets are not within the tolerace
            ...
        """
        assert self.targets
        num_connected = 0
        print('not connected targets:{}'.format(t for t in self.targets if not t['connected']))
        # connect closest node to target id
        for t in (t for t in self.targets if not t['connected']):
            if len(self.connections) >= self.max_peers:
                break
            #if len(candidates) != 0:
            #    print(candidates)
            #    assert not random_within_distance
            elif random_within_distance:
                candidates = self.proto.routing.neighbours_within_distance(
                    t['address'], t['tolerance'])
                random.shuffle(candidates)
            else:
                candidates = self.proto.routing.neighbours(t['address'])
            print('for node {} candidates {}'.format(t, candidates))
            for knode in candidates:
                assert isinstance(knode, devp2p.kademlia.Node)
                assert len(self.connections) < self.max_peers
                # assure within tolerance
                if knode.id_distance(t['address']) < t['tolerance']:
                    # make sure we are not connected yet
                    remote = self.network[knode.id]
                    if remote not in self.connections and remote != self:
                        if remote.receive_connect(self):
                            assert len(self.connections) < self.max_peers
                            t['connected'] = remote
                            self.connections.append(remote)
                            assert len(self.connections) <= self.max_peers
                            num_connected += 1
                            if max_connects and num_connected == max_connects:
                                return num_connected
                            break
        return num_connected

    def setup_targets(self):
        """
        calculate select target distances, addresses and tolerances
        """
        for i in range(self.min_peers):
            self.targets.append(dict(address=0, tolerance=0, connected=None))
            # NOT IMPLEMENTED HERE

    def __lt__(self, other):
        return self.id < other.id

class CNodeRandom(CNodeBase):

    def setup_targets(self):
        """
        connects random nodes
        """
        for i in range(self.min_peers):
            distance = random.randint(0, self.k_max_node_id)
            address = (self.id + distance) % (self.k_max_node_id + 1)
            tolerance = self.k_max_node_id / self.max_peers
            self.targets.append(dict(address=address, tolerance=tolerance, connected=None))


class CNodeRandomFast(CNodeRandom):

    @classmethod
    def prepare_dht(cls, num_nodes):
        return [get_wired_protocol() for i in range(num_nodes)]

    def find_targets(self):
        self.candidates = []
        assert len(self.network.all_nodes)
        tolerance = devp2p.kademlia.k_max_node_id / len(self.network.all_nodes) * 100
        for t in self.targets:
            nodes = [n for n in self.network.all_nodes if n.id_distance(t['address']) < tolerance]
            assert nodes
            c = sorted(nodes, key=operator.methodcaller('id_distance', t['address']))[:4]
            self.candidates.extend(c)

    def connect_peers(self, max_connects, random_within_distance=False, candidates=[]):
        return CNodeBase.connect_peers(self, max_connects, random_within_distance, self.candidates)


class CNodeFelixNoLimits(CNodeBase):
    # https://github.com/ethereum/go-ethereum/wiki/RLPx-Peer-Selection-Proposal

    """
    no id selection limits on incomming connections
    """

    def connect_peers(self, max_connects=0):
        return CNodeBase.connect_peers(self, max_connects=max_connects, random_within_distance=True)

    def setup_targets(self):
        """
        The 512 bit (256 bit) space is divided into N ranges.
        The dialer maintains N slots, one for each range.

        In order to find peers, it performs a node lookup with a random target
        in reach range corresponding to an empty slot.
        The results of the lookup are then dialed.

        If dialing does not succeed for any node, another random lookup is
        performed in that range.

        """

        slot_width = self.k_max_node_id / self.min_peers
        for i in range(self.min_peers):
            distance = int((i + 0.5) * slot_width)
            tolerance = slot_width / 2
            address = (self.id + distance) % (self.k_max_node_id + 1)
            assert isinstance(address, int), address
            self.targets.append(dict(address=address, tolerance=tolerance, connected=None))


class CNodeRandomSelfLookup(CNodeBase):

    """
    As proposed by Alex:
        node joining the network
        doing a self lookup
        selects random peers from the returned peers

    note: all repsonding nodes probably know the complete network,
          but this is sensible, assuming they are way longer in the network

    limits: if a node does not accept the connection, there is no backup (rarely)
    """

    def setup_targets(self):
        bootstrap_node = random.choice(list(self.proto.routing))
        # reset routing table
        self.proto.routing = devp2p.kademlia.RoutingTable(self.proto.this_node)
        self.proto.bootstrap([bootstrap_node])
        self.network.process()

        selected = set()
        for i in range(self.min_peers):
            while True:
                target = random.choice(list(self.proto.routing))
                if target not in selected:
                    selected.add(target)
                    break
            self.targets.append(dict(address=target.id, tolerance=1, connected=None))


class CNodeSelfOtherSide(CNodeBase):

    """
    Daniel:
    Thus, the self-lookup finds (enough) nodes that are closer to self than the bootstrap node,
    while the "other side" lookup makes sure that it has neighbors that are sufficiently far.
    These two lookups are both necessary and sufficient to make sure that the node becomes
    "well connected", i.e. connected to enough nodes at every distance level.

    Nodes must have enough neighbors both near and far in order to have short (i.e. O(log N)
     length) path to every node in the network with a centrality roughly equaling that of
    all other nodes.
    """

    def setup_targets(self):
        bootstrap_node = random.choice(list(self.proto.routing))
        # reset routing table
        self.proto.routing = devp2p.kademlia.RoutingTable(self.proto.this_node)
        # lookup self
        self.proto.bootstrap([bootstrap_node])
        self.network.process()
        # lookup not self
        other_side_id = self.id ^ 0
        self.proto.find_node(other_side_id)
        self.network.process()

        selected = set()
        for i in range(self.min_peers):
            while True:
                target = random.choice(list(self.proto.routing))
                if target not in selected:
                    selected.add(target)
                    break
            self.targets.append(dict(address=target.id, tolerance=1, connected=None))


class CNodeKademlia(CNodeBase):

    def setup_targets(self):
        """
        connects nodes according to a dht routing
        """
        distance = self.k_max_node_id
        for i in range(self.min_peers):
            distance //= 2
            address = (self.id + distance) % (self.k_max_node_id + 1)
            tolerance = distance // 2
            #print('address {}'.format(address))
            assert isinstance(address, int), address
            self.targets.append(dict(address=address, tolerance=tolerance, connected=None))


class CNodeKademliaRandom(CNodeKademlia):

    def connect_peers(self, max_connects=0):
        return CNodeBase.connect_peers(self, max_connects=max_connects, random_within_distance=True)


def analyze(network):
    import networkx as nx

    G = nx.Graph()

    def weight(a, b):
        """
        Weight of edge. In dot,
        the heavier the weight, the shorter, straighter and more vertical the edge is.
        For other layouts, a larger weight encourages the layout to make the edge length
        closer to that specified by the len attribute.
        """
        # same node is weight == 1
        return (1 - a.distance(b) / devp2p.kademlia.k_max_node_id) * 10

    for node in network.values():
        for r in node.connections:
            G.add_edge(node, r, weight=weight(node, r))

    num_peers = [len(n.connections) for n in network.values()]
    metrics = OrderedDict(num_nodes=len(network))
    metrics['max_peers'] = max(num_peers)
    metrics['min_peers'] = min(num_peers)
    metrics['avg_peers'] = statistics.mean(num_peers)
    metrics['rsd_peers'] = statistics.stdev(num_peers) / statistics.mean(num_peers)

    # calc shortests paths
    # lower is better
    if nx.is_connected(G):
        print('calculating avg_shortest_path')
        avg_shortest_paths = []
        for node in G:
            path_length = nx.single_source_shortest_path_length(G, node)
            avg_shortest_paths.append(statistics.mean(path_length.values()))
        metrics['avg_shortest_path'] = statistics.mean(avg_shortest_paths)
        metrics['rsd_shortest_path'] = statistics.stdev(
            avg_shortest_paths) / metrics['avg_shortest_path']

    try:
        # Closeness centrality at a node is 1/average distance to all other nodes.
        # higher is better
        print('calculating closeness centrality')
        vs = nx.closeness_centrality(G).values()
        metrics['min_closeness_centrality'] = min(vs)
        metrics['avg_closeness_centrality'] = statistics.mean(vs)
        metrics['rsd_closeness_centrality'] = statistics.stdev(
            vs) / metrics['avg_closeness_centrality']

        # The load centrality of a node is the fraction of all shortest paths that
        # pass through that node
        # Daniel:
        # I recommend calculating (or estimating) the centrality of each node and making sure that
        # there are no nodes with much higher centrality than the average.
        # lower is better
        print('calculating load centrality')
        vs = nx.load_centrality(G).values()
        metrics['max_load_centrality'] = max(vs)
        metrics['avg_load_centrality'] = statistics.mean(vs)
        #metrics['rsd_load_centrality'] = statistics.stdev(vs) / metrics['avg_load_centrality']

        print('calculating node_connectivity')
        # higher is better
        metrics['node_connectivity'] = nx.node_connectivity(G)

        print('calculating diameter')
        # lower is better
        metrics['diameter '] = nx.diameter(G)

    except nx.exception.NetworkXError as e:
        metrics['ERROR'] = -1
    return metrics


def draw(G, metrics=dict()):
    import matplotlib.pyplot as plt
    """
    dot - "hierarchical" or layered drawings of directed graphs. This is the default tool to use if edges have directionality.

    neato - "spring model'' layouts. This is the default tool to use if the graph is not too large (about 100 nodes) and you don't know anything else about it. Neato attempts to minimize a global energy function, which is equivalent to statistical multi-dimensional scaling.

    fdp - "spring model'' layouts similar to those of neato, but does this by reducing forces rather than working with energy.

    sfdp - multiscale version of fdp for the layout of large graphs.

    twopi - radial layouts, after Graham Wills 97. Nodes are placed on concentric circles depending their distance from a given root node.

    circo - circular layout, after Six and Tollis 99, Kauffman and Wiese 02. This is suitable for certain diagrams of multiple cyclic structures, such as certain telecommunications networks.

    """
    print('layouting')

    text = ''
    for k, v in metrics.items():
        text += '%s: %.4f\n' % (k.ljust(max(len(x) for x in metrics.keys())), v)

    print(text)
    #pos = nx.graphviz_layout(G, prog='dot', args='')
    pos = nx.spring_layout(G)
    plt.figure(figsize=(8, 8))
    nx.draw(G, pos, node_size=20, alpha=0.5, node_color="blue", with_labels=False)
    plt.text(0.02, 0.02, text, transform=plt.gca().transAxes)  # , font_family='monospace')
    plt.axis('equal')
    outfile = 'network_graph.png'
    plt.savefig(outfile)
    print('saved visualization to {}'.format(outfile))
    plt.ion()
    plt.show()
    while True:
        time.sleep(0.1)


def simulate(node_class, set_num_nodes=20, set_min_peers=7, set_max_peers=14):
    print('running simulation {} {}'.format(node_class.__name__, \
        dict(num_nodes=set_num_nodes, min_peers=set_min_peers, max_peers=set_max_peers)))

    print('bootstrapping discovery protocols')
    kademlia_protocols = node_class.prepare_dht(set_num_nodes)

    # create ConnectableNode instances
    print('executing connection strategy')
    network = OrderedDict()  # node.id -> Node
    # .process executes all messages on the network
    network.process = lambda: kademlia_protocols[0].wire.process(kademlia_protocols)
    network.all_nodes = [p.this_node for p in kademlia_protocols]
    # wrap protos in connectable nodes and map via network
    for p in kademlia_protocols:
        cn = node_class(p, network, min_peers=set_min_peers, max_peers=set_max_peers)
        network[cn.id] = cn

    print('setup targets')
    # setup targets
    for cn in network.values():
        cn.setup_targets()
        print('setup node:{}'.format(cn))

    print('lookup targets')
    # lookup targets
    for cn in network.values():
        cn.find_targets()
        print('find_targets:{}'.format(cn))

    print('connect peers')
    # connect peers (one client per round may connect)
    while True:
        n_connects = 0
        for cn in network.values():
            n_connects += cn.connect_peers(max_connects=1)
        if n_connects == 0:
            break

    metrics = analyze(network)
    return metrics


def print_results(results=[]):
    labels = results[0].keys()
    print('\t'.join(labels))
    f = lambda x: str(x) if isinstance(x, (int, str)) else ('%.4f' % x)  # .replace('.', ',')
    for r in results:
        print('\t'.join(f(r.get(k, 'n/a')) for k in labels))


def main(num_nodes):
    # strategies to test
    klasses = [CNodeRandom,
               CNodeSelfOtherSide,
               CNodeKademliaRandom,
               CNodeRandomSelfLookup,
               CNodeFelixNoLimits]
    klasses = [CNodeKademliaRandom]

    # min_peer settings to test
    min_peer_options = (6,)

    print('running {} simulations'.format((len(min_peer_options) * len(klasses))))

    results = []
    for min_peers in min_peer_options:
        max_peers = min_peers * 2
        for node_class in klasses:
            p = OrderedDict(node_class=node_class)
            p.update(OrderedDict(set_num_nodes=num_nodes, set_min_peers=min_peers,
                                 set_max_peers=max_peers))
            metrics = simulate(**p)
            p.update(metrics)
            p['node_class'] = p['node_class'].__name__
            print(p)
            results.append(p)

    print_results(results)

if __name__ == '__main__':
    # import pyethereum.slogging
    # pyethereum.slogging.configure(config_string=':debug')
    import sys
    if not len(sys.argv) == 2:
        print('usage:{} <num_nodes>'.format(sys.argv[0]))
        sys.exit(1)
    num_nodes = int(sys.argv[1])
    main(num_nodes)

"""
todos:
    colorize nodes being closest to 0, 1/4, 1/2, 3/4 of the id space
    validate graph (assert bidirectional connections, max_peers satisfactions)
    support ananlytics about nodes added to an established network
"""
