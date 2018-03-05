from devp2p import discovery
from devp2p import kademlia
from devp2p import crypto
from devp2p.app import BaseApp
from rlp.utils import decode_hex, encode_hex
from devp2p.utils import remove_chars
import pytest
import gevent
import random

random.seed(42)

###############################


def test_address():
    Address = discovery.Address

    ipv4 = '127.98.19.21'
    ipv6 = u'5aef:2b::8'
    # hostname = 'localhost'
    port = 1

    a4 = Address(ipv4, port)
    aa4 = Address(ipv4, port)
    assert a4 == aa4
    a6 = Address(ipv6, port)
    aa6 = Address(ipv6, port)
    assert a6 == aa6

    b_a4 = a4.to_binary()
    assert a4 == Address.from_binary(*b_a4)

    b_a6 = a6.to_binary()
    assert len(b_a6) == 3
    assert a6 == Address.from_binary(*b_a6)

    e_a4 = a4.to_endpoint()
    assert a4 == Address.from_endpoint(*e_a4)

    e_a6 = a6.to_endpoint()
    assert a6 == Address.from_endpoint(*e_a6)

    assert len(b_a6[0]) == 16
    assert len(b_a4[0]) == 4
    assert isinstance(b_a6[1], bytes)

    # temporarily disabled hostname test, see commit discussion:
    # https://github.com/ethereum/pydevp2p/commit/8e1f2b2ef28ecba22bf27eac346bfa7007eaf0fe
    # host_a = Address(hostname, port)
    # assert host_a.ip in ("127.0.0.1", "::1")

#############################


class AppMock(object):
    pass


class NodeDiscoveryMock(object):

    messages = []  # [(to_address, from_address, message), ...] shared between all instances

    def __init__(self, host, port, seed):
        self.address = discovery.Address(host, port)

        config = dict(
            discovery=dict(),
            node=dict(privkey_hex=encode_hex(crypto.sha3(seed))),
            p2p=dict(listen_port=port),
        )
        config_discovery = config['discovery']
        config_discovery['listen_host'] = host
        config_discovery['listen_port'] = port

        app = AppMock()
        app.config = config
        self.protocol = discovery.DiscoveryProtocol(app=app, transport=self)

    def send(self, address, message):
        assert isinstance(address, discovery.Address)
        assert address != self.address
        self.messages.append((address, self.address, message))

    def receive(self, address, message):
        assert isinstance(address, discovery.Address)
        self.protocol.receive(address, message)

    def poll(self):
        # try to receive a message
        for i, (to_address, from_address, message) in enumerate(self.messages):
            if to_address == self.address:
                del self.messages[i]
                self.receive(from_address, message)


def test_packing():
    """
    https://github.com/ethereum/go-ethereum/blob/develop/crypto/secp256k1/secp256.go#L299
    https://github.com/ethereum/go-ethereum/blob/develop/p2p/discover/udp.go#L343
    """

    # get two DiscoveryProtocol instances
    alice = NodeDiscoveryMock(host='127.0.0.1', port=1, seed='alice').protocol
    bob = NodeDiscoveryMock(host='127.0.0.1', port=1, seed='bob').protocol

    cmd_id = 3  # findnode
    payload = [b'a', [b'b', b'c']]
    message = alice.pack(cmd_id, payload)

    r_pubkey, r_cmd_id, r_payload, mdc = bob.unpack(message)
    assert r_cmd_id == cmd_id
    assert r_payload == payload
    assert len(r_pubkey) == len(alice.pubkey)
    assert r_pubkey == alice.pubkey


def test_ping_pong():
    alice = NodeDiscoveryMock(host='127.0.0.1', port=1, seed='alice')
    bob = NodeDiscoveryMock(host='127.0.0.2', port=2, seed='bob')

    bob_node = alice.protocol.get_node(bob.protocol.pubkey, bob.address)
    alice.protocol.kademlia.ping(bob_node)
    assert len(NodeDiscoveryMock.messages) == 1
    # inspect message in queue
    msg = NodeDiscoveryMock.messages[0][2]
    remote_pubkey, cmd_id, payload, mdc = bob.protocol.unpack(msg)
    assert cmd_id == alice.protocol.cmd_id_map['ping']
    bob.poll()  # receives ping, sends pong
    assert len(NodeDiscoveryMock.messages) == 1
    alice.poll()  # receives pong
    assert len(NodeDiscoveryMock.messages) == 0


eip8_packets = dict(
    # ping packet with version 4, additional list elements
    ping1=decode_hex(remove_chars('''
    e9614ccfd9fc3e74360018522d30e1419a143407ffcce748de3e22116b7e8dc92ff74788c0b6663a
    aa3d67d641936511c8f8d6ad8698b820a7cf9e1be7155e9a241f556658c55428ec0563514365799a
    4be2be5a685a80971ddcfa80cb422cdd0101ec04cb847f000001820cfa8215a8d790000000000000
    000000000000000000018208ae820d058443b9a3550102
    ''', ' \n\t')),

    # ping packet with version 555, additional list elements and additional random data
    ping2=decode_hex(remove_chars('''
    577be4349c4dd26768081f58de4c6f375a7a22f3f7adda654d1428637412c3d7fe917cadc56d4e5e
    7ffae1dbe3efffb9849feb71b262de37977e7c7a44e677295680e9e38ab26bee2fcbae207fba3ff3
    d74069a50b902a82c9903ed37cc993c50001f83e82022bd79020010db83c4d001500000000abcdef
    12820cfa8215a8d79020010db885a308d313198a2e037073488208ae82823a8443b9a355c5010203
    040531b9019afde696e582a78fa8d95ea13ce3297d4afb8ba6433e4154caa5ac6431af1b80ba7602
    3fa4090c408f6b4bc3701562c031041d4702971d102c9ab7fa5eed4cd6bab8f7af956f7d565ee191
    7084a95398b6a21eac920fe3dd1345ec0a7ef39367ee69ddf092cbfe5b93e5e568ebc491983c09c7
    6d922dc3
    ''', ' \n\t')),

    # pong packet with additional list elements and additional random data
    pong=decode_hex(remove_chars('''
    09b2428d83348d27cdf7064ad9024f526cebc19e4958f0fdad87c15eb598dd61d08423e0bf66b206
    9869e1724125f820d851c136684082774f870e614d95a2855d000f05d1648b2d5945470bc187c2d2
    216fbe870f43ed0909009882e176a46b0102f846d79020010db885a308d313198a2e037073488208
    ae82823aa0fbc914b16819237dcd8801d7e53f69e9719adecb3cc0e790c57e91ca4461c9548443b9
    a355c6010203c2040506a0c969a58f6f9095004c0177a6b47f451530cab38966a25cca5cb58f0555
    42124e
    ''', ' \n\t')),

    # findnode packet with additional list elements and additional random data
    findnode=decode_hex(remove_chars('''
    c7c44041b9f7c7e41934417ebac9a8e1a4c6298f74553f2fcfdcae6ed6fe53163eb3d2b52e39fe91
    831b8a927bf4fc222c3902202027e5e9eb812195f95d20061ef5cd31d502e47ecb61183f74a504fe
    04c51e73df81f25c4d506b26db4517490103f84eb840ca634cae0d49acb401d8a4c6b6fe8c55b70d
    115bf400769cc1400f3258cd31387574077f301b421bc84df7266c44e9e6d569fc56be0081290476
    7bf5ccd1fc7f8443b9a35582999983999999280dc62cc8255c73471e0a61da0c89acdc0e035e260a
    dd7fc0c04ad9ebf3919644c91cb247affc82b69bd2ca235c71eab8e49737c937a2c396
    ''', ' \t\n')),

    # neighbours packet with additional list elements and additional random data
    neighbours=decode_hex(remove_chars('''
    c679fc8fe0b8b12f06577f2e802d34f6fa257e6137a995f6f4cbfc9ee50ed3710faf6e66f932c4c8
    d81d64343f429651328758b47d3dbc02c4042f0fff6946a50f4a49037a72bb550f3a7872363a83e1
    b9ee6469856c24eb4ef80b7535bcf99c0004f9015bf90150f84d846321163782115c82115db84031
    55e1427f85f10a5c9a7755877748041af1bcd8d474ec065eb33df57a97babf54bfd2103575fa8291
    15d224c523596b401065a97f74010610fce76382c0bf32f84984010203040101b840312c55512422
    cf9b8a4097e9a6ad79402e87a15ae909a4bfefa22398f03d20951933beea1e4dfa6f968212385e82
    9f04c2d314fc2d4e255e0d3bc08792b069dbf8599020010db83c4d001500000000abcdef12820d05
    820d05b84038643200b172dcfef857492156971f0e6aa2c538d8b74010f8e140811d53b98c765dd2
    d96126051913f44582e8c199ad7c6d6819e9a56483f637feaac9448aacf8599020010db885a308d3
    13198a2e037073488203e78203e8b8408dcab8618c3253b558d459da53bd8fa68935a719aff8b811
    197101a4b2b47dd2d47295286fc00cc081bb542d760717d1bdd6bec2c37cd72eca367d6dd3b9df73
    8443b9a355010203b525a138aa34383fec3d2719a0
    ''', ' \n\t')),
)


def test_eip8_packets():
    disc = NodeDiscoveryMock(host='127.0.0.1', port=1, seed='bob').protocol
    fromaddr = discovery.Address("127.0.0.1", 9999)
    for packet in eip8_packets.values():
        disc.unpack(packet)


# ############ test with real UDP ##################

def get_app(port, seed):
    config = dict(
        discovery=dict(),
        node=dict(privkey_hex=encode_hex(crypto.sha3(seed))),
        p2p=dict(listen_port=port),
    )
    config_discovery = config['discovery']
    config_discovery['listen_host'] = '127.0.0.1'
    config_discovery['listen_port'] = port
    config_discovery['bootstrap_nodes'] = []
    # create app
    app = BaseApp(config)
    discovery.NodeDiscovery.register_with_app(app)
    return app


def test_ping_pong_udp():
    alice_app = get_app(30000, 'alice')
    alice_app.start()
    alice_discovery = alice_app.services.discovery
    bob_app = get_app(30001, 'bob')
    bob_app.start()
    bob_discovery = bob_app.services.discovery

    gevent.sleep(0.1)
    bob_node = alice_discovery.protocol.get_node(bob_discovery.protocol.pubkey,
                                                 bob_discovery.address)
    assert bob_node not in alice_discovery.protocol.kademlia.routing
    alice_discovery.protocol.kademlia.ping(bob_node)
    assert bob_node not in alice_discovery.protocol.kademlia.routing
    gevent.sleep(0.1)
    bob_app.stop()
    alice_app.stop()
    assert bob_node in alice_discovery.protocol.kademlia.routing


# must use yield_fixture rather than fixture prior to pytest 2.10
@pytest.yield_fixture
def kademlia_timeout():
    """
    Rolls back kademlia timeout after the test.
    """
    # backup the previous value
    k_request_timeout = kademlia.k_request_timeout
    # return kademlia
    yield kademlia
    # restore the previous value
    kademlia.k_request_timeout = k_request_timeout

def test_bootstrap_udp(kademlia_timeout):
    """
    startup num_apps udp server and node applications
    """

    # set timeout to something more tolerant
    kademlia_timeout.k_request_timeout = 10000.

    num_apps = 6
    apps = []
    for i in range(num_apps):
        app = get_app(30002 + i, 'app%d' % i)
        app.start()
        apps.append(app)

    gevent.sleep(0.1)
    sleep_delay = 1  # we need to wait for the packets to be delivered

    kproto = lambda app: app.services.discovery.protocol.kademlia
    this_node = lambda app: kproto(app).this_node

    boot_node = this_node(apps[0])
    assert boot_node.address

    for app in apps[1:]:
        print('test bootstrap from=%s to=%s' % (this_node(app), boot_node))
        kproto(app).bootstrap([boot_node])
        gevent.sleep(sleep_delay)

    gevent.sleep(sleep_delay * 2)

    for app in apps[1:]:
        print('test find_node from=%s' % (this_node(app)))
        kproto(app).find_node(this_node(app).id)
        gevent.sleep(sleep_delay)

    gevent.sleep(sleep_delay * 2)

    for app in apps:
        app.stop()

    # now all nodes should know each other
    for i, app in enumerate(apps):
        num = len(kproto(app).routing)
        print(num)
        if i < len(apps) // 2:  # only the first half has enough time to get all updates
            assert num >= num_apps - 1


def main():
    "test connecting nodes"

    # stop on every unhandled exception!
    gevent.get_hub().SYSTEM_ERROR = BaseException  # (KeyboardInterrupt, SystemExit, SystemError)

    app = get_app(30304, 'theapp')
    # app.config['p2p']['listen_host'] = '127.0.0.1'
    app.config['p2p']['listen_host'] = '0.0.0.0'

    print("this node is")
    proto = app.services.discovery.protocol.kademlia
    this_node = proto.this_node
    print(encode_hex(this_node.pubkey))

    # add external node

    go_local = b'enode://6ed2fecb28ff17dec8647f08aa4368b57790000e0e9b33a7b91f32c41b6ca9ba21600e9a8c44248ce63a71544388c6745fa291f88f8b81e109ba3da11f7b41b9@127.0.0.1:30303'

    go_bootstrap = b'enode://6cdd090303f394a1cac34ecc9f7cda18127eafa2a3a06de39f6d920b0e583e062a7362097c7c65ee490a758b442acd5c80c6fce4b148c6a391e946b45131365b@54.169.166.226:30303'

    cpp_bootstrap = b'enode://24f904a876975ab5c7acbedc8ec26e6f7559b527c073c6e822049fee4df78f2e9c74840587355a068f2cdb36942679f7a377a6d8c5713ccf40b1d4b99046bba0@5.1.83.226:30303'

    n1 = b'enode://1d799d32547761cf66250f94b4ac1ebfc3246ce9bd87fbf90ef8d770faf48c4d96290ea0c72183d6c1ddca3d2725dad018a6c1c5d1971dbaa182792fa937e89d@162.247.54.200:1024'
    n2 = b'enode://1976e20d6ec2de2dd4df34d8e949994dc333da58c967c62ca84b4d545d3305942207565153e94367f5d571ef79ce6da93c5258e88ca14788c96fbbac40f4a4c7@52.0.216.64:30303'
    n3 = b'enode://14bb48727c8a103057ba06cc010c810e9d4beef746c54d948b681218195b3f1780945300c2534d422d6069f7a0e378c450db380f8efff8b4eccbb48c0c5bb9e8@179.218.168.19:30303'

    nb = b'enode://1976e20d6ec2de2dd4df34d8e949994dc333da58c967c62ca84b4d545d3305942207565153e94367f5d571ef79ce6da93c5258e88ca14788c96fbbac40f4a4c7@52.0.216.64:30303'

    node_uri = cpp_bootstrap

    r_node = discovery.Node.from_uri(node_uri)
    print("remote node is", r_node)
    # add node to the routing table

    print("START & TEST BOOTSTRAP")
    app.config['p2p']['bootstrap_nodes'] = [node_uri]
    app.start()

    gevent.sleep(2.)
    print("TEST FIND_NODE")
    for i in range(10):
        nodeid = kademlia.random_nodeid()
        assert isinstance(nodeid, type(this_node.id))
        proto.find_node(nodeid)
    gevent.sleep(1.)

    pinged = lambda: set(n for t, n, r in proto._expected_pongs.values())

    for i in range(10):
        print('num nodes', len(proto.routing))
        gevent.sleep(1)
        # proto.find_node(this_node.id)
        # for node in proto.routing:
        proto.ping(r_node)
        # proto.find_node(this_node.id)

    print('nodes in routing')
    for node in proto.routing:
        print(node.to_uri())
    print('nodes we are waiting for pongs')

    for node in pinged():
        print(node.to_uri())


if __name__ == '__main__':
    import ethereum.slogging

    ethereum.slogging.configure(config_string=':debug')
    main()


"""
unexpected pongs from cpp client

case:
    bootstrap pubkey does not match



versions would be good
i get a ping reply by 2 nodes



"""
