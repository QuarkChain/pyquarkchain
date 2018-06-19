import platform

import pytest

from devp2p import peermanager, peer
from devp2p import crypto
from devp2p.app import BaseApp
import devp2p.muxsession
import rlp
import devp2p.p2p_protocol
import time
import gevent
import copy
from rlp.utils import encode_hex, decode_hex, str_to_bytes


def get_connected_apps():
    a_config = dict(p2p=dict(listen_host='127.0.0.1', listen_port=3005),
                    node=dict(privkey_hex=encode_hex(crypto.sha3(b'a'))))
    b_config = copy.deepcopy(a_config)
    b_config['p2p']['listen_port'] = 3006
    b_config['node']['privkey_hex'] = encode_hex(crypto.sha3(b'b'))

    a_app = BaseApp(a_config)
    peermanager.PeerManager.register_with_app(a_app)
    a_app.start()

    b_app = BaseApp(b_config)
    peermanager.PeerManager.register_with_app(b_app)
    b_app.start()

    a_peermgr = a_app.services.peermanager
    b_peermgr = b_app.services.peermanager

    # connect
    host = b_config['p2p']['listen_host']
    port = b_config['p2p']['listen_port']
    pubkey = crypto.privtopub(decode_hex(b_config['node']['privkey_hex']))
    a_peermgr.connect((host, port), remote_pubkey=pubkey)

    return a_app, b_app


def test_handshake():
    a_app, b_app = get_connected_apps()
    gevent.sleep(1)
    assert a_app.services.peermanager.peers
    assert b_app.services.peermanager.peers
    a_app.stop()
    b_app.stop()


def test_big_transfer():

    class transfer(devp2p.p2p_protocol.BaseProtocol.command):
        cmd_id = 4
        structure = [('raw_data', rlp.sedes.binary)]

        def create(self, proto, raw_data=''):
            return [raw_data]

    # money patches
    devp2p.p2p_protocol.P2PProtocol.transfer = transfer
    devp2p.muxsession.MultiplexedSession.max_window_size = 8 * 1024

    a_app, b_app = get_connected_apps()
    gevent.sleep(.1)

    a_protocol = a_app.services.peermanager.peers[0].protocols[devp2p.p2p_protocol.P2PProtocol]
    b_protocol = b_app.services.peermanager.peers[0].protocols[devp2p.p2p_protocol.P2PProtocol]

    st = time.time()

    def cb(proto, **data):
        print('took', time.time() - st, len(data['raw_data']))

    b_protocol.receive_transfer_callbacks.append(cb)
    raw_data = '0' * 1 * 1000 * 100
    a_protocol.send_transfer(raw_data=raw_data)

    # 0.03 secs for 0.1mb
    # 0.28 secs for 1mb
    # 2.7 secs for 10mb
    # 3.7 MB/s == 30Mbit

    gevent.sleep(1)
    a_app.stop()
    b_app.stop()
    gevent.sleep(0.1)


def test_dumb_peer():
    """ monkeypatch receive_hello to make peer not to mark that hello was received.
    no hello in defined timeframe makes peer to stop """

    def mock_receive_hello(self, proto, version, client_version_string,
                           capabilities, listen_port, remote_pubkey):
        pass

    peer.Peer.receive_hello = mock_receive_hello

    a_app, b_app = get_connected_apps()

    gevent.sleep(1.0)
    assert a_app.services.peermanager.num_peers() == 1
    assert b_app.services.peermanager.num_peers() == 1

    gevent.sleep(peer.Peer.dumb_remote_timeout)
    assert a_app.services.peermanager.num_peers() == 0
    assert b_app.services.peermanager.num_peers() == 0

    a_app.stop()
    b_app.stop()
    gevent.sleep(0.1)

def test_offset_dispatch():
    """ test offset-based cmd_id translation """

    def make_mock_service(n, size):
        class MockProtocol(devp2p.protocol.BaseProtocol):
            protocol_id = n
            max_cmd_id = size
            name = str_to_bytes('mock%d' % n)
            version = 1
            def __init__(self, *args, **kwargs):
                super(MockProtocol, self).__init__(*args, **kwargs)
                self.cmd_by_id = ['mock_cmd%d' % i for i in range(size + 1)]

        class MockService(devp2p.service.WiredService):
            name = 'mock%d' % n
            default_config = {}
            wire_protocol = MockProtocol
            def __init__(self):
                pass
        return MockService()

    services = [
        make_mock_service(2, 7),
        make_mock_service(19, 1),
    ]

    class MockPeerManager(peermanager.PeerManager):
        privkey = crypto.sha3(b'a')
        pubkey = crypto.privtopub(privkey)
        wired_services = services
        config = {
            'client_version_string': 'mock',
            'p2p': {'listen_port': 3006},
            'node': {
                'privkey_hex': encode_hex(privkey),
                'id': encode_hex(pubkey),
            }}
        def __init__(self):
            pass

    class MockConnection(object):
        def getpeername(*_):
            return "mock"

    packets = []

    def mock_add_packet(x):
        packets.append(x)

    class MockPacket(object):
        def __init__(self, proto, cmd, cookie):
            self.protocol_id = proto
            self.cmd_id = cmd
            self.__cookie = cookie

    mpm = MockPeerManager()
    p = peer.Peer(mpm, MockConnection())
    mpm.peers = [p]
    p.offset_based_dispatch = True
    p.mux.add_packet = mock_add_packet
    p.connect_service(services[0])
    p.connect_service(services[1])
    for i in range(8):
        p.send_packet(MockPacket(2, i, 'protoA%d' % i))
    for i in range(2):
        p.send_packet(MockPacket(19, i, 'protoB%d' % i))
    for i in range(8):
        pkt = packets.pop(0)
        proto, cmd_id = p.protocol_cmd_id_from_packet(pkt)
        assert proto.protocol_id == 2
        assert proto.name == b'mock2'
        assert cmd_id == i
    for i in range(2):
        pkt = packets.pop(0)
        proto, cmd_id = p.protocol_cmd_id_from_packet(pkt)
        assert proto.protocol_id == 19
        assert proto.name == b'mock19'
        assert cmd_id == i

    p.stop()

def connect_go():
    a_config = dict(p2p=dict(listen_host='127.0.0.1', listen_port=3010),
                    node=dict(privkey_hex=encode_hex(crypto.sha3(b'a'))))

    a_app = BaseApp(a_config)
    peermanager.PeerManager.register_with_app(a_app)
    a_app.start()

    a_peermgr = a_app.services.peermanager

    # connect
    pubkey = decode_hex("6ed2fecb28ff17dec8647f08aa4368b57790000e0e9b33a7b91f32c41b6ca9ba21600e9a8c44248ce63a71544388c6745fa291f88f8b81e109ba3da11f7b41b9")
    a_peermgr.connect(('127.0.0.1', 30303), remote_pubkey=pubkey)
    gevent.sleep(50)
    a_app.stop()


if __name__ == '__main__':
    # ethereum -loglevel 5 --bootnodes ''
    import ethereum.slogging
    ethereum.slogging.configure(config_string=':debug')
    # connect_go()
    test_big_transfer()
    test_dumb_peer()
