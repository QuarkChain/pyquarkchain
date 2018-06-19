from devp2p.p2p_protocol import P2PProtocol
from devp2p.service import WiredService
from devp2p.app import BaseApp
from devp2p.multiplexer import Packet
from devp2p.utils import remove_chars
import pytest
from rlp.utils import decode_hex
# notify peer of successfulll handshake!
# so other protocols get registered
# so other protocols can do their handshake


class PeerMock(object):
    packets = []
    config = dict(p2p=dict(listen_port=3000),
                  node=dict(id='\x00' * 64), client_version_string='devp2p 0.1.1')
    capabilities = [(b'p2p', 2), (b'eth', 57)]
    stopped = False
    hello_received = False
    remote_client_version = ''
    remote_pubkey = ''
    remote_hello_version = 0

    def receive_hello(self, proto, version, client_version_string, capabilities,
                      listen_port, remote_pubkey):
        for name, version in capabilities:
            assert isinstance(name, bytes)
            assert isinstance(version, int)
        self.hello_received = True
        self.remote_client_version = client_version_string
        self.remote_pubkey = remote_pubkey
        self.remote_hello_version = version

    def send_packet(self, packet):
        print('sending', packet)
        self.packets.append(packet)

    def stop(self):
        self.stopped = True

    def report_error(self, reason):
        pass


@pytest.mark.xfail
def test_protocol():
    peer = PeerMock()
    proto = P2PProtocol(peer, WiredService(BaseApp()))

    # ping pong
    proto.send_ping()
    ping_packet = peer.packets.pop()
    proto._receive_ping(ping_packet)
    pong_packet = peer.packets.pop()
    proto._receive_pong(pong_packet)
    assert not peer.packets

    # hello (fails same nodeid)
    proto.send_hello()
    hello_packet = peer.packets.pop()
    proto._receive_hello(hello_packet)
    disconnect_packet = peer.packets.pop()  # same nodeid
    assert disconnect_packet.cmd_id == P2PProtocol.disconnect.cmd_id
    assert not peer.stopped  # FIXME: @heikoheiko this fails currently

    # hello (works)
    proto.send_hello()
    hello_packet = peer.packets.pop()
    peer.config['node']['id'] = '\x01' * 64  # change nodeid
    proto._receive_hello(hello_packet)
    assert not peer.packets
    assert not peer.stopped  # FIXME: @heikoheiko this fails currently
    assert peer.hello_received

    # disconnect
    proto.send_disconnect(reason=proto.disconnect.reason.disconnect_requested)
    disconnect_packet = peer.packets.pop()
    proto._receive_disconnect(disconnect_packet)
    assert not peer.packets
    assert peer.stopped


eip8_hello = decode_hex(remove_chars('''
    f87137916b6e6574682f76302e39312f706c616e39cdc5836574683dc6846d6f726b1682270fb840
    fda1cff674c90c9a197539fe3dfb53086ace64f83ed7c6eabec741f7f381cc803e52ab2cd55d5569
    bce4347107a310dfd5f88a010cd2ffd1005ca406f1842877c883666f6f836261720304
''', ' \n\t'))

def test_eip8_hello():
    peer = PeerMock()
    proto = P2PProtocol(peer, WiredService(BaseApp()))
    test_packet = Packet(cmd_id=1, payload=eip8_hello)
    proto._receive_hello(test_packet)
    assert peer.hello_received
    assert peer.remote_client_version == b"kneth/v0.91/plan9"
    assert peer.remote_hello_version == 22
    assert peer.remote_pubkey == decode_hex('fda1cff674c90c9a197539fe3dfb53086ace64f83ed7c6eabec741f7f381cc803e52ab2cd55d5569bce4347107a310dfd5f88a010cd2ffd1005ca406f1842877')


def test_callback():
    peer = PeerMock()
    proto = P2PProtocol(peer, WiredService(BaseApp()))

    # setup callback
    r = []

    def cb(_proto, **data):
        assert _proto == proto
        r.append(data)
    proto.receive_pong_callbacks.append(cb)

    # trigger
    proto.send_ping()
    ping_packet = peer.packets.pop()
    proto._receive_ping(ping_packet)
    pong_packet = peer.packets.pop()
    proto._receive_pong(pong_packet)
    assert not peer.packets
    assert len(r) == 1
    assert r[0] == dict()
