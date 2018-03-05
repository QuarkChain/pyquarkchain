# -*- coding: utf-8 -*-
import re
import time
from socket import AF_INET, AF_INET6

from repoze.lru import LRUCache
import gevent
import gevent.socket
import ipaddress
import rlp
from rlp.utils import decode_hex, is_integer, str_to_bytes, bytes_to_str, safe_ord
from gevent.server import DatagramServer

from devp2p import slogging
from devp2p import crypto
from devp2p import kademlia
from devp2p import utils
from .service import BaseService
from .upnp import add_portmap, remove_portmap


log = slogging.get_logger('p2p.discovery')


class DefectiveMessage(Exception):
    pass


class InvalidSignature(DefectiveMessage):
    pass


class WrongMAC(DefectiveMessage):
    pass


class PacketExpired(DefectiveMessage):
    pass

enc_port = lambda p: utils.ienc4(p)[-2:]
dec_port = utils.idec

import sys
PY3 = sys.version_info[0] >= 3

ip_pattern = re.compile(b"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|([0-9a-f]{0,4}:)*([0-9a-f]{0,4})?$")


class Address(object):

    """
    Extend later, but make sure we deal with objects
    Multiaddress
    https://github.com/haypo/python-ipy
    """

    def __init__(self, ip, udp_port, tcp_port=0, from_binary=False):
        tcp_port = tcp_port or udp_port
        if from_binary:
            self.udp_port = dec_port(udp_port)
            self.tcp_port = dec_port(tcp_port)
        else:
            assert is_integer(udp_port)
            assert is_integer(tcp_port)
            self.udp_port = udp_port
            self.tcp_port = tcp_port
        try:
            # `ip` could be in binary or ascii format, independent of
            # from_binary's truthy. We use ad-hoc regexp to determine format
            _ip = str_to_bytes(ip)
            _ip = (bytes_to_str(ip) if PY3 else unicode(ip)) if ip_pattern.match(_ip) else _ip
            self._ip = ipaddress.ip_address(_ip)
        except ipaddress.AddressValueError as e:
            log.debug("failed to parse ip", error=e, ip=ip)
            raise e

    @property
    def ip(self):
        return str(self._ip)

    def update(self, addr):
        if not self.tcp_port:
            self.tcp_port = addr.tcp_port

    def __eq__(self, other):
        # addresses equal if they share ip and udp_port
        return (self.ip, self.udp_port) == (other.ip, other.udp_port)

    def __repr__(self):
        return 'Address(%s:%s)' % (self.ip, self.udp_port)

    def to_dict(self):
        return dict(ip=self.ip, udp_port=self.udp_port, tcp_port=self.tcp_port)

    def to_binary(self):
        """
        struct Endpoint
            unsigned address; // BE encoded 32-bit or 128-bit unsigned (layer3 address; size determins ipv4 vs ipv6)
            unsigned udpPort; // BE encoded 16-bit unsigned
            unsigned tcpPort; // BE encoded 16-bit unsigned        }
        """
        return list((self._ip.packed, enc_port(self.udp_port), enc_port(self.tcp_port)))
    to_endpoint = to_binary

    @classmethod
    def from_binary(cls, ip, udp_port, tcp_port='\x00\x00'):
        return cls(ip, udp_port, tcp_port, from_binary=True)
    from_endpoint = from_binary


class Node(kademlia.Node):

    def __init__(self, pubkey, address=None):
        kademlia.Node.__init__(self, pubkey)
        assert address is None or isinstance(address, Address)
        self.address = address
        self.reputation = 0
        self.rlpx_version = 0

    @classmethod
    def from_uri(cls, uri):
        ip, port, pubkey = utils.host_port_pubkey_from_uri(uri)
        return cls(pubkey, Address(ip, int(port)))

    def to_uri(self):
        return utils.host_port_pubkey_to_uri(str_to_bytes(self.address.ip),
            self.address.udp_port, self.pubkey)


class DiscoveryProtocolTransport(object):

    def send(self, address, message):
        assert isinstance(address, Address)

    def receive(self, address, message):
        assert isinstance(address, Address)


class KademliaProtocolAdapter(kademlia.KademliaProtocol):
    pass

"""
# Node Discovery Protocol

**Node**: an entity on the network
**Node ID**: 512 bit public key of node

The Node Discovery protocol provides a way to find RLPx nodes
that can be connected to. It uses a Kademlia-like protocol to maintain a
distributed database of the IDs and endpoints of all listening nodes.

Each node keeps a node table as described in the Kademlia paper
[[Maymounkov, Mazières 2002][kad-paper]]. The node table is configured
with a bucket size of 16 (denoted `k` in Kademlia), concurrency of 3
(denoted `α` in Kademlia), and 8 bits per hop (denoted `b` in
Kademlia) for routing. The eviction check interval is 75 milliseconds,
and the idle bucket-refresh interval is
3600 seconds.

In order to maintain a well-formed network, RLPx nodes should try to connect
to an unspecified number of close nodes. To increase resilience against Sybil attacks,
nodes should also connect to randomly chosen, non-close nodes.

Each node runs the UDP-based RPC protocol defined below. The
`FIND_DATA` and `STORE` requests from the Kademlia paper are not part
of the protocol since the Node Discovery Protocol does not provide DHT
functionality.

[kad-paper]: http://www.cs.rice.edu/Conferences/IPTPS02/109.pdf

## Joining the network

When joining the network, fills its node table by perfoming a
recursive Find Node operation with its own ID as the `Target`. The
initial Find Node request is sent to one or more bootstrap nodes.

## RPC Protocol

RLPx nodes that want to accept incoming connections should listen on
the same port number for UDP packets (Node Discovery Protocol) and
TCP connections (RLPx protocol).

All requests time out after are 300ms. Requests are not re-sent.

"""


class DiscoveryProtocol(kademlia.WireInterface):

    """
    ## Packet Data
    All packets contain an `Expiration` date to guard against replay attacks.
    The date should be interpreted as a UNIX timestamp.
    The receiver should discard any packet whose `Expiration` value is in the past.
    """
    version = 4
    expiration = 60  # let messages expire after N secondes
    cmd_id_map = dict(ping=1, pong=2, find_node=3, neighbours=4)
    rev_cmd_id_map = dict((v, k) for k, v in cmd_id_map.items())

    # number of required top-level list elements for each cmd_id.
    # elements beyond this length are trimmed.
    cmd_elem_count_map = dict(ping=4, pong=3, find_node=2, neighbours=2)

    encoders = dict(cmd_id=chr,
                    expiration=rlp.sedes.big_endian_int.serialize)

    decoders = dict(cmd_id=safe_ord,
                    expiration=rlp.sedes.big_endian_int.deserialize)

    def __init__(self, app, transport):
        self.app = app
        self.transport = transport
        self.privkey = decode_hex(app.config['node']['privkey_hex'])
        self.pubkey = crypto.privtopub(self.privkey)
        self.nodes = LRUCache(2048)   # nodeid->Node,  fixme should be loaded
        self.this_node = Node(self.pubkey, self.transport.address)
        self.kademlia = KademliaProtocolAdapter(self.this_node, wire=self)
        this_enode = utils.host_port_pubkey_to_uri(self.app.config['discovery']['listen_host'],
                                                   self.app.config['discovery']['listen_port'],
                                                   self.pubkey)
        log.info('starting discovery proto', this_enode=this_enode)

    def get_node(self, nodeid, address=None):
        "return node or create new, update address if supplied"
        assert isinstance(nodeid, bytes)
        assert len(nodeid) == 512 // 8
        assert address or self.nodes.get(nodeid)
        if not self.nodes.get(nodeid):
            self.nodes.put(nodeid, Node(nodeid, address))
        node = self.nodes.get(nodeid)
        if address:
            assert isinstance(address, Address)
            node.address = address
        assert node.address
        return node

    def sign(self, msg):
        """
        signature: sign(privkey, sha3(packet-type || packet-data))
        signature: sign(privkey, sha3(pubkey || packet-type || packet-data))
            // implementation w/MCD
        """
        msg = crypto.sha3(msg)
        return crypto.sign(msg, self.privkey)

    def pack(self, cmd_id, payload):
        """
        UDP packets are structured as follows:

        hash || signature || packet-type || packet-data
        packet-type: single byte < 2**7 // valid values are [1,4]
        packet-data: RLP encoded list. Packet properties are serialized in the order in
                    which they're defined. See packet-data below.

        Offset  |
        0       | MDC       | Ensures integrity of packet,
        65      | signature | Ensures authenticity of sender, `SIGN(sender-privkey, MDC)`
        97      | type      | Single byte in range [1, 4] that determines the structure of Data
        98      | data      | RLP encoded, see section Packet Data

        The packets are signed and authenticated. The sender's Node ID is determined by
        recovering the public key from the signature.

            sender-pubkey = ECRECOVER(Signature)

        The integrity of the packet can then be verified by computing the
        expected MDC of the packet as:

            MDC = SHA3(sender-pubkey || type || data)

        As an optimization, implementations may look up the public key by
        the UDP sending address and compute MDC before recovering the sender ID.
        If the MDC values do not match, the packet can be dropped.
        """
        assert cmd_id in self.cmd_id_map.values()
        assert isinstance(payload, list)

        cmd_id = str_to_bytes(self.encoders['cmd_id'](cmd_id))
        expiration = self.encoders['expiration'](int(time.time() + self.expiration))
        encoded_data = rlp.encode(payload + [expiration])
        signed_data = crypto.sha3(cmd_id + encoded_data)
        signature = crypto.sign(signed_data, self.privkey)
        # assert crypto.verify(self.pubkey, signature, signed_data)
        # assert self.pubkey == crypto.ecdsa_recover(signed_data, signature)
        # assert crypto.verify(self.pubkey, signature, signed_data)
        assert len(signature) == 65
        mdc = crypto.sha3(signature + cmd_id + encoded_data)
        assert len(mdc) == 32
        return mdc + signature + cmd_id + encoded_data

    def unpack(self, message):
        """
        macSize  = 256 / 8 = 32
        sigSize  = 520 / 8 = 65
        headSize = macSize + sigSize = 97
        hash, sig, sigdata := buf[:macSize], buf[macSize:headSize], buf[headSize:]
        shouldhash := crypto.Sha3(buf[macSize:])
        """
        mdc = message[:32]
        if mdc != crypto.sha3(message[32:]):
            log.debug('packet with wrong mcd')
            raise WrongMAC()
        signature = message[32:97]
        assert len(signature) == 65
        signed_data = crypto.sha3(message[97:])
        remote_pubkey = crypto.ecdsa_recover(signed_data, signature)
        assert len(remote_pubkey) == 512 / 8
        # if not crypto.verify(remote_pubkey, signature, signed_data):
        #     raise InvalidSignature()
        cmd_id = self.decoders['cmd_id'](message[97])
        cmd = self.rev_cmd_id_map[cmd_id]
        payload = rlp.decode(message[98:], strict=False)
        assert isinstance(payload, list)
        # ignore excessive list elements as required by EIP-8.
        payload = payload[:self.cmd_elem_count_map.get(cmd, len(payload))]
        return remote_pubkey, cmd_id, payload, mdc

    def receive(self, address, message):
        log.debug('<<< message', address=address)
        assert isinstance(address, Address)
        try:
            remote_pubkey, cmd_id, payload, mdc = self.unpack(message)
            # Note: as of discovery version 4, expiration is the last element for all
            # packets. This might not be the case for a later version, but just popping
            # the last element is good enough for now.
            expiration = self.decoders['expiration'](payload.pop())
            if time.time() > expiration:
                raise PacketExpired()
        except DefectiveMessage:
            return
        cmd = getattr(self, 'recv_' + self.rev_cmd_id_map[cmd_id])
        nodeid = remote_pubkey
        remote = self.get_node(nodeid, address)
        log.debug("Dispatching received message", local=self.this_node, remoteid=remote,
                  cmd=self.rev_cmd_id_map[cmd_id])
        cmd(nodeid, payload, mdc)

    def send(self, node, message):
        assert node.address
        log.debug('>>> message', address=node.address)
        self.transport.send(node.address, message)

    def send_ping(self, node):
        """
        ### Ping (type 0x01)

        Ping packets can be sent and received at any time. The receiver should
        reply with a Pong packet and update the IP/Port of the sender in its
        node table.

        PingNode packet-type: 0x01

        PingNode packet-type: 0x01
        struct PingNode             <= 59 bytes
        {
            h256 version = 0x3;     <= 1
            Endpoint from;          <= 23
            Endpoint to;            <= 23
            unsigned expiration;    <= 9
        };

        struct Endpoint             <= 24 == [17,3,3]
        {
            unsigned address; // BE encoded 32-bit or 128-bit unsigned (layer3 address; size determins ipv4 vs ipv6)
            unsigned udpPort; // BE encoded 16-bit unsigned
            unsigned tcpPort; // BE encoded 16-bit unsigned
        }
        """
        assert isinstance(node, type(self.this_node)) and node != self.this_node
        log.debug('>>> ping', remoteid=node)
        version = rlp.sedes.big_endian_int.serialize(self.version)
        ip = self.app.config['discovery']['listen_host']
        udp_port = self.app.config['discovery']['listen_port']
        tcp_port = self.app.config['p2p']['listen_port']
        payload = [version,
                   Address(ip, udp_port, tcp_port).to_endpoint(),
                   node.address.to_endpoint()]
        assert len(payload) == 3
        message = self.pack(self.cmd_id_map['ping'], payload)
        self.send(node, message)
        return message[:32]  # return the MDC to identify pongs

    def recv_ping(self, nodeid, payload, mdc):
        """
        update ip, port in node table
        Addresses can only be learned by ping messages
        """
        if not len(payload) == 3:
            log.error('invalid ping payload', payload=payload)
            return
        node = self.get_node(nodeid)
        log.debug('<<< ping', node=node)
        remote_address = Address.from_endpoint(*payload[1])  # from address
        #my_address = Address.from_endpoint(*payload[2])  # my address
        self.get_node(nodeid).address.update(remote_address)
        self.kademlia.recv_ping(node, echo=mdc)

    def send_pong(self, node, token):
        """
        ### Pong (type 0x02)

        Pong is the reply to a Ping packet.

        Pong packet-type: 0x02
        struct Pong                 <= 66 bytes
        {
            Endpoint to;
            h256 echo;
            unsigned expiration;
        };
        """
        log.debug('>>> pong', remoteid=node)
        payload = [node.address.to_endpoint(), token]
        assert len(payload[0][0]) in (4, 16), payload
        message = self.pack(self.cmd_id_map['pong'], payload)
        self.send(node, message)

    def recv_pong(self, nodeid,  payload, mdc):
        if not len(payload) == 2:
            log.error('invalid pong payload', payload=payload)
            return
        assert len(payload[0]) == 3, payload

        # Verify address is valid
        Address.from_endpoint(*payload[0])
        echoed = payload[1]
        if self.nodes.get(nodeid):
            node = self.get_node(nodeid)
            self.kademlia.recv_pong(node, echoed)
        else:
            log.debug('<<< unexpected pong from unkown node')

    def send_find_node(self, node, target_node_id):
        """
        ### Find Node (type 0x03)

        Find Node packets are sent to locate nodes close to a given target ID.
        The receiver should reply with a Neighbors packet containing the `k`
        nodes closest to target that it knows about.

        FindNode packet-type: 0x03
        struct FindNode             <= 76 bytes
        {
            NodeId target; // Id of a node. The responding node will send back nodes closest to the target.
            unsigned expiration;
        };
        """
        assert is_integer(target_node_id)
        target_node_id = utils.int_to_big_endian(target_node_id).rjust(kademlia.k_pubkey_size // 8, b'\0')
        assert len(target_node_id) == kademlia.k_pubkey_size // 8
        log.debug('>>> find_node', remoteid=node)
        message = self.pack(self.cmd_id_map['find_node'], [target_node_id])
        self.send(node, message)

    def recv_find_node(self, nodeid, payload, mdc):
        node = self.get_node(nodeid)
        log.debug('<<< find_node', remoteid=node)
        assert len(payload[0]) == kademlia.k_pubkey_size / 8
        target = utils.big_endian_to_int(payload[0])
        self.kademlia.recv_find_node(node, target)

    def send_neighbours(self, node, neighbours):
        """
        ### Neighbors (type 0x04)

        Neighbors is the reply to Find Node. It contains up to `k` nodes that
        the sender knows which are closest to the requested `Target`.

        Neighbors packet-type: 0x04
        struct Neighbours           <= 1423
        {
            list nodes: struct Neighbour    <= 88: 1411; 76: 1219
            {
                inline Endpoint endpoint;
                NodeId node;
            };

            unsigned expiration;
        };
        """
        assert isinstance(neighbours, list)
        assert not neighbours or isinstance(neighbours[0], Node)
        nodes = []
        neighbours = sorted(neighbours)
        for n in neighbours:
            l = n.address.to_endpoint() + [n.pubkey]
            nodes.append(l)
        log.debug('>>> neighbours', remoteid=node, count=len(nodes), local=self.this_node,
                  neighbours=neighbours)
        # FIXME: don't brake udp packet size / chunk message / also when receiving
        message = self.pack(self.cmd_id_map['neighbours'], [nodes[:12]])  # FIXME
        self.send(node, message)

    def recv_neighbours(self, nodeid, payload, mdc):
        remote = self.get_node(nodeid)
        assert len(payload) == 1
        neighbours_lst = payload[0]
        assert isinstance(neighbours_lst, list)

        neighbours = []
        for n in neighbours_lst:
            nodeid = n.pop()
            address = Address.from_endpoint(*n)
            node = self.get_node(nodeid, address)
            assert node.address
            neighbours.append(node)

        log.debug('<<< neighbours', remoteid=remote, local=self.this_node, neighbours=neighbours,
                  count=len(neighbours_lst))
        self.kademlia.recv_neighbours(remote, neighbours)


class NodeDiscovery(BaseService, DiscoveryProtocolTransport):

    """
    Persist the list of known nodes with their reputation
    """

    name = 'discovery'
    server = None  # will be set to DatagramServer
    nat_upnp = None
    default_config = dict(
        discovery=dict(
            listen_port=30303,
            listen_host='0.0.0.0',
        ),
        node=dict(privkey_hex=''))

    def __init__(self, app):
        BaseService.__init__(self, app)
        log.info('NodeDiscovery init')
        # man setsockopt
        self.protocol = DiscoveryProtocol(app=self.app, transport=self)

    @property
    def address(self):
        ip = self.app.config['discovery']['listen_host']
        port = self.app.config['discovery']['listen_port']
        return Address(ip, port)

    # def _send(self, address, message):
    #     assert isinstance(address, Address)
    #     sock = gevent.socket.socket(type=gevent.socket.SOCK_DGRAM)
    # sock.bind(('0.0.0.0', self.address.port))  # send from our recv port
    #     sock.connect((address.ip, address.port))
    #     log.debug('sending', size=len(message), to=address)
    #     sock.send(message)

    def send(self, address, message):
        assert isinstance(address, Address)
        log.debug('sending', size=len(message), to=address)
        try:
            self.server.sendto(message, (address.ip, address.udp_port))
        except gevent.socket.error as e:
            log.debug('udp write error', address=address, errno=e.errno, reason=e.strerror)
            log.debug('waiting for recovery')
            gevent.sleep(0.5)

    def receive(self, address, message):
        assert isinstance(address, Address)
        self.protocol.receive(address, message)

    def _handle_packet(self, message, ip_port):
        try:
            log.debug('handling packet', address=ip_port, size=len(message))
            assert len(ip_port) == 2
            address = Address(ip=ip_port[0], udp_port=ip_port[1])
            self.receive(address, message)
        except Exception as e:
            log.debug("failed to handle discovery packet",
                      error=e, message=message, ip_port=ip_port)

    def start(self):
        log.info('starting discovery')
        # start a listening server
        ip = self.app.config['discovery']['listen_host']
        port = self.app.config['discovery']['listen_port']
        # nat port mappin
        self.nat_upnp = add_portmap(port, 'UDP', 'Ethereum DEVP2P Discovery')
        log.info('starting listener', port=port, host=ip)
        self.server = DatagramServer((ip, port), handle=self._handle_packet)
        self.server.start()
        super(NodeDiscovery, self).start()

        # bootstap
        nodes = [Node.from_uri(x) for x in self.app.config['discovery']['bootstrap_nodes']]
        if nodes:
            self.protocol.kademlia.bootstrap(nodes)

    def _run(self):
        log.debug('_run called')
        evt = gevent.event.Event()
        evt.wait()

    def stop(self):
        log.info('stopping discovery')
        remove_portmap(self.nat_upnp, self.app.config['discovery']['listen_port'], 'UDP')
        if self.server:
            self.server.stop()
        super(NodeDiscovery, self).stop()

if __name__ == '__main__':
    pass
