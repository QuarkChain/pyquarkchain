import time
import gevent
from rlp import sedes
from .multiplexer import Packet
from .protocol import BaseProtocol
from devp2p import slogging
import collections


class ConnectionMonitor(gevent.Greenlet):

    "monitors the connection by sending pings and checking pongs"
    ping_interval = 15.
    response_delay_threshold = 120.  # FIXME, apply msg takes too long
    max_samples = 1000
    log = slogging.get_logger('p2p.ctxmonitor')

    def __init__(self, proto):
        self.log.debug('init')
        assert isinstance(proto, P2PProtocol)
        self.proto = proto
        self.samples = collections.deque(maxlen=self.max_samples)
        self.last_response = self.last_request = time.time()
        super(ConnectionMonitor, self).__init__()
        # track responses
        self.proto.receive_pong_callbacks.append(self.track_response)
        self.proto.receive_hello_callbacks.append(lambda p, **kargs: self.start())

    def track_response(self, proto):
        self.last_response = time.time()
        self.samples.appendleft(self.last_response - self.last_request)

    def latency(self, num_samples=max_samples):
        num_samples = min(num_samples, len(self.samples))
        return sum(self.samples[i] for i in range(num_samples)) / num_samples if num_samples else 1

    def _run(self):
        self.log.debug('started', monitor=self)
        while True:
            self.log.debug('pinging', monitor=self)
            self.proto.send_ping()
            now = self.last_request = time.time()
            gevent.sleep(self.ping_interval)
            self.log.debug('latency', peer=self.proto, latency='%.3f' % self.latency())
            if now - self.last_response > self.response_delay_threshold:
                self.log.debug('unresponsive_peer', monitor=self)
                self.proto.peer.report_error('not responding to ping')
                self.proto.stop()
                self.proto.peer.stop()
                self.kill()

    def stop(self):
        self.log.debug('stopped', monitor=self)
        self.kill()

########################################

log = slogging.get_logger('p2p.protocol')


class P2PProtocol(BaseProtocol):

    """
    DEV P2P Wire Protocol
    https://github.com/ethereum/wiki/wiki/%C3%90%CE%9EVp2p-Wire-Protocol
    """
    protocol_id = 0
    name = b'p2p'
    version = 4
    max_cmd_id = 15

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        assert hasattr(peer, 'capabilities')
        assert callable(peer.stop)
        assert callable(peer.receive_hello)
        super(P2PProtocol, self).__init__(peer, service)

        def _on_monitor_exit(mon):
            peer.stop()
        self.monitor = ConnectionMonitor(self)
        self.monitor.link(_on_monitor_exit)

    def stop(self):
        self.monitor.stop()
        super(P2PProtocol, self).stop()

    class ping(BaseProtocol.command):
        cmd_id = 2

        def receive(self, proto, data):
            proto.send_pong()

    class pong(BaseProtocol.command):
        cmd_id = 3

    class hello(BaseProtocol.command):
        cmd_id = 0
        structure = [
            ('version', sedes.big_endian_int),
            ('client_version_string', sedes.binary),
            ('capabilities', sedes.CountableList(sedes.List([sedes.binary, sedes.big_endian_int]))),
            ('listen_port', sedes.big_endian_int),
            ('remote_pubkey', sedes.binary)
        ]
        # don't throw for additional list elements as
        # mandated by EIP-8.
        decode_strict = False

        def create(self, proto):
            return dict(version=proto.version,
                        client_version_string=proto.config['client_version_string'],
                        capabilities=proto.peer.capabilities,
                        listen_port=proto.config['p2p']['listen_port'],
                        remote_pubkey=proto.config['node']['id'],
                        )

        def receive(self, proto, data):
            log.debug('receive_hello', peer=proto.peer, version=data['version'])
            reasons = proto.disconnect.reason
            if data['remote_pubkey'] == proto.config['node']['id']:
                log.debug('connected myself')
                return proto.send_disconnect(reason=reasons.connected_to_self)

            proto.peer.receive_hello(proto, **data)
            # super(hello, self).receive(proto, data)
            BaseProtocol.command.receive(self, proto, data)

    @classmethod
    def get_hello_packet(cls, peer):
        "special: we need this packet before the protocol can be initalized"
        res = dict(version=cls.version,
                   client_version_string=peer.config['client_version_string'],
                   capabilities=peer.capabilities,
                   listen_port=peer.config['p2p']['listen_port'],
                   remote_pubkey=peer.config['node']['id'])
        payload = cls.hello.encode_payload(res)
        return Packet(cls.protocol_id, cls.hello.cmd_id, payload=payload)

    class disconnect(BaseProtocol.command):
        cmd_id = 1
        structure = [('reason', sedes.big_endian_int)]

        class reason(object):
            disconnect_requested = 0
            tcp_sub_system_error = 1
            bad_protocol = 2         # e.g. a malformed message, bad RLP, incorrect magic number
            useless_peer = 3
            too_many_peers = 4
            already_connected = 5
            incompatible_p2p_version = 6
            null_node_identity_received = 7
            client_quitting = 8
            unexpected_identity = 9  # i.e. a different identity to a previous connection or
            #                          what a trusted peer told us
            connected_to_self = 10
            timeout = 11             # i.e. nothing received since sending last ping
            subprotocol_error = 12
            other = 16               # Some other reason specific to a subprotocol

        def reason_name(self, _id):
            d = dict((_id, name) for name, _id in self.reason.__dict__.items())
            return d.get(_id, 'unknown (id:{})'.format(_id))

        def create(self, proto, reason=reason.client_quitting):
            assert self.reason_name(reason)
            log.debug('send_disconnect', peer=proto.peer, reason=self.reason_name(reason))
            proto.peer.report_error('sending disconnect %s' % self.reason_name(reason))
            # Defer disconnect until message is sent out.
            gevent.spawn_later(0.5, proto.peer.stop)
            return dict(reason=reason)

        def receive(self, proto, data):
            log.debug('receive_disconnect', peer=proto.peer,
                      reason=self.reason_name(data['reason']))
            proto.peer.report_error('disconnected %s' % self.reason_name(data['reason']))
            proto.peer.stop()
