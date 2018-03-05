import time
import errno
import gevent
import operator
from collections import OrderedDict
from .protocol import BaseProtocol
from .p2p_protocol import P2PProtocol
from .service import WiredService
from .multiplexer import MultiplexerError, Packet
from .muxsession import MultiplexedSession
from .crypto import ECIESDecryptionError
from devp2p import slogging
import gevent.socket
from devp2p import rlpxcipher
from rlp.utils import decode_hex

log = slogging.get_logger('p2p.peer')


class UnknownCommandError(Exception):

    "raised if we recive an unknown command for a known protocol"
    pass


class Peer(gevent.Greenlet):

    remote_client_version = ''
    offset_based_dispatch = False
    wait_read_timeout = 0.001
    dumb_remote_timeout = 10.0
    compatible_p2p_version = 5

    def __init__(self, peermanager, connection, remote_pubkey=None):
        super(Peer, self).__init__()
        self.is_stopped = False
        self.hello_received = False
        self.peermanager = peermanager
        self.connection = connection
        self.config = peermanager.config
        self.protocols = OrderedDict()
        log.debug('peer init', peer=self)

        # create multiplexed encrypted session
        privkey = decode_hex(self.config['node']['privkey_hex'])
        hello_packet = P2PProtocol.get_hello_packet(self)
        self.mux = MultiplexedSession(privkey, hello_packet, remote_pubkey=remote_pubkey)
        self.remote_pubkey = remote_pubkey
        self.remote_capabilities = None

        # register p2p protocol
        assert issubclass(self.peermanager.wire_protocol, P2PProtocol)
        self.connect_service(self.peermanager)

        # assure, we don't get messages while replies are not read
        self.safe_to_read = gevent.event.Event()
        self.safe_to_read.set()

        self.greenlets = dict()

        # Stop peer if hello not received in self.dumb_remote_timeout seconds
        self.greenlets['dumb_checker'] = gevent.spawn_later(self.dumb_remote_timeout, self.check_if_dumb_remote)

    @property
    def remote_pubkey(self):
        "if peer is responder, then the remote_pubkey will not be available"
        "before the first packet is received"
        return self.mux.remote_pubkey

    @remote_pubkey.setter
    def remote_pubkey(self, value):
        self.remote_pubkey_available = True if value else False
        self.mux.remote_pubkey = value

    def __repr__(self):
        try:
            pn = self.connection.getpeername()
        except gevent.socket.error:
            pn = ('not ready',)
        try:
            cv = '/'.join(self.remote_client_version.split('/')[:2])
        except:
            cv = self.remote_client_version
        return '<Peer%r %s>' % (pn, cv)
        # return '<Peer%r>' % repr(pn)

    def report_error(self, reason):
        try:
            ip_port = self.ip_port
        except:
            ip_port = 'ip_port not available fixme'
        self.peermanager.errors.add(ip_port, reason, self.remote_client_version)

    @property
    def ip_port(self):
        try:
            return self.connection.getpeername()
        except Exception as e:
            log.debug('ip_port failed', e=e)
            raise e

    def connect_service(self, service):
        assert isinstance(service, WiredService)
        protocol_class = service.wire_protocol
        assert issubclass(protocol_class, BaseProtocol)
        # create protcol instance which connects peer with serivce
        protocol = protocol_class(self, service)
        # register protocol
        assert protocol_class not in self.protocols
        log.debug('registering protocol', protocol=protocol.name, peer=self)
        self.protocols[protocol_class] = protocol
        self.mux.add_protocol(protocol.protocol_id)
        protocol.start()

    def has_protocol(self, protocol):
        assert issubclass(protocol, BaseProtocol)
        return protocol in self.protocols

    def receive_hello(self, proto, version, client_version_string, capabilities,
                      listen_port, remote_pubkey):
        log.debug('received hello', proto=proto, version=version,
                  client_version=client_version_string, capabilities=capabilities)
        assert isinstance(remote_pubkey, bytes)
        assert len(remote_pubkey) == 64
        if self.remote_pubkey_available:
            assert self.remote_pubkey == remote_pubkey
        self.hello_received = True

        # enable backwards compatibility for legacy peers
        if version <= self.compatible_p2p_version:
            self.offset_based_dispatch = True
            max_window_size = 2**32  # disable chunked transfers
        else:
            proto.send_disconnect(proto.disconnect.reason.incompatible_p2p_version)

        # call peermanager
        agree = self.peermanager.on_hello_received(
            proto, version, client_version_string, capabilities, listen_port, remote_pubkey)
        if not agree:
            return

        self.remote_client_version = client_version_string
        self.remote_pubkey = remote_pubkey
        self.remote_capabilities = capabilities

        # register in common protocols
        log.debug('connecting services', services=self.peermanager.wired_services)
        remote_services = dict()
        for name, version in capabilities:
            if isinstance(name, bytes) and not isinstance(name, str):
                name = name.decode('utf-8')
            if not name in remote_services:
                remote_services[name] = []
            remote_services[name].append(version)
        for service in sorted(self.peermanager.wired_services, key=operator.attrgetter('name')):
            proto = service.wire_protocol
            assert isinstance(service, WiredService)
            assert isinstance(proto.name, (bytes, str))
            if proto.name in remote_services:
                if proto.version in remote_services[proto.name]:
                    if service != self.peermanager:  # p2p protocol already registered
                        self.connect_service(service)
                else:
                    log.debug('wrong version', service=proto.name, local_version=proto.version,
                              remote_version=remote_services[proto.name])
                    self.report_error('wrong version')

    @property
    def capabilities(self):
        return [(s.wire_protocol.name, s.wire_protocol.version)
                for s in self.peermanager.wired_services]

    # sending p2p messages

    def send_packet(self, packet):
        for i, protocol in enumerate(self.protocols.values()):
            if packet.protocol_id == protocol.protocol_id:
                break

        assert packet.protocol_id == protocol.protocol_id, 'no protocol found'
        log.debug('send packet', cmd=protocol.cmd_by_id[packet.cmd_id], protcol=protocol.name,
                  peer=self)
        # rewrite cmd_id (backwards compatibility)
        if self.offset_based_dispatch:
            for i, protocol in enumerate(self.protocols.values()):
                if packet.protocol_id == protocol.protocol_id:
                    break
                packet.cmd_id += (0 if protocol.max_cmd_id == 0 else protocol.max_cmd_id + 1)
            packet.protocol_id = 0
        self.mux.add_packet(packet)

    # receiving p2p messages

    def protocol_cmd_id_from_packet(self, packet):
        # offset-based dispatch (backwards compatibility)
        if self.offset_based_dispatch:
            max_id = 0
            for protocol in self.protocols.values():
                if packet.cmd_id < max_id + protocol.max_cmd_id + 1:
                    return protocol, packet.cmd_id - max_id
                max_id += protocol.max_cmd_id + 1
            raise UnknownCommandError('no protocol for id %s' % packet.cmd_id)
        # new-style dispatch based on protocol_id
        for i, protocol in enumerate(self.protocols.values()):
            if packet.protocol_id == protocol.protocol_id:
                return protocol, packet.cmd_id
        raise UnknownCommandError('no protocol for protocol id %s' % packet.protocol_id)

    def _handle_packet(self, packet):
        assert isinstance(packet, Packet)
        try:
            protocol, cmd_id = self.protocol_cmd_id_from_packet(packet)
            log.debug('recv packet', peer=self, cmd=protocol.cmd_by_id[cmd_id], protocol=protocol.name, orig_cmd_id=packet.cmd_id)
            packet.cmd_id = cmd_id  # rewrite
            protocol.receive_packet(packet)
        except UnknownCommandError as e:
            log.debug('received unknown cmd', peer=self, error=e, packet=packet)
        except Exception as e:
            log.debug('failed to handle packet', peer=self, error=e)
            self.stop()

    def send(self, data):
        if not data:
            return
        self.safe_to_read.clear()  # make sure we don't accept any data until message is sent
        try:
            self.connection.sendall(data)  # check if gevent chunkes and switches contexts
            log.debug('wrote data', size=len(data), ts=time.time())
        except gevent.socket.error as e:
            log.debug('write error', errno=e.errno, reason=e.strerror)
            self.report_error('write error %r' % e.strerror)
            self.stop()
        except gevent.socket.timeout:
            log.debug('write timeout')
            self.report_error('write timeout')
            self.stop()
        self.safe_to_read.set()

    def _run_egress_message(self):
        while not self.is_stopped:
            self.send(self.mux.message_queue.get())

    def _run_decoded_packets(self):
        # handle decoded packets
        while not self.is_stopped:
            self._handle_packet(self.mux.packet_queue.get())  # get_packet blocks

    def _run_ingress_message(self):
        log.debug('peer starting main loop')
        assert not self.connection.closed, "connection is closed"
        self.greenlets['decoder'] = gevent.spawn(self._run_decoded_packets)
        self.greenlets['sender'] = gevent.spawn(self._run_egress_message)

        while not self.is_stopped:
            self.safe_to_read.wait()
            try:
                gevent.socket.wait_read(self.connection.fileno())
            except gevent.socket.error as e:
                log.debug('read error', errno=e.errno, reason=e.strerror, peer=self)
                self.report_error('network error %s' % e.strerror)
                if e.errno in(errno.EBADF,):
                    # ('Bad file descriptor')
                    self.stop()
                else:
                    raise e
                    break
            try:
                imsg = self.connection.recv(4096)
            except gevent.socket.error as e:
                log.debug('read error', errno=e.errno, reason=e.strerror, peer=self)
                self.report_error('network error %s' % e.strerror)
                if e.errno in(errno.ENETDOWN, errno.ECONNRESET, errno.ETIMEDOUT,
                              errno.EHOSTUNREACH, errno.ECONNABORTED):
                    # (Network down, Connection reset by peer, timeout, no route to host,
                    # Software caused connection abort (windows weirdness))
                    self.stop()
                else:
                    raise e
                    break
            if imsg:
                log.debug('read data', ts=time.time(), size=len(imsg))
                try:
                    self.mux.add_message(imsg)
                except (rlpxcipher.RLPxSessionError, ECIESDecryptionError) as e:
                    log.debug('rlpx session error', peer=self, error=e)
                    self.report_error('rlpx session error')
                    self.stop()
                except MultiplexerError as e:
                    log.debug('multiplexer error', peer=self, error=e)
                    self.report_error('multiplexer error')
                    self.stop()

    _run = _run_ingress_message

    def stop(self):
        if not self.is_stopped:
            try:
                self.is_stopped = True
                log.debug('peer stopped', peer=self)
                for g in self.greenlets.values():
                    try:
                        g.kill()
                    except gevent.GreenletExit:
                        pass
                self.greenlets = None
                for p in self.protocols.values():
                    p.stop()
                self.protocols = None  # break circular dependency with BaseProtocol
            except Exception as e:
                log.debug("failed to gracefully shutdown peer", error=e)
            finally:
                self.peermanager.peers.remove(self)
                self.kill()

    def check_if_dumb_remote(self):
        "Stop peer if hello not received"
        if not self.hello_received:
            self.report_error('No hello in {} seconds'.format(self.dumb_remote_timeout))
            self.stop()
