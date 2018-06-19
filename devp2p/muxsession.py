import gevent
from .multiplexer import Multiplexer, Packet
from .rlpxcipher import RLPxSession
from .crypto import ECCx


class MultiplexedSession(Multiplexer):
    def __init__(self, privkey, hello_packet, remote_pubkey=None):
        self.is_initiator = bool(remote_pubkey)
        self.hello_packet = hello_packet
        self.message_queue = gevent.queue.Queue()  # wire msg egress queue
        self.packet_queue = gevent.queue.Queue()  # packet ingress queue
        ecc = ECCx(raw_privkey=privkey)
        self.rlpx_session = RLPxSession(
            ecc, is_initiator=bool(remote_pubkey))
        self._remote_pubkey = remote_pubkey
        Multiplexer.__init__(self, frame_cipher=self.rlpx_session)
        if self.is_initiator:
            self._send_init_msg()

    @property
    def is_ready(self):
        # only authenticated and ready after successfully authenticated hello packet
        return self.rlpx_session.is_ready

    @property
    def remote_pubkey(self):
        "if responder not be available until first message is received"
        return self._remote_pubkey or self.rlpx_session.remote_pubkey

    @remote_pubkey.setter
    def remote_pubkey(self, value):
        self._remote_pubkey = value

    def _send_init_msg(self):
        auth_msg = self.rlpx_session.create_auth_message(self._remote_pubkey)
        auth_msg_ct = self.rlpx_session.encrypt_auth_message(auth_msg)
        self.message_queue.put(auth_msg_ct)

    def _add_message_during_handshake(self, msg):
        assert not self.is_ready
        session = self.rlpx_session
        if self.is_initiator:
            # expecting auth ack message
            rest = session.decode_auth_ack_message(msg)
            session.setup_cipher()
            if len(rest) > 0:  # add remains (hello) to queue
                self._add_message_post_handshake(rest)
        else:
            # expecting auth_init
            rest = session.decode_authentication(msg)
            auth_ack_msg = session.create_auth_ack_message()
            auth_ack_msg_ct = session.encrypt_auth_ack_message(auth_ack_msg)
            self.message_queue.put(auth_ack_msg_ct)
            session.setup_cipher()
            if len(rest) > 0:
                self._add_message_post_handshake(rest)
        self.add_message = self._add_message_post_handshake

        # send hello
        assert session.is_ready
        self.add_packet(self.hello_packet)

    add_message = _add_message_during_handshake  # on_ready set to _add_message_post_handshake

    def _add_message_post_handshake(self, msg):
        "decodes msg and adds decoded packets to queue"
        for packet in self.decode(msg):
            self.packet_queue.put(packet)

    def add_packet(self, packet):
        "encodes a packet and adds the message(s) to the msg queue"
        assert isinstance(packet, Packet)
        assert self.is_ready  # don't send anything until handshake is finished
        Multiplexer.add_packet(self, packet)
        for f in self.pop_all_frames():
            self.message_queue.put(f.as_bytes())
