import gevent
import rlp
from rlp import sedes
from .multiplexer import Packet
from .service import WiredService
from devp2p import slogging
log = slogging.get_logger('protocol')


class ProtocolError(Exception):
    pass


class SubProtocolError(ProtocolError):
    pass


class BaseProtocol(gevent.Greenlet):

    """
    A protocol mediates between the network and the service.
    It implements a collection of commands.

    For each command X the following methods are created at initialization:
    -    packet = protocol.create_X(*args, **kargs)
    -   protocol.send_X(*args, **kargs) is a shortcut for:
            protocol.send_packet(protocol.create_X(*args, **kargs))
    - protocol._receive_X(data)


    on protocol.receive_packet, the packet is deserialized according to the command.structure
        and the command.receive method called with a dict containing the received data

    the default implementation of command.receive calls callbacks
    which can be registered in a list which is available as:
    protocol.receive_X_callbacks
    """
    protocol_id = 0
    name = ''
    version = 0
    max_cmd_id = 0  # reserved cmd space

    class command(object):

        """
        - set cmd_id
        - define structure for rlp de/endcoding by sedes
            - list(arg_name, rlp.sedes.type), ...)  # for structs
            - sedes.CountableList(sedes.type)       # for lists with uniform item type
        - if you want non-strict decoding, define decode_strict = False
        optionally implement
        - create
        - receive

        default receive implementation, call callbacks with (proto_instance, data_dict)
        """
        cmd_id = 0
        structure = []  # [(arg_name, rlp.sedes.type), ...]
        decode_strict = True

        def create(self, proto, *args, **kargs):
            "optionally implement create"
            assert isinstance(proto, BaseProtocol)
            assert not (kargs and isinstance(self.structure, sedes.CountableList))
            return kargs or args

        def receive(self, proto, data):
            "optionally implement receive"
            for cb in self.receive_callbacks:
                if isinstance(self.structure, sedes.CountableList):
                    cb(proto, data)
                else:
                    cb(proto, **data)

        # no need to redefine the following ##################################

        def __init__(self):
            assert isinstance(self.structure, (list, sedes.CountableList))
            self.receive_callbacks = []

        @classmethod
        def encode_payload(cls, data):
            if isinstance(data, dict):  # convert dict to ordered list
                assert isinstance(cls.structure, list)
                data = [data[x[0]] for x in cls.structure]
            if isinstance(cls.structure, sedes.CountableList):
                return rlp.encode(data, cls.structure)
            else:
                assert len(data) == len(cls.structure)
                return rlp.encode(data, sedes=sedes.List([x[1] for x in cls.structure]))

        @classmethod
        def decode_payload(cls, rlp_data):
            # log.debug('decoding rlp', size=len(rlp_data))
            if isinstance(cls.structure, sedes.CountableList):
                decoder = cls.structure
            else:
                decoder = sedes.List([x[1] for x in cls.structure], strict=cls.decode_strict)
            try:
                data = rlp.decode(rlp_data, sedes=decoder)
            except (AssertionError, rlp.RLPException, TypeError) as e:
                print(repr(rlp.decode(rlp_data)))
                raise e
            if isinstance(cls.structure, sedes.CountableList):
                return data
            else:  # convert to dict
                return dict((cls.structure[i][0], v) for i, v in enumerate(data))

        # end command base ###################################################

    def __init__(self, peer, service):
        "hint: implement peer_started notification of associated protocol here"
        assert isinstance(service, WiredService)
        assert callable(peer.send_packet)
        self.is_stopped = False
        self.peer = peer
        self.service = service
        self._setup()
        super(BaseProtocol, self).__init__()

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.peer)

    def _setup(self):

        # collect commands
        klasses = [k for k in self.__class__.__dict__.values()
                   if isinstance(k, type) and issubclass(k, self.command) and k != self.command]
        assert len(set(k.cmd_id for k in klasses)) == len(klasses)

        def create_methods(klass):
            instance = klass()

            def receive(packet):
                "decode rlp, create dict, call receive"
                assert isinstance(packet, Packet)
                instance.receive(proto=self, data=klass.decode_payload(packet.payload))

            def create(*args, **kargs):
                "get data, rlp encode, return packet"
                res = instance.create(self, *args, **kargs)
                payload = klass.encode_payload(res)
                return Packet(self.protocol_id, klass.cmd_id, payload=payload)

            def send(*args, **kargs):
                "create and send packet"
                packet = create(*args, **kargs)
                self.send_packet(packet)

            return receive, create, send, instance.receive_callbacks

        for klass in klasses:
            receive, create, send, receive_callbacks = create_methods(klass)
            setattr(self, '_receive_' + klass.__name__, receive)
            setattr(self, 'receive_' + klass.__name__ + '_callbacks', receive_callbacks)
            setattr(self, 'create_' + klass.__name__, create)
            setattr(self, 'send_' + klass.__name__, send)

        self.cmd_by_id = dict((klass.cmd_id, klass.__name__) for klass in klasses)

    def receive_packet(self, packet):
        cmd_name = self.cmd_by_id[packet.cmd_id]
        cmd = getattr(self, '_receive_' + cmd_name)
        try:
            cmd(packet)
        except ProtocolError as e:
            log.debug('protocol exception, stopping', error=e, peer=self.peer)
            self.stop()

    def send_packet(self, packet):
        self.peer.send_packet(packet)

    def start(self):
        log.debug('starting', proto=self)
        super(BaseProtocol, self).start()
        self.service.on_wire_protocol_start(self)

    def _run(self):
        pass

    def stop(self):
        log.debug('stopping', proto=self)
        self.is_stopped = True
        self.service.on_wire_protocol_stop(self)
        super(BaseProtocol, self).kill()
