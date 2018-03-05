import sys
import random
from devp2p.app import BaseApp
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import colors, COLOR_END
from devp2p import app_helper
import rlp
from rlp.utils import encode_hex, decode_hex, is_integer
import gevent
try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
except:
    import devp2p.slogging as slogging
log = slogging.get_logger('app')


class Token(rlp.Serializable):

    "Object with the information to update a decentralized counter"
    fields = [
        ('counter', rlp.sedes.big_endian_int),
        ('sender', rlp.sedes.binary)
    ]

    def __init__(self, counter=0, sender=''):
        assert is_integer(counter)
        assert isinstance(sender, bytes)
        super(Token, self).__init__(counter, sender)

    @property
    def hash(self):
        return sha3(rlp.encode(self))

    def __repr__(self):
        try:
            return '<%s(counter=%d hash=%s)>' % (self.__class__.__name__, self.counter,
                                                 encode_hex(self.hash)[:4])
        except:
            return '<%s>' % (self.__class__.__name__)


class ExampleProtocol(BaseProtocol):
    protocol_id = 1
    network_id = 0
    max_cmd_id = 1  # Actually max id is 0, but 0 is the special value.
    name = b'example'
    version = 1

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)

    class token(BaseProtocol.command):

        """
        message sending a token and a nonce
        """
        cmd_id = 0

        structure = [
            ('token', Token)
        ]


class DuplicatesFilter(object):

    def __init__(self, max_items=1024):
        self.max_items = max_items
        self.filter = list()

    def update(self, data):
        "returns True if unknown"
        if data not in self.filter:
            self.filter.append(data)
            if len(self.filter) > self.max_items:
                self.filter.pop(0)
            return True
        else:
            self.filter.append(self.filter.pop(0))
            return False

    def __contains__(self, v):
        return v in self.filter


class ExampleService(WiredService):

    # required by BaseService
    name = 'exampleservice'
    default_config = dict(example=dict(num_participants=1))

    # required by WiredService
    wire_protocol = ExampleProtocol  # create for each peer

    def __init__(self, app):
        self.config = app.config
        self.address = privtopub_raw(decode_hex(self.config['node']['privkey_hex']))
        super(ExampleService, self).__init__(app)

    def start(self):
        super(ExampleService, self).start()

    def log(self, text, **kargs):
        node_num = self.config['node_num']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "NODE%d" % node_num,
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        log.debug(msg)

    def broadcast(self, obj, origin=None):
        fmap = {Token: 'token'}
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(ExampleProtocol, fmap[type(obj)], args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    def on_wire_protocol_stop(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_wire_protocol_stop', proto=proto)

    # application logic

    def on_wire_protocol_start(self, proto):
        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto, peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        # register callbacks
        proto.receive_token_callbacks.append(self.on_receive_token)
        self.send_token()

    def on_receive_token(self, proto, token):
        assert isinstance(token, Token)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive token', token=token, proto=proto)
        self.send_token()

    def send_token(self):
        gevent.sleep(random.random())
        token = Token(counter=random.randint(0, 1024), sender=self.address)
        self.log('----------------------------------')
        self.log('sending token', token=token)
        self.broadcast(token)


class ExampleApp(BaseApp):
    client_name = 'exampleapp'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None


if __name__ == '__main__':
    app_helper.run(ExampleApp, ExampleService)
