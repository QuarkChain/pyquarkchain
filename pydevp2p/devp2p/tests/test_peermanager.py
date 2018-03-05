from devp2p import peermanager
from devp2p import crypto
from devp2p.app import BaseApp
from rlp.utils import encode_hex
import devp2p.muxsession
import rlp
import devp2p.p2p_protocol
import time
import gevent
import copy
import socket


def try_tcp_connect(addr):
    s = socket.socket()
    s.connect(addr)
    s.close()


def test_app_restart():
    host, port = '127.0.0.1', 3020

    a_config = dict(p2p=dict(listen_host=host, listen_port=port),
                    node=dict(privkey_hex=encode_hex(crypto.sha3(b'a'))))

    a_app = BaseApp(a_config)
    peermanager.PeerManager.register_with_app(a_app)

    # Restart app 10-times: there should be no exception
    for i in range(10):
        a_app.start()
        assert a_app.services.peermanager.server.started
        try_tcp_connect((host, port))
        assert a_app.services.peermanager.num_peers() == 0
        a_app.stop()
        assert a_app.services.peermanager.is_stopped

    # Start the app 10-times: there should be no exception like 'Bind error'
    for i in range(10):
        a_app.start()
        assert a_app.services.peermanager.server.started
        try_tcp_connect((host, port))

    a_app.stop()
    assert a_app.services.peermanager.is_stopped

if __name__ == '__main__':
    # ethereum -loglevel 5 --bootnodes ''
    import ethereum.slogging
    ethereum.slogging.configure(config_string=':debug')
    test_app_restart()
