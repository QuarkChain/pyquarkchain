try:
    from UserDict import IterableUserDict
except ImportError:
    from collections import UserDict as IterableUserDict
from .service import BaseService
from .slogging import get_logger
from devp2p import utils
from devp2p import crypto
from rlp.utils import decode_hex
from devp2p import __version__
log = get_logger('app')


class BaseApp(object):

    default_config = dict(client_version_string='pydevp2p {}'.format(__version__),
                          deactivated_services=[])

    def __init__(self, config=default_config):
        self.config = utils.update_config_with_defaults(config, self.default_config)
        self.services = IterableUserDict()

    def register_service(self, service):
        """
        registers protocol with app, which will be accessible as
        app.services.<protocol.name> (e.g. app.services.p2p or app.services.eth)
        """
        assert isinstance(service, BaseService)
        assert service.name not in self.services
        log.info('registering service', service=service.name)
        self.services[service.name] = service
        setattr(self.services, service.name, service)

    def deregister_service(self, service):
        assert isinstance(service, BaseService)
        self.services.remove(service)
        delattr(self.services, service.name)

    def start(self):
        for service in self.services.values():
            service.start()

    def stop(self):
        for service in self.services.values():
            service.stop()

    def join(self):
        for service in self.services.values():
            service.join()


def main():
    # config
    import yaml
    import io
    import sys
    import signal
    import gevent
    from .peermanager import PeerManager
    from .discovery import NodeDiscovery
    from devp2p import slogging
    log = slogging.get_logger('app')
    slogging.configure(config_string=':debug')

    # read config
    sample_config = b"""
p2p:
    num_peers: 10
    bootstrap_nodes:
        # local bootstrap
        # - enode://6ed2fecb28ff17dec8647f08aa4368b57790000e0e9b33a7b91f32c41b6ca9ba21600e9a8c44248ce63a71544388c6745fa291f88f8b81e109ba3da11f7b41b9@127.0.0.1:30303
        # go_bootstrap
        #- enode://6cdd090303f394a1cac34ecc9f7cda18127eafa2a3a06de39f6d920b0e583e062a7362097c7c65ee490a758b442acd5c80c6fce4b148c6a391e946b45131365b@54.169.166.226:30303
        # cpp_bootstrap
        - enode://4a44599974518ea5b0f14c31c4463692ac0329cb84851f3435e6d1b18ee4eae4aa495f846a0fa1219bd58035671881d44423876e57db2abd57254d0197da0ebe@5.1.83.226:30303

    listen_host: 0.0.0.0
    listen_port: 30303
node:
    privkey_hex: 65462b0520ef7d3df61b9992ed3bea0c56ead753be7c8b3614e0ce01e4cac41b
    """
    if len(sys.argv) == 1:
        config = yaml.load(io.BytesIO(sample_config))
        pubkey = crypto.privtopub(decode_hex(config['node']['privkey_hex']))
        config['node']['id'] = crypto.sha3(pubkey)
    else:
        fn = sys.argv[1]
        log.info('loading config from', fn=fn)
        config = yaml.load(open(fn))

    # stop on every unhandled exception!
    gevent.get_hub().SYSTEM_ERROR = BaseException  # (KeyboardInterrupt, SystemExit, SystemError)

    print(config)
    # create app
    app = BaseApp(config)

    # register services
    NodeDiscovery.register_with_app(app)
    PeerManager.register_with_app(app)

    # start app
    app.start()

    # wait for interupt
    evt = gevent.event.Event()
    # gevent.signal(signal.SIGQUIT, gevent.kill) ## killall pattern
    gevent.signal(signal.SIGQUIT, evt.set)
    gevent.signal(signal.SIGTERM, evt.set)
    gevent.signal(signal.SIGINT, evt.set)
    evt.wait()

    # finally stop
    app.stop()

if __name__ == '__main__':
    #  python app.py 2>&1 | less +F
    main()
