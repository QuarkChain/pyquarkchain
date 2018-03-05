import random

from devp2p import peermanager
from devp2p.service import BaseService
from devp2p.discovery import NodeDiscovery
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import host_port_pubkey_to_uri, update_config_with_defaults
from rlp.utils import encode_hex
import gevent
import copy


def mk_privkey(seed):
    return sha3(seed)


def assert_config(node_num, num_nodes, min_peers, max_peers):
    # node number cannot be greater than total number of nodes
    assert node_num < num_nodes
    # node cannot be connected to self and
    # node cannot be connected twice to the same node
    assert min_peers <= max_peers < num_nodes


def create_app(node_num, config, services, app_class):
    num_nodes = config['num_nodes']
    base_port = config['base_port']
    seed = config['seed']
    min_peers = config['min_peers']
    max_peers = config['max_peers']

    assert_config(node_num, num_nodes, min_peers, max_peers)
    config = copy.deepcopy(config)
    config['node_num'] = node_num

    # create this node priv_key
    config['node']['privkey_hex'] = encode_hex(mk_privkey('%d:udp:%d' % (seed, node_num)))
    # set ports based on node
    config['discovery']['listen_port'] = base_port + node_num
    config['p2p']['listen_port'] = base_port + node_num
    config['p2p']['min_peers'] = min(10, min_peers)
    config['p2p']['max_peers'] = max_peers
    config['client_version_string'] = 'NODE{}'.format(node_num)

    app = app_class(config)

    # register services
    for service in services:
        assert issubclass(service, BaseService)
        if service.name not in app.config['deactivated_services']:
            assert service.name not in app.services
            service.register_with_app(app)
            assert hasattr(app.services, service.name)

    return app


def serve_until_stopped(apps):
    for app in apps:
        app.start()
        if app.config['post_app_start_callback'] is not None:
            app.config['post_app_start_callback'](app)

    # wait for apps to finish
    for app in apps:
        app.join()

    # finally stop
    for app in apps:
        app.stop()


def run(app_class, service_class, num_nodes=3, seed=0, min_peers=2, max_peers=2, random_port=False):
    gevent.get_hub().SYSTEM_ERROR = BaseException
    if random_port:
        base_port = random.randint(10000, 60000)
    else:
        base_port = 29870

    # get bootstrap node (node0) enode
    bootstrap_node_privkey = mk_privkey('%d:udp:%d' % (seed, 0))
    bootstrap_node_pubkey = privtopub_raw(bootstrap_node_privkey)
    enode = host_port_pubkey_to_uri('0.0.0.0', base_port, bootstrap_node_pubkey)

    services = [NodeDiscovery, peermanager.PeerManager, service_class]

    # prepare config
    base_config = dict()
    for s in services:
        update_config_with_defaults(base_config, s.default_config)

    base_config['discovery']['bootstrap_nodes'] = [enode]
    base_config['seed'] = seed
    base_config['base_port'] = base_port
    base_config['num_nodes'] = num_nodes
    base_config['min_peers'] = min_peers
    base_config['max_peers'] = max_peers

    # prepare apps
    apps = []
    for node_num in range(num_nodes):
        app = create_app(node_num, base_config, services, app_class)
        apps.append(app)

    # start apps
    serve_until_stopped(apps)
