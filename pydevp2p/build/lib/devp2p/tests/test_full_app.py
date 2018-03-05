import platform

import pytest
import os
import time
import copy
import inspect
from devp2p import app_helper
from devp2p.app_helper import mk_privkey
from devp2p.peermanager import PeerManager
from devp2p.examples.full_app import Token, ExampleService, ExampleProtocol, ExampleApp
import gevent


class ExampleServiceIncCounter(ExampleService):
    def __init__(self, app):
        super(ExampleServiceIncCounter, self).__init__(app)
        self.collected = set()
        self.broadcasted = set()
        self.is_stopping = False
        gevent.spawn_later(0.5, self.tick)

    def on_wire_protocol_start(self, proto):
        assert isinstance(proto, self.wire_protocol)
        my_version = self.config['client_version_string']
        my_peers = self.app.services.peermanager.peers
        assert my_peers

        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto, my_peers=my_peers)

        # check the peers is not connected to self
        for p in my_peers:
            assert p.remote_client_version != my_version

        # check the peers is connected to distinct nodes
        my_peers_with_hello_received = list(filter(lambda p: p.remote_client_version != '', my_peers))
        versions = list(map(lambda p: p.remote_client_version, my_peers_with_hello_received))
        self.log('versions', versions=versions)
        assert len(set(versions)) == len(versions)

        proto.receive_token_callbacks.append(self.on_receive_token)

        # check if number of peers that received hello is equal to number of min_peers
        if self.config['p2p']['min_peers'] == len(my_peers_with_hello_received):
            self.testdriver.NODES_PASSED_SETUP.add(my_version)
            if len(self.testdriver.NODES_PASSED_SETUP) == self.testdriver.NUM_NODES:
                self.send_synchro_token()

    def on_receive_token(self, proto, token):
        assert isinstance(token, Token)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive token {}'.format(token.counter),
                 collected=len(self.collected), broadcasted=len(self.broadcasted))

        assert token.counter not in self.collected

        # NODE0 must send first token to make algorithm work
        if not self.collected and not self.broadcasted and token.counter == 0:
            if self.config['node_num'] == 0:
                self.log("send initial token to the wire.")
                self.try_send_token()
            else:
                self.send_synchro_token()
            return

        if token.counter == 0:
            return

        self.collected.add(token.counter)
        self.log('collected token {}'.format(token.counter))

        if token.counter >= self.testdriver.COUNTER_LIMIT:
            self.stop_test()
            return

        self.try_send_token()

    def send_synchro_token(self):
        self.log("send synchronization token")
        self.broadcast(Token(counter=0, sender=self.address))

    def try_send_token(self):
        counter = 0 if not self.collected else max(self.collected)
        turn = counter % self.config['num_nodes']
        if turn != self.config['node_num']:
            return
        if counter+1 in self.broadcasted:
            return
        self.broadcasted.add(counter+1)
        token = Token(counter=counter+1, sender=self.address)
        self.log('sending token {}'.format(counter+1), token=token)
        self.broadcast(token)
        if counter+1 == self.testdriver.COUNTER_LIMIT:
            self.stop_test()

    def stop_test(self):
        if not self.is_stopping:
            self.log("COUNTER LIMIT REACHED. STOP THE APP")
            self.is_stopping = True
            # defer until all broadcast arrive
            gevent.spawn_later(2.0, self.assert_collected)

    def assert_collected(self):
        self.log("ASSERT", broadcasted=len(self.broadcasted), collected=len(self.collected))
        assert len(self.collected) > len(self.broadcasted)

        for turn in range(1, self.testdriver.COUNTER_LIMIT):
            if (turn-1) % self.testdriver.NUM_NODES == self.config['node_num']:
                assert turn in self.broadcasted
            else:
                assert turn in self.collected

        self.testdriver.NODES_PASSED_INC_COUNTER.add(self.config['node_num'])

    def tick(self):
        if len(self.testdriver.NODES_PASSED_INC_COUNTER) == self.testdriver.NUM_NODES:
            self.app.stop()
            return
        gevent.spawn_later(0.5, self.tick)


class ExampleServiceAppRestart(ExampleService):
    def __init__(self, app):
        super(ExampleServiceAppRestart, self).__init__(app)
        gevent.spawn_later(0.5, self.tick)

    def on_wire_protocol_start(self, proto):
        my_version = self.config['node_num']
        # Restart only NODE0's app
        self.log('protocol_start {}'.format(my_version))
        if my_version == 0:
            if self.testdriver.APP_RESTARTED:
                self.testdriver.TEST_SUCCESSFUL = True
            else:
                self.app.stop()
                gevent.sleep(1)
                self.app.start()
                self.testdriver.APP_RESTARTED = True

    def tick(self):
        if self.testdriver.TEST_SUCCESSFUL:
            self.app.stop()
            return
        gevent.spawn_later(0.5, self.tick)


class ExampleServiceAppDisconnect(ExampleService):
    def __init__(self, app):
        super(ExampleServiceAppDisconnect, self).__init__(app)
        gevent.spawn_later(self.testdriver.DISCOVERY_LOOP_SEC, self.assert_num_peers)

    def assert_num_peers(self):
        assert self.app.services.peermanager.num_peers() <= self.testdriver.MIN_PEERS
        self.app.stop()


@pytest.mark.parametrize('num_nodes', [3, 6])
class TestFullApp:
    @pytest.mark.timeout(60)
    def test_inc_counter_app(self, num_nodes):
        class TestDriver(object):
            NUM_NODES = num_nodes
            COUNTER_LIMIT = 256
            NODES_PASSED_SETUP = set()
            NODES_PASSED_INC_COUNTER = set()

        ExampleServiceIncCounter.testdriver = TestDriver()

        app_helper.run(
            ExampleApp,
            ExampleServiceIncCounter,
            num_nodes=num_nodes,
            min_peers=num_nodes-1,
            max_peers=num_nodes-1,
            random_port=True  # Use a random port to avoid 'Address already in use' errors
        )


@pytest.mark.timeout(20)
def test_app_restart():
    """
    Test scenario:
    - Restart the app on 1st node when the node is on_wire_protocol_start
    - Check that this node gets on_wire_protocol_start at least once after restart
        - on_wire_protocol_start indicates that node was able to communicate after restart
    """
    class TestDriver(object):
        APP_RESTARTED = False
        TEST_SUCCESSFUL = False

    ExampleServiceAppRestart.testdriver = TestDriver()

    app_helper.run(ExampleApp, ExampleServiceAppRestart,
                   num_nodes=3, min_peers=2, max_peers=2)


@pytest.mark.timeout(30)
def test_disconnect():
    """
    Test scenario:
    - Run app with min_peers > max_peers to force lots of peer.stop() (too many peers)
    - After X seconds of unsuccessful (by definition) discovery check that len(peers) <= min_peers
    """

    class TestDriver(object):
        DISCOVERY_LOOP_SEC = 10
        MIN_PEERS = 2

    ExampleServiceAppDisconnect.testdriver = TestDriver()

    # To be able to run app with min_peers > max_peers one has to bypass asserts.
    app_helper.assert_config = lambda a, b, c, d: True

    app_helper.run(ExampleApp, ExampleServiceAppDisconnect,
                   num_nodes=3, min_peers=2, max_peers=1, random_port=True)


if __name__ == "__main__":
    import devp2p.slogging as slogging
    slogging.configure(config_string=':debug,p2p:info')
    log = slogging.get_logger('app')
    TestFullApp().test_inc_counter_app(3)
    TestFullApp().test_inc_counter_app(6)
    test_app_restart()
    test_disconnect()
