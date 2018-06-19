import gevent
from devp2p import service
from devp2p import utils
from devp2p.app import BaseApp


def test_baseservice():
    app = BaseApp()

    class TestService(service.BaseService):
        counter = 0

        def _run(self):
            while True:
                self.counter += 1
                gevent.sleep(0.01)

    s = TestService.register_with_app(app)
    assert hasattr(s, 'name')
    # register another services
    TestService.name = 'other'
    s2 = TestService.register_with_app(app)

    # start app
    app.start()
    gevent.sleep(.1)
    app.stop()

    # check if they ran concurrently
    assert s.counter > 0 and s2.counter > 0
    assert abs(s.counter - s2.counter) <= 2


def test_wiredservice():
    app = BaseApp(config=dict())
    s = service.WiredService.register_with_app(app)


def test_config_update():
    c = dict(a=dict(b=1), g=5)
    d = dict(a=dict(b=2, c=3), d=4, e=dict(f=1))
    r = dict(a=dict(b=1, c=3), d=4, e=dict(f=1), g=5)
    assert utils.update_config_with_defaults(c, d) == r
    assert c == r

    c = dict(a=dict(b=1), g=5, h=[], k=[2])
    d = dict(a=dict(b=2, c=3), d=4, e=dict(f=1, i=[1, 2]), j=[])
    r = dict(a=dict(b=1, c=3), d=4, e=dict(f=1, i=[1, 2]), j=[], g=5, h=[], k=[2])
    assert utils.update_config_with_defaults(c, d) == r
    assert c == r
