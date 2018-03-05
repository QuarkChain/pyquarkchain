import logging
import unittest
from devp2p import slogging
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class LoggingTest(unittest.TestCase):
    def get_logger(self, name):
        pass

    def setUp(self):
        self.stream = StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.handler.setLevel(logging.INFO)
        self.log = self.get_logger(__name__)
        self.log.addHandler(self.handler)

    def tearDown(self):
        self.log.removeHandler(self.handler)

    def expect_log(self, msg, *parts):
        self.handler.flush()
        # FIXME: slogging adds unnecessary space in the end in some cases.
        # Workaround with rstrip().
        log_messages = self.stream.getvalue().rstrip()
        self.assertTrue(log_messages.startswith(msg.rstrip()))
        for part in parts:
            self.assertIn(part, log_messages)


class LoggingPatchTest(LoggingTest):
    def get_logger(self, name):
        return logging.getLogger(name)

    def test_logging_patching1(self):
        """Test format arguments"""
        self.log.info('Hello World number %d!', 13)
        self.expect_log('Hello World number 13!\n')

    def test_logging_patching2(self):
        """Test msg not being a string"""
        self.log.info(('Hello World!', "Hakuna Matata!"))
        self.expect_log("('Hello World!', 'Hakuna Matata!')\n")


class SloggingTest(LoggingTest):
    def get_logger(self, name):
        return slogging.get_logger(name)

    def test_slogging(self):
        """Test format arguments"""
        self.log.info('Hello World number %d!', 13)
        self.expect_log('Hello World number 13!\n')

    def test_slogging_kwargs(self):
        """Test patched kwargs support"""
        self.log.info("Test kwargs:", number=1, f=2.3, comment='works!')
        self.expect_log("Test kwargs:", "comment=works!", "number=1", "f=2.3")
