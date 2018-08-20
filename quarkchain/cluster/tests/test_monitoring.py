import json
import unittest
import sys
from absl import flags, logging as GLOG

from quarkchain.cluster.monitoring import Sample


class MonitoringTest(unittest.TestCase):
    def test_toJSON(self):
        sample = Sample(a=1, b=2, c=["x", "y"])
        self.assertEqual(json.dumps(sample.data), '{"a": 1, "b": 2, "c": ["x", "y"]}')

    def test_unknown_structure(self):
        flags.FLAGS(sys.argv)
        flags.FLAGS.kafka_rest_address = "x"
        sample = Sample(a=1, b=2, c={"x", "y"})
        sample.logToTopic("topic")  # should trigger warning log

    def test_kafka_log(self):
        flags.FLAGS(sys.argv)
        # set --kafka_rest_address correctly in commandline
        sample = Sample(a=1, b=2, c=["x", "y"])
        sample.logToTopic("dlltest")
