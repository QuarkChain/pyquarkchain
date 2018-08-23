import json
import unittest
import argparse
from quarkchain.cluster.cluster_config import ClusterConfig


class MonitoringTest(unittest.TestCase):
    def test_toJSON(self):
        sample = dict(a=1, b=2, c=["x", "y"])
        self.assertEqual(json.dumps(sample), '{"a": 1, "b": 2, "c": ["x", "y"]}')

    def test_unknown_structure(self):
        parser = argparse.ArgumentParser()
        ClusterConfig.attach_arguments(parser)
        args = parser.parse_args(["--monitoring_kafka_rest_address=x"])
        cluster_config = ClusterConfig.create_from_args(args)
        sample = dict(a=1, b=2, c={"x", "y"})
        cluster_config.kafka_logger.log_kafka_sample(
            "topic", sample
        )  # should trigger warning log

    def test_kafka_log(self):
        parser = argparse.ArgumentParser()
        ClusterConfig.attach_arguments(parser)
        args = parser.parse_args(
            []
        )  # set --kafka_rest_address correctly to see real actions
        cluster_config = ClusterConfig.create_from_args(args)
        sample = dict(a=1, b=2, c=["x", "y"])
        cluster_config.kafka_logger.log_kafka_sample("dlltest", sample)
