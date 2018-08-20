import typing
import json
import requests
from absl import flags, logging as GLOG

flags.DEFINE_string(
    "kafka_rest_address", "", "REST API endpoint for logging to Kafka, IP[:PORT] format"
)

FLAGS = flags.FLAGS


class Sample:
    """
    Sample for monitoring purpose
    Supports logging samples to Kafka via REST API (Confluent)
    Column guidelines:
    time: timestamp of the record
    sample_rate: pre-sampled record shall set this to sample rate
    column type shall be log int, str, or vector of str
    """

    def __init__(self, **kwargs):
        self.data = dict()
        self.data.update(kwargs)

    def addData(self, **kwargs):
        self.data.update(kwargs)

    def addString(self, key: str, value: str):
        self.data[key] = value

    def addInt(self, key: str, value: int):
        self.data[key] = value

    def addSet(self, key: str, value: typing.Set[str]):
        self.data[key] = list(value)

    def addVector(self, key: str, value: typing.List[str]):
        self.data[key] = value

    def logToTopic(self, topic: str):
        if not FLAGS.is_parsed():
            GLOG.log_every_n(
                GLOG.WARNING,
                "FLAGS is not parsed so kafka_rest_address is not set",
                100,
            )
            return
        if FLAGS.kafka_rest_address == "":
            return
        url = "http://{}/topics/{}".format(FLAGS.kafka_rest_address, topic)
        try:
            record_data = json.dumps({"records": [{"value": self.data}]})
            headers = {
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            }
            response = requests.post(url, data=record_data, headers=headers)
            GLOG.info(response)
        except Exception as ex:
            GLOG.log_every_n(GLOG.ERROR, "Failed to log sample to Kafka: %s", 100, ex)
