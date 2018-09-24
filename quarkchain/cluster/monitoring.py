import json
import requests
import aiohttp
from quarkchain.utils import Logger


class KafkaSampleLogger:
    def __init__(self, cluster_config):
        self.cluster_config = cluster_config

    def log_kafka_sample(self, topic: str, sample: dict):
        """This is for testing/debugging only, use async version for production"""
        if self.cluster_config.MONITORING.KAFKA_REST_ADDRESS == "":
            return
        url = "http://{}/topics/{}".format(
            self.cluster_config.MONITORING.KAFKA_REST_ADDRESS, topic
        )
        try:
            record_data = json.dumps({"records": [{"value": sample}]})
            headers = {
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            }
            response = requests.post(url, data=record_data, headers=headers)
            if response.status_code != 200:
                raise Exception(
                    "non-OK response status code: {}".format(response.status_code)
                )
        except Exception as ex:
            Logger.error_only("Failed to log sample to Kafka: {}".format(ex))

    async def log_kafka_sample_async(self, topic: str, sample: dict):
        """logs sample to Kafka topic asynchronously
        Sample for monitoring purpose:
        Supports logging samples to Kafka via REST API (Confluent)

        Column guidelines:
        time: epoch in seconds
        sample_rate: pre-sampled record shall set this to sample rate, e.g., 100 means one sample is logged out of 100
        column type shall be log int, str, or vector of str
        """
        if not self.cluster_config.MONITORING.KAFKA_REST_ADDRESS or not topic:
            return
        url = "http://{}/topics/{}".format(
            self.cluster_config.MONITORING.KAFKA_REST_ADDRESS, topic
        )
        try:
            record_data = json.dumps({"records": [{"value": sample}]})
            headers = {
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            }
            session = aiohttp.ClientSession()
            response = await session.post(url, data=record_data, headers=headers)
            if response.status != 200:
                raise Exception(
                    "non-OK response status code: {}".format(response.status_code)
                )
        except Exception as ex:
            Logger.error_only_every_n(
                "Failed to log sample to Kafka: {}".format(ex), 100
            )
        finally:
            await session.close()
